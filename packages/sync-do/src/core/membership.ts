import type { SqlDriver } from "./driver.ts"

interface Member {
  readonly path: string
  readonly table: string
  readonly key: string
}

/**
 * Persistent, copy-on-write sets. Growing windows share bounded chunks of their
 * prefix; changing a shared chunk copies only that chunk, never the whole view.
 * All bookkeeping is SQL so transaction rollback and object eviction are safe.
 */
export class MembershipStore {
  constructor(
    private readonly db: SqlDriver,
    private readonly newId: () => string,
  ) {}

  init(): void {
    this.db.transaction(() => this.initStorage())
  }

  private initStorage(): void {
    const old = this.db.query(`SELECT type FROM sqlite_master WHERE name = 'membership'`)[0]
    if (old?.["type"] === "table") this.db.run(`ALTER TABLE membership RENAME TO membership_rows`)
    this.db.run(
      `CREATE TABLE IF NOT EXISTS membership_rows (subscription TEXT NOT NULL, path TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (subscription, path, tbl, key)) WITHOUT ROWID`,
    )
    this.db.run(
      `CREATE INDEX IF NOT EXISTS membership_by_row ON membership_rows (tbl, key, subscription)`,
    )
    this.db.run(
      `CREATE TABLE IF NOT EXISTS membership_chunks (subscription TEXT NOT NULL, chunk TEXT NOT NULL, PRIMARY KEY (subscription, chunk)) WITHOUT ROWID`,
    )
    this.db.run(
      `CREATE INDEX IF NOT EXISTS membership_chunk_owners ON membership_chunks (chunk, subscription)`,
    )
    this.db.run(
      `CREATE TABLE IF NOT EXISTS membership_writers (subscription TEXT PRIMARY KEY, chunk TEXT NOT NULL, size INTEGER NOT NULL) WITHOUT ROWID`,
    )
    // Legacy memberships become single chunks without rewriting their rows.
    // New chunks are bounded; an old chunk is copied only if a shared view edits it.
    if (old?.["type"] === "table")
      this.db.run(
        `INSERT INTO membership_chunks (subscription, chunk) SELECT id, id FROM subscriptions`,
      )
    // Format v2 removes the redundant primary-key B-tree. Never rewrite a populated
    // cache on wake: old and new layouts share the same queries and can coexist across
    // objects. Upgrade only once all membership state is naturally empty (after GC/reset).
    // DDL and the view replacement are in the caller's transaction, including rollback.
    const tables = ["membership_rows", "membership_chunks", "membership_writers"]
    const legacy =
      this.db.query(
        `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name IN ('membership_rows', 'membership_chunks', 'membership_writers') AND upper(sql) NOT LIKE '%WITHOUT ROWID%' LIMIT 1`,
      ).length > 0
    if (
      legacy &&
      tables.every((table) => this.db.query(`SELECT 1 FROM ${table} LIMIT 1`).length === 0)
    ) {
      this.db.run(`DROP VIEW IF EXISTS membership`)
      for (const table of tables) this.db.run(`DROP TABLE ${table}`)
      // A single recursive call creates the new layout; no user rows or cursors are touched.
      this.initStorage()
      return
    }
    this.db.run(
      `CREATE VIEW IF NOT EXISTS membership AS SELECT c.subscription, r.path, r.tbl, r.key FROM membership_chunks c JOIN membership_rows r ON r.subscription = c.chunk`,
    )
  }

  clear(): void {
    this.db.run(`DELETE FROM membership_writers`)
    this.db.run(`DELETE FROM membership_chunks`)
    this.db.run(`DELETE FROM membership_rows`)
  }

  share(target: string, base: string): void {
    this.db.run(`DELETE FROM membership_writers WHERE subscription IN (?, ?)`, [target, base])
    this.db.run(
      `INSERT OR IGNORE INTO membership_chunks (subscription, chunk) SELECT ?, chunk FROM membership_chunks WHERE subscription = ?`,
      [target, base],
    )
  }

  add(subscription: string, members: ReadonlyArray<Member>): void {
    for (let i = 0; i < members.length;) {
      let writer = this.db.query(
        `SELECT chunk, size FROM membership_writers WHERE subscription = ?`,
        [subscription],
      )[0]
      if (writer === undefined || Number(writer["size"]) >= 512) {
        const chunk = this.newId()
        this.db.run(`INSERT INTO membership_chunks (subscription, chunk) VALUES (?, ?)`, [
          subscription,
          chunk,
        ])
        this.db.run(
          `INSERT INTO membership_writers (subscription, chunk, size) VALUES (?, ?, 0) ON CONFLICT(subscription) DO UPDATE SET chunk = excluded.chunk, size = 0`,
          [subscription, chunk],
        )
        writer = { chunk, size: 0 }
      }
      const size = Number(writer["size"])
      const batch = members.slice(i, i + 512 - size)
      this.db.run(
        `INSERT OR IGNORE INTO membership_rows (subscription, path, tbl, key) SELECT ?, json_extract(value, '$.path'), json_extract(value, '$.table'), json_extract(value, '$.key') FROM json_each(?) j WHERE NOT EXISTS (SELECT 1 FROM membership m WHERE m.subscription = ? AND m.path = json_extract(j.value, '$.path') AND m.tbl = json_extract(j.value, '$.table') AND m.key = json_extract(j.value, '$.key'))`,
        [String(writer["chunk"]), JSON.stringify(batch), subscription],
      )
      this.db.run(`UPDATE membership_writers SET size = size + ? WHERE subscription = ?`, [
        batch.length,
        subscription,
      ])
      i += batch.length
    }
  }

  remove(subscription: string, member: Member): void {
    const chunks = this.db.query(
      `SELECT r.subscription AS chunk FROM membership_rows r JOIN membership_chunks c ON c.chunk = r.subscription WHERE c.subscription = ? AND r.path = ? AND r.tbl = ? AND r.key = ?`,
      [subscription, member.path, member.table, member.key],
    )
    for (const row of chunks) {
      let chunk = String(row["chunk"])
      if (
        this.db.query(
          `SELECT 1 FROM membership_chunks WHERE chunk = ? AND subscription <> ? LIMIT 1`,
          [chunk, subscription],
        ).length > 0
      ) {
        const copy = this.newId()
        this.db.run(
          `INSERT INTO membership_rows (subscription, path, tbl, key) SELECT ?, path, tbl, key FROM membership_rows WHERE subscription = ?`,
          [copy, chunk],
        )
        this.db.run(`UPDATE membership_chunks SET chunk = ? WHERE subscription = ? AND chunk = ?`, [
          copy,
          subscription,
          chunk,
        ])
        // Sharing invalidates writers, so a copied chunk is never a current writer.
        chunk = copy
      }
      this.db.run(
        `DELETE FROM membership_rows WHERE subscription = ? AND path = ? AND tbl = ? AND key = ?`,
        [chunk, member.path, member.table, member.key],
      )
    }
  }

  /** Work is bounded by physical rows removed plus shared/empty references released. */
  collect(subscription: string, budget: number): { spent: number; complete: boolean } {
    this.db.run(`DELETE FROM membership_writers WHERE subscription = ?`, [subscription])
    let spent = 0
    const chunks = this.db.query(
      `SELECT chunk FROM membership_chunks WHERE subscription = ? LIMIT ?`,
      [subscription, budget],
    )
    for (const row of chunks) {
      if (spent >= budget) break
      const chunk = String(row["chunk"])
      const shared =
        this.db.query(
          `SELECT 1 FROM membership_chunks WHERE chunk = ? AND subscription <> ? LIMIT 1`,
          [chunk, subscription],
        ).length > 0
      if (!shared) {
        this.db.run(
          `DELETE FROM membership_rows WHERE (subscription, path, tbl, key) IN (SELECT subscription, path, tbl, key FROM membership_rows WHERE subscription = ? LIMIT ?)`,
          [chunk, budget - spent],
        )
        const deleted = Number(this.db.query(`SELECT changes() AS n`)[0]?.["n"] ?? 0)
        spent += Math.max(1, deleted)
        if (
          this.db.query(`SELECT 1 FROM membership_rows WHERE subscription = ? LIMIT 1`, [chunk])
            .length > 0
        )
          continue
      } else spent += 1
      this.db.run(`DELETE FROM membership_chunks WHERE subscription = ? AND chunk = ?`, [
        subscription,
        chunk,
      ])
    }
    return {
      spent,
      complete:
        this.db.query(`SELECT 1 FROM membership_chunks WHERE subscription = ? LIMIT 1`, [
          subscription,
        ]).length === 0,
    }
  }

  drop(subscription: string): void {
    while (!this.collect(subscription, 1000).complete) {
      /* explicit full removal */
    }
  }
}
