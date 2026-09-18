# Incremental view maintenance

## Goal

Each client subscribes to queries. The Durable Object must tell each client which rows enter or leave a result after every source transaction. It must do this with work that depends on the rows the transaction touched, not on the size of the partition: an organization with 100,000 documents must cost the same per change as one with 100. Orbit maintains every subscription incrementally, level by level, with indexed lookups.

## Queries and subscriptions

A query is data, not code. The AST in `packages/protocol/src/query-ast.ts` names one table, an optional predicate, an ordering, a limit, and relations to include. `planQuery` in `packages/query/src/plan.ts` validates the query against the schema and normalizes it. The primary key is always appended to `orderBy`, so the order is total and a limited window is well defined. Includes are sorted. The canonical JSON of the normalized query is the subscription id. Identical queries from different clients share one materialization.

The planner also enforces limits:

- `MAX_LIMIT`: 10000 rows.
- `MAX_IN_VALUES`: 40 values per `in`.
- `MAX_PREDICATE_NODES`: 60 nodes.
- `MAX_PREDICATE_PARAMS`: 80 bound parameters for the predicate.
- `MAX_RELATION_DEPTH`: 3 levels of includes and of `exists`.

A subscription stores its normalized query, the set of tables it depends on, a `live` flag and `orphaned_at` (see "Grace period"). The `membership` table stores `(subscription, path, table, key)` for every row in the result. `path` is `""` for the primary rows and the include path (`folder`, `tags/tag`) for included rows. A row reached through two paths has two membership rows; the client-facing membership is the distinct set of `(table, key)`.

## Levels

A subscription is a tree of levels (`levelsOf` in `packages/sync-do/src/core/maintain.ts`): the primary level and one level per include at any depth. Each level owns the membership rows of its path. The invariant maintained for every level is:

- Primary level: the first `limit` rows (all rows without a limit) of the primary table that satisfy the predicate, in the query's order.
- Include level: the rows of its target table that satisfy the include filter and are referenced through the relation by at least one row of the parent level.

Recording the path is what makes the invariant local: with a self relation (`Chatbot.folder` targets `Chatbot`), the same row can be a primary row and an included row at once, and each level decides for itself.

## One transaction

`maintainSubscriptions` in `packages/sync-do/src/core/engine.ts` runs after every applied transaction, inside the same SQLite transaction. It receives the applied changes grouped by table. For each live subscription whose tables intersect, a `SubscriptionMaintainer` does the following.

### Primary level

1. Collect candidates. A change to the primary table makes its row a candidate. A change to a table reached by an `exists` chain makes every primary row that reaches the changed row a candidate: `compileChainCandidates` walks the chain backwards with indexed `EXISTS` hops and compares the last hop with the changed image's relation columns, so the changed table itself is never read. A row changed several times in one transaction collapses to one candidate whose `before` image is the image at the start of the transaction.
2. Classify each candidate with two indexed queries over the candidate keys: is it a member (`membership` by primary key), and does its current cache row satisfy the predicate (`compileWhere` over the row, so `exists` reads the post-transaction related rows). A deleted row never matches.
3. Apply, in this order: rows that leave, then the free places of a limited window, then the children of rows that stayed, then rows that enter. Every reference check reads the current membership, so leaving rows must be gone before another row's children are re-derived.

A limited query keeps a top-k window:

- A row enters a full window only when it precedes the last member. One statement orders the members plus the candidate and returns the last one; when that is the candidate, nothing changes, otherwise the candidate enters and the last member leaves.
- When rows left a full window, the free places are filled with the first non-members in order (`LIMIT` free places). Rows that started to match in this transaction compete there.
- When a member's sort key changed while the window is full, the best non-member is compared with the last member, and they swap while the non-member precedes.

The two window repairs read the primary table in the query's order (bounded by `LIMIT`) and are the only steps whose cost grows with the table. Everything else is an index probe.

### Include levels

A row enters a level through its parent: when a parent row enters, `compileRowsByColumns` fetches the rows of the target whose `to_columns` equal the parent's `from_columns` values (index probe), filtered by the include's `where`, and each of them enters unless present. When a parent row leaves, the same rows are candidates to leave; each leaves unless another parent-level member still references it, which one indexed join of the parent table with the parent level's membership decides. When a member row's `from_columns` change, the rows it referenced before and after are diffed the same way. Every entry and exit cascades to the child levels.

A change to a row of an include target is also handled directly: the row should be present exactly when it exists, satisfies the filter, and a parent-level member references it. Levels are processed parents first, so the parent level is final when a child level looks at it.

### Membership delta

Every insert and delete records whether the `(table, key)` was a member through any path before its first modification in this transaction. After all levels are done, a row that had no path and now has one is `added`; a row that had a path and now has none is `removed`. This is the `MembershipChange` the clients receive.

## Materialization

The first subscription to a query, and every re-materialization after a scope reset, evaluates the whole query: `compileSelect` for the primary rows and `compileIncludeSelect` for every include at every depth (`flattenIncludes`, parents first). The include select returns the target rows that the parent level references:

```sql
SELECT r."__key", ... FROM "t_Chatbot" r
WHERE r."id" IN (SELECT p."groupId" FROM (<primary select>) p)
ORDER BY r."__key"
```

The `IN (SELECT ...)` subquery is not correlated. SQLite evaluates the parent level once into an ephemeral index, and every candidate row probes that index. A correlated `EXISTS` over the same derived table would re-run the parent select for every candidate row, which is quadratic in the cache size: with 2,000 documents, one insert cost more than one second per subscription in that form. A relation with several columns uses a row value on the left: `(r."a", r."b") IN (SELECT p."x", p."y" FROM ...)`.

`materialize` writes only the difference between the stored membership and the evaluated result, so re-materializing an unchanged subscription writes nothing.

## Indexes

Every maintenance lookup is an index probe. `createIndexesSql` in `packages/schema/src/ddl.ts` creates an index on the source side (`from_columns`) and on the target side (`to_columns`) of every declared relation, in the Durable Object and in the browser cache. Columns that are a prefix of the primary key are covered by its unique index. The engine runs the index DDL on every start, so an existing cache gains the indexes it lacks.

## Parameter limits

Durable Object SQLite binds at most 100 parameters per statement. The engine limits itself to `MAX_BOUND_PARAMS = 90`. Candidate keys are classified in chunks that leave room for the predicate's own parameters. The test driver in `packages/sync-do/test/core/support/node-driver.ts` throws on more than 100 parameters, so the tests catch a regression.

## Grace period

When the last session leaves, the subscription becomes orphaned and stops incremental maintenance immediately. Its membership stays for `subscriptionGraceMs` (default one hour). A server-owned version proof permits reuse only while the underlying cache is unchanged; otherwise a returning subscriber reconciles the view. An alarm reclaims at most 1,000 physical membership rows or shared chunk references per pass, examining at most 16 due subscriptions. A partial sweep invalidates the proof before deleting anything, so reopening between passes rebuilds a complete view.

## Shared window membership

Growing windows share the membership of their smaller live prefix. `membership_chunks` maps subscriptions to immutable shared chunks, and `membership_rows` stores each chunk's `(path, table, key)` entries. The read-only `membership` view presents the same logical set to incremental maintenance. New chunks contain at most 512 entries. Appending a window writes its new entries plus chunk references instead of copying the whole prefix. A removal clones only the affected chunk when another subscription owns it; unrelated windows keep their original membership. Sharing invalidates the writable tail, preventing later inserts from leaking into another view. Allocation, writers, ownership and cleanup are transactional SQLite state and survive rollback or object eviction.

Old per-subscription membership tables are renamed and adopted as legacy chunks without rewriting their rows. The first edit of a shared legacy chunk may copy that larger chunk; subsequent newly allocated chunks use the 512-entry bound. Cache-format migration is forward-only: rolling back to a Worker predating chunk membership requires rebuilding its disposable DO cache. No source database or client protocol changes are involved.

## Delta contents

The `delta` event of one transaction carries:

- `cursor`: the `seq` of the transaction.
- `origin`: gtid, commit timestamp, `seq`, trace timestamps and the apply time.
- `memberships`: one `MembershipChange` per affected subscription with `added` and `removed` references.
- `rows`: row images. Every added member travels with its image. A deleted row travels as `row: null` so clients drop it in any case. A changed row that is a member of any subscription travels with its new image, read back from the cache after the change.

A change to a row that is not a member and does not enter any result produces no delta content.

## Delivery to sessions

The Durable Object sends each `delta` to every session with at least one live subscription. Before it sends, it:

- Maps engine subscription ids to the client subscription ids of that session.
- Keeps a row only when it is a delete, or when it is a member of a subscription of that session (one indexed lookup over the delta's rows, shared by all sessions).
- Projects each row to the columns the client knows, when the client schema is compatible but not identical.

A session with no relevant membership change and no relevant row receives nothing for that transaction.

A session that subscribes to a query other sessions already hold receives the snapshot alone. The other sessions have the rows; a preload of 100,000 rows is not resent to everyone each time a client joins. Snapshots that come from a fill completion or a re-materialization go to every session of the subscription, because those sessions were waiting for them.

## The client side

`LocalStore.applyDelta` in `packages/client/src/store.ts` turns one delta into one atomic batch:

1. Delete removed membership rows and insert added ones.
2. Upsert each row image, but only when some membership row references it.
3. Delete each removed row that no subscription references any more.
4. Store the cursor.

The client engine then re-runs every live query whose subscription or tables were touched. `readSubscription` filters the local table by the membership of that subscription, so a live query shows exactly the members the server computed. Includes are attached in memory from the local include rows.

## Cost per change

Measured with the engine core on Node (`docs/benchmarks.md` has the full table): on a partition with 100,000 documents and the example schema's 11 subscriptions, one insert or one rename costs a few milliseconds and a handful of rows written, and the cost does not change between 2,000 and 100,000 documents. The full re-evaluation the engine used before cost 1.2 s per insert at 2,000 documents and grew linearly.

## Tests

The engine tests in `packages/sync-do/test/core/engine.test.ts` check the mechanism against a full recompute:

- `materializes, then emits deltas with membership changes and row images`: a window of two rows with includes, through insert, update and delete.
- `applies 300 inserts in one transaction while predicate subscriptions are live`: many candidates through the chunked classification.
- `randomized incremental maintenance equals full recomputation`: 12 rounds of 80 random steps (up to three changes per transaction, including the same row several times, and changes to folders as well as documents) over 9 query shapes: windows in both directions, `or`/`not` predicates, `exists` chains of depth 1 and 2, negated nested `exists`, filtered and nested includes, and a self relation at two levels under a window. After every step, `membershipOf` must equal `recompute` for every subscription, and every delta row image must equal the cache row.

## Related documents

- [consistency-invariants.md](consistency-invariants.md): invariant 6.
- [bootstrap.md](bootstrap.md): how a subscription becomes live.
- [scalability-limits.md](scalability-limits.md): what still grows with the partition.
