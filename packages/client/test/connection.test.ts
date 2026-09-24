import { Deferred, Effect, Stream } from "effect"
import { expect, it } from "vitest"

import { makeConnection } from "../src/connection.ts"

class Socket extends EventTarget {
  readyState = 1
  send(): void {}
  close(): void {
    this.readyState = 3
  }
}

it("delivers large snapshots and subsequent frames in socket order", async () => {
  const socket = new Socket()
  const received = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* makeConnection({
          target: () => Effect.succeed({ url: "ws://test", protocols: [] }),
          onOpen: () => Effect.void,
          makeWebSocket: () => socket as unknown as WebSocket,
        })
        yield* Effect.sleep("10 millis")
        socket.dispatchEvent(new Event("open"))
        const rows = Array.from({ length: 2000 }, (_, i) => ({
          table: "items",
          key: [String(i)],
          row: { id: String(i), name: "test" },
        }))
        for (const message of [
          {
            type: "snapshot",
            subscriptionId: "s",
            cursor: 1,
            rows,
            members: rows.map(({ table, key }) => ({ table, key })),
            complete: false,
          },
          {
            type: "snapshot",
            subscriptionId: "s",
            cursor: 1,
            rows: [],
            members: [],
            complete: true,
          },
          { type: "pong", sentAt: 1, serverTime: 2 },
        ])
          socket.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(message) }))
        return yield* connection.events.pipe(
          Stream.filter((event) => event.type === "message"),
          Stream.take(3),
          Stream.runCollect,
        )
      }),
    ),
  )
  expect(
    Array.from(received).map((event) => (event.type === "message" ? event.message : null)),
  ).toMatchObject([
    { type: "snapshot", complete: false },
    { type: "snapshot", complete: true },
    { type: "pong" },
  ])
})

it("holds application messages and heartbeats until hello passes a busy client lock", async () => {
  const socket = new Socket()
  const sent: Array<{ type: string }> = []
  socket.send = (data?: string) => {
    sent.push(JSON.parse(data!))
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>()
        const connection = yield* makeConnection({
          target: () => Effect.succeed({ url: "ws://test", protocols: [] }),
          makeWebSocket: () => socket as unknown as WebSocket,
          pingIntervalMs: 5,
          onOpen: (send) =>
            Deferred.await(release).pipe(
              Effect.andThen(
                Effect.sync(() =>
                  send({
                    type: "hello",
                    protocolVersion: 1,
                    clientId: "test",
                    token: "",
                    partition: "org",
                    schema: { schemaHash: "test", tables: [] },
                    cursor: null,
                    subscriptions: [],
                  }),
                ),
              ),
            ),
        })
        yield* Effect.sleep("10 millis")
        socket.dispatchEvent(new Event("open"))
        yield* Effect.sleep("25 millis")
        expect(yield* connection.send({ type: "unsubscribe", id: "old" })).toBe(false)
        expect(sent).toEqual([])
        yield* Deferred.succeed(release, undefined)
        yield* Effect.sleep("10 millis")
        expect(yield* connection.send({ type: "unsubscribe", id: "old" })).toBe(true)
        expect(sent[0]?.type).toBe("hello")
      }),
    ),
  )
})

const renewalHello = {
  type: "hello" as const,
  protocolVersion: 1,
  clientId: "c",
  token: "",
  partition: "org",
  schema: { schemaHash: "test", tables: [] },
  cursor: null,
  subscriptions: [{ type: "subscribe" as const, id: "view", query: { name: "mine", args: {} } }],
}
const renewalWelcome = (extra: Record<string, unknown> = {}) => ({
  type: "welcome",
  protocolVersion: 1,
  sessionId: "s",
  partition: "org",
  schemaHash: "test",
  cursor: 0,
  serverTime: 1_000_000,
  ...extra,
})
const message = (socket: Socket, data: unknown) =>
  socket.dispatchEvent(
    new MessageEvent("message", { data: typeof data === "string" ? data : JSON.stringify(data) }),
  )

it("renews fresh credentials on the same socket, uses server-relative time", async () => {
  let targets = 0
  let opens = 0
  const sent: string[] = []
  const calls: Array<{ url: string; init: RequestInit }> = []
  const socket = new Socket()
  socket.send = (data?: string) => sent.push(data!)
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* makeConnection({
          target: () =>
            Effect.sync(() => ({
              url: "wss://orbit.test/orbit/ws?partition=org",
              protocols: ["orbit", `orbit.token.token${++targets}`],
            })),
          makeWebSocket: () => {
            opens++
            return socket as unknown as WebSocket
          },
          onOpen: (send) => Effect.sync(() => send(renewalHello)),
          fetch: async (input, init) => {
            calls.push({ url: String(input), init: init! })
            return Response.json({ expiresAt: 2_000_000, serverTime: 1_000_300 })
          },
          pingIntervalMs: 10000,
        })
        yield* Effect.sleep("10 millis")
        socket.dispatchEvent(new Event("open"))
        message(socket, renewalWelcome({ sessionRenewal: { expiresAt: 1_000_300 } }))
        yield* connection.send({
          type: "subscribe",
          id: "new",
          query: { name: "folder", args: { id: "f" } },
        })
        yield* connection.send({ type: "unsubscribe", id: "view" })
        yield* Effect.sleep("350 millis")
        expect(opens).toBe(1)
        expect(targets).toBe(2)
        expect(connection.generation()).toBe(1)
        expect(sent.filter((s) => JSON.parse(s).type === "hello")).toHaveLength(1)
        expect(calls).toHaveLength(1)
        expect(calls[0]!.url).toBe("https://orbit.test/orbit/session/renew?partition=org")
        expect(calls[0]!.init).toMatchObject({
          method: "POST",
          credentials: "omit",
          redirect: "error",
          headers: { authorization: "Bearer token2" },
        })
        expect(JSON.parse(String(calls[0]!.init.body))).toEqual({ sessionId: "s" })
      }),
    ),
  )
})

it.each(["denied", "malformed", "target-changed", "timeout"])(
  "fails closed and reconnects when renewal is %s",
  async (mode) => {
    let targets = 0
    const sockets: Socket[] = []
    let calls = 0
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* makeConnection({
            target: () =>
              Effect.sync(() => ({
                url: `wss://${mode === "target-changed" && ++targets > 1 ? "different" : "orbit"}.test/orbit/ws?partition=org`,
                protocols: ["orbit", "orbit.token.fresh"],
              })),
            makeWebSocket: () => {
              const ws = new Socket()
              sockets.push(ws)
              return ws as unknown as WebSocket
            },
            onOpen: (send) => Effect.sync(() => send(renewalHello)),
            fetch: async (_input, init) => {
              calls++
              if (mode === "timeout")
                return new Promise<Response>((_resolve, reject) =>
                  init?.signal?.addEventListener("abort", () => reject(Error("aborted"))),
                )
              return mode === "denied"
                ? new Response(null, { status: 403 })
                : Response.json({ expiresAt: "bad" })
            },
            backoffMinMs: 1,
            backoffMaxMs: 1,
            pingIntervalMs: 10000,
          })
          yield* Effect.sleep("10 millis")
          sockets[0]!.dispatchEvent(new Event("open"))
          message(sockets[0]!, renewalWelcome({ sessionRenewal: { expiresAt: 1_000_200 } }))
          yield* Effect.sleep("300 millis")
          expect(sockets[0]!.readyState).toBe(3)
          expect(connection.generation()).toBeGreaterThan(1)
          expect(calls).toBe(mode === "target-changed" ? 0 : 1)
        }),
      ),
    )
  },
)

it("keeps legacy heartbeat compatibility and does not renew when the server omits capabilities", async () => {
  const socket = new Socket()
  const sent: string[] = []
  let targets = 0
  socket.send = (data?: string) => {
    sent.push(data!)
    const value = JSON.parse(data!)
    if (value.type === "ping")
      message(socket, { type: "pong", sentAt: value.sentAt, serverTime: 1 })
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* makeConnection({
          target: () =>
            Effect.sync(() => {
              targets++
              return { url: "ws://orbit.test/orbit/ws?partition=org", protocols: [] }
            }),
          makeWebSocket: () => socket as unknown as WebSocket,
          onOpen: (send) => Effect.sync(() => send(renewalHello)),
          pingIntervalMs: 10,
          pongTimeoutMs: 30,
          fetch: async () => {
            throw Error("must not renew")
          },
        })
        yield* Effect.sleep("10 millis")
        socket.dispatchEvent(new Event("open"))
        message(socket, renewalWelcome())
        yield* Effect.sleep("100 millis")
        expect(sent.some((s) => JSON.parse(s).type === "ping")).toBe(true)
        expect(targets).toBe(1)
        expect(connection.generation()).toBe(1)
      }),
    ),
  )
})

it("negotiates native heartbeat and reconnects on a missing pong without accepting an old socket's pong", async () => {
  const sockets: Socket[] = []
  const native: number[] = []
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* makeConnection({
          target: () =>
            Effect.succeed({ url: "ws://orbit.test/orbit/ws?partition=org", protocols: [] }),
          makeWebSocket: () => {
            const socket = new Socket()
            const index = sockets.length
            sockets.push(socket)
            socket.send = (data?: string) => {
              if (data === "orbit:ping:v1") {
                native.push(index)
                // Stale response from the first socket must not acknowledge the second socket.
                if (index > 0) message(sockets[0]!, "orbit:pong:v1")
              }
            }
            setTimeout(() => {
              socket.dispatchEvent(new Event("open"))
              message(socket, renewalWelcome({ heartbeat: "static-v1" }))
            }, 5)
            return socket as unknown as WebSocket
          },
          onOpen: (send) => Effect.sync(() => send(renewalHello)),
          pingIntervalMs: 15,
          pongTimeoutMs: 20,
          backoffMinMs: 1,
          backoffMaxMs: 1,
        })
        yield* Effect.sleep("160 millis")
        expect(native).toContain(0)
        expect(native).toContain(1)
        expect(sockets[0]!.readyState).toBe(3)
        expect(sockets[1]!.readyState).toBe(3)
        expect(connection.generation()).toBeGreaterThanOrEqual(3)
      }),
    ),
  )
})

it("does not loop on a cached token and cancels pending renewal when the scope closes", async () => {
  let calls = 0
  let tokens = 0
  const socket = new Socket()
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        yield* makeConnection({
          target: () =>
            Effect.sync(() => {
              tokens++
              return {
                url: "ws://orbit.test/orbit/ws?partition=org",
                protocols: ["orbit.token.cached"],
              }
            }),
          makeWebSocket: () => socket as unknown as WebSocket,
          onOpen: (send) => Effect.sync(() => send(renewalHello)),
          pingIntervalMs: 10000,
          fetch: async () => {
            calls++
            return Response.json({ expiresAt: 1_000_200, serverTime: 1_000_170 })
          },
        })
        yield* Effect.sleep("10 millis")
        socket.dispatchEvent(new Event("open"))
        message(socket, renewalWelcome({ sessionRenewal: { expiresAt: 1_000_200 } }))
        yield* Effect.sleep("400 millis")
        expect(calls).toBe(1)
        expect(tokens).toBe(2)
      }),
    ),
  )
  await new Promise((resolve) => setTimeout(resolve, 50))
  expect(calls).toBe(1)
})

it("accepts a pending legacy pong when native mode is negotiated mid-flight", async () => {
  const socket = new Socket()
  let legacy = 0
  let native = 0
  socket.send = (data?: string) => {
    if (data === "orbit:ping:v1") {
      native++
      message(socket, "orbit:pong:v1")
      return
    }
    const sent = JSON.parse(data!)
    if (sent.type === "ping") {
      legacy++
      message(socket, renewalWelcome({ heartbeat: "static-v1" }))
      setTimeout(() => message(socket, { type: "pong", sentAt: sent.sentAt, serverTime: 1 }), 5)
    }
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* makeConnection({
          target: () =>
            Effect.succeed({ url: "ws://orbit.test/orbit/ws?partition=org", protocols: [] }),
          makeWebSocket: () => socket as unknown as WebSocket,
          onOpen: (send) => Effect.sync(() => send(renewalHello)),
          pingIntervalMs: 15,
          pongTimeoutMs: 40,
        })
        yield* Effect.sleep("10 millis")
        socket.dispatchEvent(new Event("open"))
        yield* Effect.sleep("100 millis")
        expect(legacy).toBe(1)
        expect(native).toBeGreaterThan(1)
        expect(connection.generation()).toBe(1)
      }),
    ),
  )
})
