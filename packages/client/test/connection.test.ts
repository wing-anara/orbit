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
