import { Effect, Stream } from "effect"
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
