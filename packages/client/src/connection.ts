/**
 * WebSocket connection to a Sync Durable Object with reconnect and typed messages.
 *
 * The connection is a scoped Effect resource: opening it starts a fiber that keeps the socket
 * alive, reconnecting with exponential backoff and jitter; closing the scope tears everything
 * down. Incoming messages are validated against the protocol schema and delivered through a
 * queue; malformed messages are surfaced as typed errors, never dropped.
 */

import { Data, Effect, Queue, Ref, Schema, Stream, type Scope } from "effect"
import {
  CloseCode,
  ClientMessage,
  ServerMessage,
  type ClientMessage as ClientMessageType,
  type ServerMessage as ServerMessageType,
} from "@orbit/protocol/client"

export type ConnectionState =
  | { readonly status: "connecting"; readonly attempt: number }
  | { readonly status: "open" }
  | {
      readonly status: "reconnecting"
      readonly attempt: number
      readonly retryInMs: number
      readonly lastError: string
    }
  | {
      readonly status: "closed"
      readonly reason: string
      readonly fatal: boolean
      readonly code: number | null
    }

export class ConnectionError extends Data.TaggedError("ConnectionError")<{
  readonly message: string
  readonly code: number | null
  readonly fatal: boolean
}> {}

export interface ConnectionConfig {
  /** WebSocket URL including partition and token query parameters (or a function producing it). */
  readonly url: () => Effect.Effect<string, ConnectionError>
  /** Called on every (re)connect so the caller can send `hello`. */
  readonly onOpen: (send: (m: ClientMessageType) => void) => Effect.Effect<void>
  readonly makeWebSocket?: (url: string) => WebSocket
  readonly backoffMinMs?: number
  readonly backoffMaxMs?: number
  /**
   * Heartbeat: a `ping` is sent every `pingIntervalMs`; when no `pong` arrives within
   * `pongTimeoutMs` the socket is closed so the reconnect loop takes over. An established
   * WebSocket does not notice a dead network by itself.
   */
  readonly pingIntervalMs?: number
  readonly pongTimeoutMs?: number
}

export type ConnectionEvent =
  | { readonly type: "state"; readonly state: ConnectionState }
  | { readonly type: "message"; readonly message: ServerMessageType }

const decodeServer = Schema.decodeUnknownEffect(ServerMessage)
const encodeClient = Schema.encodeSync(ClientMessage)

/** Close codes that mean "do not retry": the client must change something first. */
const FATAL_CLOSE_CODES = new Set<number>([
  CloseCode.protocolVersionMismatch,
  CloseCode.unauthorized,
  CloseCode.partitionDenied,
  CloseCode.schemaMismatch,
])

export interface Connection {
  readonly events: Stream.Stream<ConnectionEvent>
  readonly send: (message: ClientMessageType) => Effect.Effect<boolean>
  readonly state: Effect.Effect<ConnectionState>
}

interface AttemptResult {
  readonly code: number
  readonly reason: string
  readonly opened: boolean
}

/** Socket `error` events carry no detail; the `close` event that follows has the code. */
const ignoreSocketError = (): void => {}

/** State reported at the start of connection attempt `n` (the first attempt is `connecting`). */
const attemptState = (n: number): ConnectionState =>
  n === 0
    ? { status: "connecting", attempt: n }
    : { status: "reconnecting", attempt: n, retryInMs: 0, lastError: "" }

export const makeConnection = (
  config: ConnectionConfig,
): Effect.Effect<Connection, never, Scope.Scope> =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<ConnectionEvent>()
    const state = yield* Ref.make<ConnectionState>({ status: "connecting", attempt: 0 })
    const socket = yield* Ref.make<WebSocket | null>(null)
    const backoffMin = config.backoffMinMs ?? 500
    const backoffMax = config.backoffMaxMs ?? 30_000
    // A dead link is detected within ping + pong timeout even when the browser fires no
    // `offline` event (that event is not reliable while the page is busy).
    const pingInterval = config.pingIntervalMs ?? 10_000
    const pongTimeout = config.pongTimeoutMs ?? 5_000
    const makeWebSocket = config.makeWebSocket ?? ((url: string) => new WebSocket(url))
    const hasWindow = typeof window !== "undefined" && typeof window.addEventListener === "function"

    /** Resolves when the browser reports it is back online (never, outside a browser). */
    const awaitOnline: Effect.Effect<void> = hasWindow
      ? Effect.callback<void>((resume) => {
          const handler = () => resume(Effect.void)
          window.addEventListener("online", handler)
          return Effect.sync(() => window.removeEventListener("online", handler))
        })
      : Effect.never

    const setState = (s: ConnectionState): Effect.Effect<void> =>
      Ref.set(state, s).pipe(
        Effect.andThen(Queue.offer(events, { type: "state", state: s })),
        Effect.asVoid,
      )

    const send = (message: ClientMessageType): Effect.Effect<boolean> =>
      Ref.get(socket).pipe(
        Effect.map((ws) => {
          if (ws === null || ws.readyState !== WebSocket.OPEN) return false
          ws.send(JSON.stringify(encodeClient(message)))
          return true
        }),
      )

    /** One connection attempt: resolves when the socket closes (with the close reason). */
    const attempt = (n: number): Effect.Effect<AttemptResult, ConnectionError> =>
      Effect.gen(function* () {
        yield* setState(attemptState(n))
        // The browser already knows it is offline: do not open a socket that cannot connect.
        // The loop then waits for the `online` event instead of a full backoff.
        if (hasWindow && !navigator.onLine) return { code: 4001, reason: "offline", opened: false }
        const url = yield* config.url()
        const ws = makeWebSocket(url)
        yield* Ref.set(socket, ws)
        let opened = false
        let lastPongAt = 0
        let lastPingAt = 0
        let heartbeat: ReturnType<typeof setInterval> | null = null
        // Each attempt owns a fresh socket, so exactly one listener per event is attached to it.
        const closed = yield* Effect.callback<AttemptResult>((resume) => {
          let settled = false
          const detach = () => {
            if (heartbeat !== null) clearInterval(heartbeat)
            if (hasWindow) window.removeEventListener("offline", onOffline)
            ws.removeEventListener("open", onOpen)
            ws.removeEventListener("message", onMessage)
            ws.removeEventListener("error", ignoreSocketError)
            ws.removeEventListener("close", onClose)
          }
          const finish = (result: AttemptResult) => {
            if (settled) return
            settled = true
            detach()
            resume(Effect.succeed(result))
          }
          /**
           * The client decided the link is dead. The attempt ends now: a browser that is offline
           * never completes the close handshake, so waiting for the `close` event would leave the
           * connection "open" for as long as the network is down.
           */
          const terminate = (code: number, reason: string) => {
            finish({ code, reason, opened })
            if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)
              ws.close(code, reason)
          }
          const onOffline = () => terminate(4001, "offline")
          const onOpen = () => {
            opened = true
            lastPongAt = Date.now()
            heartbeat = setInterval(() => {
              if (ws.readyState !== WebSocket.OPEN) return
              if (lastPingAt > lastPongAt && Date.now() - lastPingAt > pongTimeout) {
                terminate(4000, "heartbeat timeout")
                return
              }
              lastPingAt = Date.now()
              try {
                ws.send(JSON.stringify(encodeClient({ type: "ping", sentAt: lastPingAt })))
              } catch {
                terminate(4000, "send failed")
              }
            }, pingInterval)
            Effect.runFork(
              setState({ status: "open" }).pipe(
                Effect.andThen(config.onOpen((m) => ws.send(JSON.stringify(encodeClient(m))))),
              ),
            )
          }
          const onMessage = (event: MessageEvent<unknown>) => {
            const data = typeof event.data === "string" ? event.data : ""
            Effect.runFork(
              Effect.gen(function* () {
                const parsedResult = yield* Effect.result(
                  Effect.try({
                    try: (): unknown => JSON.parse(data),
                    catch: () => "non-json" as const,
                  }),
                )
                if (parsedResult._tag === "Failure") {
                  yield* Queue.offer(events, {
                    type: "message",
                    message: {
                      type: "error",
                      error: { code: "invalid_message", message: "server sent non-JSON" },
                      fatal: true,
                    },
                  })
                  return
                }
                const decoded = yield* Effect.result(decodeServer(parsedResult.success))
                if (decoded._tag === "Failure") {
                  yield* Queue.offer(events, {
                    type: "message",
                    message: {
                      type: "error",
                      error: {
                        code: "invalid_message",
                        message: `server message failed validation: ${String(decoded.failure)}`,
                      },
                      fatal: true,
                    },
                  })
                  return
                }
                if (decoded.success.type === "pong") lastPongAt = Date.now()
                yield* Queue.offer(events, { type: "message", message: decoded.success })
              }),
            )
          }
          const onClose = (event: CloseEvent) =>
            finish({ code: event.code, reason: event.reason, opened })
          ws.addEventListener("open", onOpen)
          ws.addEventListener("message", onMessage)
          ws.addEventListener("error", ignoreSocketError)
          ws.addEventListener("close", onClose)
          // Registered for the whole attempt, so a socket that is still connecting is closed too.
          if (hasWindow) window.addEventListener("offline", onOffline)
          return Effect.sync(() => {
            detach()
            if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)
              ws.close(1000, "client closed")
          })
        })
        yield* Ref.set(socket, null)
        return closed
      })

    const loop = Effect.gen(function* () {
      let n = 0
      let backoff = backoffMin
      for (;;) {
        const result = yield* Effect.result(attempt(n))
        if (result._tag === "Failure") {
          yield* setState({
            status: "closed",
            reason: result.failure.message,
            fatal: result.failure.fatal,
            code: result.failure.code,
          })
          if (result.failure.fatal) return
        } else {
          const { code, reason, opened } = result.success
          if (FATAL_CLOSE_CODES.has(code)) {
            yield* setState({ status: "closed", reason, fatal: true, code })
            return
          }
          if (opened) backoff = backoffMin
        }
        n += 1
        const jitter = Math.floor(Math.random() * backoff * 0.3)
        const wait = Math.min(backoff + jitter, backoffMax)
        yield* setState({
          status: "reconnecting",
          attempt: n,
          retryInMs: wait,
          lastError:
            result._tag === "Failure" ? result.failure.message : `closed ${result.success.code}`,
        })
        // Wait out the backoff, but reconnect at once when the browser reports it is online again.
        yield* Effect.race(Effect.sleep(`${wait} millis`), awaitOnline)
        backoff = Math.min(backoff * 2, backoffMax)
      }
    })

    yield* Effect.forkScoped(loop)
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const ws = yield* Ref.get(socket)
        if (ws !== null) ws.close(1000, "client closed")
        yield* Queue.shutdown(events)
      }),
    )

    return { events: Stream.fromQueue(events), send, state: Ref.get(state) }
  })
