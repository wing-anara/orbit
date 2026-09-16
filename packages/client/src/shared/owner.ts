import type { MutationHandle } from "../mutations.ts"
import { decodeArgs, type MutatorDefinitions } from "@orbit/mutators"
import { NamedQueryCall, TypedQuery, type IncludeShape } from "@orbit/query"
import type { SyncSchemaDefinition } from "@orbit/schema"
import { createOrbitClient, type LiveQuery, type OrbitClientConfig } from "../client.ts"
import { queryKey, type Command, type Event, type WireQuery } from "./protocol.ts"

const callable = (value: unknown): value is (args: unknown) => MutationHandle =>
  typeof value === "function"

export type Definition =
  | SyncSchemaDefinition<never, unknown>
  | { readonly _tag: "SyncSchemaDefinition" }
export interface SharedOwner {
  join(peer: string): void
  leave(peer: string): Promise<void>
  command(peer: string, command: Command): Promise<void>
  abort(): void
  close(): Promise<void>
}

export const openSharedOwner = async <D extends Definition, M extends MutatorDefinitions<D>>(
  config: OrbitClientConfig<D, M>,
  send: (peer: string | null, event: Event) => void,
): Promise<SharedOwner> => {
  const abort = new AbortController()
  const sockets = new Set<WebSocket>()
  const client = await createOrbitClient({
    ...config,
    signal:
      config.signal === undefined ? abort.signal : AbortSignal.any([abort.signal, config.signal]),
    makeWebSocket: (url, protocols) => {
      if (abort.signal.aborted || config.signal?.aborted) throw new Error("Shared owner closed")
      const socket = config.makeWebSocket?.(url, protocols) ?? new WebSocket(url, [...protocols])
      sockets.add(socket)
      socket.addEventListener("close", () => sockets.delete(socket), { once: true })
      return socket
    },
  })
  const queries = new Map<
    string,
    {
      query: LiveQuery<Record<string, unknown>>
      off: () => void
      peers: Map<string, Set<string>>
    }
  >()
  const peers = new Map<string, Map<string, string>>()
  const queryOf = (
    wire: WireQuery,
  ): TypedQuery<D, string, IncludeShape> | NamedQueryCall<D, string, IncludeShape> => {
    const local = new TypedQuery<D, string, IncludeShape>(wire.ast)
    return wire.ref === undefined ? local : new NamedQueryCall(wire.ref, () => local)
  }
  const offStatus = client.onStatus(() =>
    send(null, { type: "status", status: client.getStatus() }),
  )
  const offMutation = client.onMutation((event) => send(null, { type: "mutation", event }))
  const release = async (peer: string, id: string): Promise<void> => {
    const refs = peers.get(peer)
    const key = refs?.get(id)
    if (key === undefined) return
    refs?.delete(id)
    const entry = queries.get(key)
    if (entry === undefined) return
    const ids = entry.peers.get(peer)
    ids?.delete(id)
    if (ids?.size === 0) entry.peers.delete(peer)
    if (entry.peers.size === 0) {
      queries.delete(key)
      entry.off()
      await entry.query.release()
    }
  }
  return {
    join: (peer) => {
      if (!peers.has(peer)) peers.set(peer, new Map())
      send(peer, { type: "ready", clientId: client.clientId, status: client.getStatus() })
    },
    leave: async (peer) => {
      peers.delete(peer)
      for (const [key, entry] of queries) {
        if (!entry.peers.delete(peer)) continue
        if (entry.peers.size === 0) {
          queries.delete(key)
          entry.off()
          await entry.query.release()
        }
      }
    },
    command: async (peer, command) => {
      const refs = peers.get(peer)
      if (refs === undefined) return
      try {
        switch (command.type) {
          case "awaitMutation": {
            void client.awaitMutation(command.mutationId).then(
              (outcome) => send(peer, { type: "outcome", id: command.id, outcome }),
              (error: unknown) =>
                send(peer, { type: "error", id: command.id, message: String(error) }),
            )
            break
          }
          case "subscribe": {
            await release(peer, command.id)
            if (peers.get(peer) !== refs) return
            const key = queryKey(command.query)
            let entry = queries.get(key)
            if (entry === undefined) {
              const query = client.liveQuery(queryOf(command.query))
              const members = new Map<string, Set<string>>()
              const publish = (): void => {
                const snapshot = query.getSnapshot()
                send(null, { type: "snapshot", id: key, snapshot })
              }
              entry = { query, off: query.subscribe(publish), peers: members }
              queries.set(key, entry)
            }
            refs.set(command.id, key)
            const ids = entry.peers.get(peer) ?? new Set<string>()
            ids.add(command.id)
            entry.peers.set(peer, ids)
            send(peer, {
              type: "snapshot",
              id: key,
              snapshot: entry.query.getSnapshot(),
            })
            break
          }
          case "release":
            await release(peer, command.id)
            break
          case "read": {
            const rows = await client.read(queryOf(command.query))
            send(peer, {
              type: "read",
              id: command.id,
              rows,
            })
            break
          }
          case "mutate": {
            if (
              config.mutators === undefined ||
              !Object.hasOwn(config.mutators.definitions, command.name)
            )
              throw new Error(`Unknown mutator: ${command.name}`)
            const { args } = decodeArgs(config.mutators, command.name, command.args)
            const candidate: unknown = Reflect.get(client.mutate, command.name)

            if (!callable(candidate)) throw new Error(`Unknown mutator: ${command.name}`)
            const handle = candidate(args)
            send(peer, { type: "allocated", id: command.id, mutationId: handle.id })
            void handle.local.then(
              () => send(peer, { type: "local", id: command.id }),
              (error: unknown) =>
                send(peer, { type: "error", id: command.id, message: String(error) }),
            )
            void handle.server.then(
              (outcome) => send(peer, { type: "outcome", id: command.id, outcome }),
              (error: unknown) =>
                send(peer, { type: "error", id: command.id, message: String(error) }),
            )
            break
          }
        }
      } catch (error) {
        send(peer, {
          type: "error",
          id: command.id,
          message: error instanceof Error ? error.message : String(error),
        })
      }
    },
    abort: () => {
      for (const socket of sockets) socket.close()
      abort.abort()
    },
    close: async () => {
      offStatus()
      offMutation()
      for (const entry of queries.values()) entry.off()
      queries.clear()
      peers.clear()
      await client.close()
    },
  }
}
