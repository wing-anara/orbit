/** React bindings: `useLiveQuery` and `useSyncStatus` on top of `useSyncExternalStore`. */

import { useEffect, useMemo, useSyncExternalStore } from "react"
import type { IncludeShape, NamedQueryCall, ResultRow, TypedQuery } from "@orbit/query"
import { canonicalJson } from "@orbit/schema"

import { isNamedQueryCall, type LiveQueryResult, type OrbitClient } from "./client.ts"
import type { SyncStatus } from "./engine.ts"

/** The part of a client the hooks need; any mutator configuration fits. */
export type LiveQuerySource<D> = Pick<OrbitClient<D, never>, "liveQuery">
export type StatusSource = Pick<OrbitClient<unknown, never>, "getStatus" | "onStatus">

/**
 * Subscribes to a live query for the lifetime of the component. A named query is identified by
 * its reference (name and arguments), a raw query by its canonical AST, so re-renders with an
 * equal query reuse the subscription.
 */
export const useLiveQuery = <D, N extends string, I extends IncludeShape = {}>(
  client: LiveQuerySource<D>,
  query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
): LiveQueryResult<ResultRow<D, N, I>> => {
  const key = canonicalJson(isNamedQueryCall(query) ? query.ref : query.ast)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const live = useMemo(() => client.liveQuery(query), [client, key])
  useEffect(() => {
    return () => {
      void live.release()
    }
  }, [live])
  return useSyncExternalStore(live.subscribe, live.getSnapshot, live.getSnapshot)
}

export const useSyncStatus = (client: StatusSource): SyncStatus =>
  useSyncExternalStore(client.onStatus, client.getStatus, client.getStatus)
