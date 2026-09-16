export interface SharedChannel {
  postMessage(message: unknown): void
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void
  removeEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void
  close(): void
}

export interface SharedPlatform {
  channel(name: string): SharedChannel
  lock(name: string, signal: AbortSignal, hold: () => Promise<void>): Promise<void>
  randomId(): string
  lifecycle?(suspend: () => void, resume: () => void): () => void
}

export const browserPlatform = (): SharedPlatform => {
  if (
    typeof navigator === "undefined" ||
    navigator.locks === undefined ||
    typeof BroadcastChannel === "undefined"
  )
    throw new Error("Shared Orbit clients require Web Locks and BroadcastChannel")
  return {
    channel: (name) => new BroadcastChannel(name),
    lock: (name, signal, hold) => navigator.locks.request(name, { signal }, hold),
    randomId: () => crypto.randomUUID(),
    lifecycle: (suspend, resume) => {
      if (typeof window === "undefined") return () => undefined
      window.addEventListener("pagehide", suspend)
      window.addEventListener("pageshow", resume)
      document.addEventListener("freeze", suspend)
      document.addEventListener("resume", resume)
      return () => {
        window.removeEventListener("pagehide", suspend)
        window.removeEventListener("pageshow", resume)
        document.removeEventListener("freeze", suspend)
        document.removeEventListener("resume", resume)
      }
    },
  }
}

export const sharedScope = async (identity: {
  url: string
  partition: string
  subject: string
  app: string
  databaseName?: string
}): Promise<string> => {
  const url = new URL(identity.url)
  if (url.username || url.password || url.search || url.hash)
    throw new Error("Orbit sync URL must not contain credentials, a query or a fragment")
  const bytes = new TextEncoder().encode(
    JSON.stringify([
      url.href.replace(/\/$/, ""),
      identity.app,
      identity.partition,
      identity.subject,
      identity.databaseName ?? null,
    ]),
  )
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))
  return `orbit-shared-v1-${Array.from(digest, (n) => n.toString(16).padStart(2, "0")).join("")}`
}

export interface Deferred<T> {
  readonly promise: Promise<T>
  readonly resolve: (value: T) => void
  readonly reject: (error: Error) => void
}
export const deferred = <T>(): Deferred<T> => {
  let resolve!: (value: T) => void
  let reject!: (error: Error) => void
  const promise = new Promise<T>((yes, no) => {
    resolve = yes
    reject = no
  })
  void promise.catch(() => undefined)
  return { promise, resolve, reject }
}
