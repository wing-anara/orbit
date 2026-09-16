import type { SharedChannel, SharedPlatform } from "../../src/shared/platform.ts"

export const sharedPlatform = (): SharedPlatform => {
  const channels = new Map<string, Set<SharedChannel & { deliver(data: unknown): void }>>()
  const tails = new Map<string, Promise<void>>()
  let id = 0
  return {
    randomId: () => `peer-${++id}`,
    channel: (name) => {
      const listeners = new Set<(event: MessageEvent<unknown>) => void>()
      const group = channels.get(name) ?? new Set()
      channels.set(name, group)
      let closed = false
      const channel = {
        postMessage: (data: unknown) => {
          if (closed) throw new Error("Channel closed")
          for (const other of group)
            if (other !== channel) {
              const clone = structuredClone(data)
              queueMicrotask(() => other.deliver(clone))
            }
        },
        deliver: (data: unknown) => {
          if (!closed) for (const listener of listeners) listener({ data } as MessageEvent<unknown>)
        },
        addEventListener: (_type: "message", listener: (event: MessageEvent<unknown>) => void) => {
          listeners.add(listener)
        },
        removeEventListener: (
          _type: "message",
          listener: (event: MessageEvent<unknown>) => void,
        ) => {
          listeners.delete(listener)
        },
        close: () => {
          closed = true
          group.delete(channel)
        },
      }
      group.add(channel)
      return channel
    },
    lock: async (name, signal, hold) => {
      if (signal.aborted) throw new Error("Aborted")
      const previous = tails.get(name) ?? Promise.resolve()
      let release!: () => void
      const gate = new Promise<void>((resolve) => {
        release = resolve
      })
      const tail = previous.then(() => gate)
      tails.set(name, tail)
      let abort!: () => void
      const cancelled = new Promise<never>((_resolve, reject) => {
        abort = () => reject(new Error("Aborted"))
        signal.addEventListener("abort", abort, { once: true })
      })
      try {
        await Promise.race([previous, cancelled])
        signal.removeEventListener("abort", abort)
        if (signal.aborted) throw new Error("Aborted")
        await hold()
      } finally {
        signal.removeEventListener("abort", abort)
        release()
        if (tails.get(name) === tail)
          void tail.then(() => {
            if (tails.get(name) === tail) tails.delete(name)
          })
      }
    },
  }
}
