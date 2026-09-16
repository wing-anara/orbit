import { decodeMessage, type Command, type Event, type Message } from "./protocol.ts"
import { deferred, type SharedChannel, type SharedPlatform } from "./platform.ts"
import type { SharedOwner } from "./owner.ts"

export class SharedCoordinator {
  private readonly peer: string
  private readonly channel: SharedChannel
  private readonly abort = new AbortController()
  private readonly membership = deferred<void>()
  private readonly lifetime = deferred<void>()
  private readonly watched = new Set<string>()
  private readonly commands = new Map<string, Promise<void>>()
  private owner: SharedOwner | null = null
  private ownerEpoch: string | null = null
  private epoch: string | null = null
  private closed = false
  private abandoned = false
  private election: Promise<void> | null = null
  private memberLock: Promise<void> | null = null

  constructor(
    private readonly config: {
      scope: string
      schema: string
      platform: SharedPlatform
      open: (
        send: (peer: string | null, event: Event) => void,
        signal: AbortSignal,
      ) => Promise<SharedOwner>
      event: (event: Event) => void
      change: () => void
      failure: (error: Error) => void
    },
  ) {
    this.peer = config.platform.randomId()
    this.channel = config.platform.channel(config.scope)
    this.channel.addEventListener("message", this.onMessage)
  }

  private send(message: Message): void {
    if (this.closed) return
    this.channel.postMessage(message)
    queueMicrotask(() => this.receive(message))
  }

  private readonly onMessage = (event: MessageEvent<unknown>): void => {
    let message: Message
    try {
      message = decodeMessage(event.data)
    } catch {
      return
    }
    this.receive(message)
  }

  async start(): Promise<void> {
    const acquired = deferred<void>()
    this.memberLock = this.config.platform.lock(
      `${this.config.scope}:peer:${this.peer}`,
      this.abort.signal,
      async () => {
        acquired.resolve()
        await this.membership.promise
      },
    )
    void this.memberLock.catch((error: unknown) =>
      acquired.reject(error instanceof Error ? error : new Error(String(error))),
    )
    await acquired.promise
    this.election = this.config.platform.lock(
      `${this.config.scope}:owner`,
      this.abort.signal,
      async () => {
        if (this.closed) return
        const epoch = this.config.platform.randomId()
        this.ownerEpoch = epoch
        try {
          const opening = this.config.open((to, event) => {
            if (this.ownerEpoch === epoch)
              this.send({ type: "event", peer: this.peer, epoch, to, event })
          }, this.abort.signal)
          this.owner = await Promise.race([opening, this.lifetime.promise.then(() => null)])
          if (this.owner === null) {
            void opening
              .then(async (owner) => {
                owner.abort()
                await owner.close()
              })
              .catch(() => undefined)
            return
          }
          if (this.closed) return
          this.announce()
          await this.lifetime.promise
        } finally {
          const closing = this.owner?.close()
          if (this.abandoned) void closing?.catch(() => undefined)
          else await closing
          this.owner = null
          this.ownerEpoch = null
          this.send({ type: "stopped", peer: this.peer, epoch })
        }
      },
    )
    void this.election.catch((error: unknown) => {
      if (!this.closed)
        this.config.failure(error instanceof Error ? error : new Error(String(error)))
    })
    this.hello()
  }

  private hello(): void {
    this.send({ type: "hello", peer: this.peer, schema: this.config.schema })
  }

  private announce(): void {
    if (this.ownerEpoch !== null)
      this.send({
        type: "owner",
        peer: this.peer,
        epoch: this.ownerEpoch,
        schema: this.config.schema,
      })
  }

  private watch(peer: string): void {
    if (peer === this.peer || this.watched.has(peer)) return
    this.watched.add(peer)
    void this.config.platform
      .lock(`${this.config.scope}:peer:${peer}`, this.abort.signal, async () => {
        await this.owner?.leave(peer)
        this.commands.delete(peer)
        this.watched.delete(peer)
      })
      .catch(() => undefined)
  }

  private receive(message: Message): void {
    if (this.closed) return
    switch (message.type) {
      case "hello":
        if (this.owner === null) return
        this.announce()
        if (message.schema !== this.config.schema) return
        this.watch(message.peer)
        this.owner.join(message.peer)
        break
      case "owner":
        if (message.schema !== this.config.schema) {
          this.config.failure(
            new Error(
              "Another tab is using an incompatible Orbit schema; close or reload that tab",
            ),
          )
          return
        }
        if (this.epoch !== message.epoch) {
          this.epoch = message.epoch
          this.config.change()
          this.hello()
        }
        break
      case "stopped":
        if (this.epoch !== message.epoch) return
        this.epoch = null
        this.config.change()
        break
      case "event":
        if (message.epoch === this.epoch && (message.to === null || message.to === this.peer))
          this.config.event(message.event)
        break
      case "command": {
        if (this.owner === null || this.ownerEpoch !== message.epoch) return
        const owner = this.owner
        const previous = this.commands.get(message.peer) ?? Promise.resolve()
        const next = previous.then(() => owner.command(message.peer, message.command))
        this.commands.set(
          message.peer,
          next.catch(() => undefined),
        )
        break
      }
      case "leave":
        if (this.owner !== null) {
          void this.owner.leave(message.peer)
          this.commands.delete(message.peer)
        }
        break
    }
  }

  command(command: Command): boolean {
    if (this.closed || this.epoch === null) return false
    this.send({ type: "command", peer: this.peer, epoch: this.epoch, command })
    return true
  }

  async close(abort = false): Promise<void> {
    if (this.closed) return
    this.send({ type: "leave", peer: this.peer })
    this.closed = true
    if (abort) {
      this.abandoned = true
      this.owner?.abort()
    }
    this.lifetime.resolve()
    this.membership.resolve()
    this.abort.abort()
    await this.election?.catch(() => undefined)
    await this.memberLock?.catch(() => undefined)
    this.channel.removeEventListener("message", this.onMessage)
    this.channel.close()
  }
}
