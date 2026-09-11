import type { Env } from "../worker/index.ts"

declare global {
  namespace Cloudflare {
    // eslint-disable-next-line @typescript-eslint/no-empty-object-type
    interface Env extends EnvShape {}
  }
}

type EnvShape = Env
