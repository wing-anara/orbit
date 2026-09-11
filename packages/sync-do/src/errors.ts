import { Data } from "effect"

export class AuthError extends Data.TaggedError("AuthError")<{
  readonly reason: "invalid_token" | "expired" | "malformed"
  readonly message: string
}> {}

export class PartitionDenied extends Data.TaggedError("PartitionDenied")<{
  readonly subject: string
  readonly partition: string
}> {}

export class InternalAuthError extends Data.TaggedError("InternalAuthError")<{
  readonly message: string
}> {}

export class BadRequest extends Data.TaggedError("BadRequest")<{ readonly message: string }> {}

export class FillNotActive extends Data.TaggedError("FillNotActive")<{ readonly fillId: string }> {}

export class StorageError extends Data.TaggedError("StorageError")<{
  readonly message: string
  readonly cause: unknown
}> {}
