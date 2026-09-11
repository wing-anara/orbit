/**
 * Structured logging for the Durable Object. Workers Logs and `wrangler tail` capture whatever
 * is written to `console`, so one JSON object per line on stdout is the observability contract.
 * Row contents never go through here: log fields are counters, ids and status codes.
 */

export interface LogFields {
  /** Event name, dotted, e.g. `orbit.cdc.batch`. */
  readonly event: string
  readonly [field: string]: unknown
}

/** Writes one structured log line. */
export const log = (fields: LogFields): void => {
  // oxlint-disable-next-line no-console -- Workers observability captures console output.
  console.log(JSON.stringify(fields))
}
