/**
 * Physical placement: which Durable Object hosts a logical partition.
 *
 * The only strategy today is one Durable Object per partition. The placement version from the
 * sync schema is part of the object name, so a future strategy (for example splitting hot
 * partitions or co-locating small ones) can be rolled out by bumping the version without
 * clashing with existing objects. Clients never see these names.
 */

import type { SyncSchema } from "@orbit/protocol"

export const durableObjectNameFor = (schema: SyncSchema, partition: string): string => {
  const placement = schema.partition.placement
  switch (placement.strategy) {
    case "one_per_partition":
      return `${schema.app}/p${placement.version}/${partition}`
  }
}

/** Fill ids embed the partition so uploads can be routed without a lookup. */
export const fillIdPartition = (fillId: string): string | null => {
  const i = fillId.lastIndexOf(":")
  return i <= 0 ? null : fillId.slice(0, i)
}
