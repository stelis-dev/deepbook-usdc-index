import { USDC_DISCLAIMER } from "./config.mjs";
import {
  scanOrderFilledEventsForCheckpointRange,
  sortFillRecords,
} from "./events.mjs";
import { writeCoveredBucket, writeMissingBucket } from "./buckets.mjs";

export async function collectLiveBucket(input) {
  const resolved = input.resolvedCheckpointRange;
  if (resolved.status !== "ok") {
    return await recordMissingBucketBar(
      input,
      `checkpoint_resolution_${resolved.reason}`,
    );
  }
  if (
    !input.firstTransaction ||
    BigInt(resolved.toCheckpoint) < BigInt(input.firstTransaction.checkpoint)
  ) {
    return await recordCoveredBucket(
      input,
      [],
      resolved.fromCheckpoint,
      resolved.toCheckpoint,
    );
  }
  const page = await scanOrderFilledEventsForCheckpointRange({
    pair: input.pair,
    eventType: input.eventType,
    poolId: input.pair.poolId,
    fromCheckpoint: resolved.fromCheckpoint,
    toCheckpoint: resolved.toCheckpoint,
    pageSize: Number(process.env.GRAPHQL_EVENT_PAGE_SIZE ?? 50),
    maxPages: Number(process.env.MAX_GRAPHQL_PAGES ?? 100),
    maxRecords: Number(process.env.MAX_FILL_RECORDS ?? 10000),
  });
  if (page.hasMore) {
    return await recordMissingBucketBar(
      input,
      `order_filled_scan_exceeded_bounds:${page.stoppedReason}`,
      resolved.fromCheckpoint,
      resolved.toCheckpoint,
    );
  }
  const records = sortFillRecords(page.records);
  return await recordCoveredBucket(
    input,
    records,
    resolved.fromCheckpoint,
    resolved.toCheckpoint,
  );
}

async function recordCoveredBucket(
  input,
  records,
  fromCheckpoint,
  toCheckpoint,
) {
  const result = await writeCoveredBucket({
    pair: input.pair,
    startIso: input.startIso,
    endIso: input.endIso,
    records,
    writeGeneratedData: input.writeGeneratedData,
  });
  return {
    ...result,
    fromCheckpoint,
    toCheckpoint,
    disclaimer: USDC_DISCLAIMER,
  };
}

async function recordMissingBucketBar(
  input,
  reason,
  fromCheckpoint = null,
  toCheckpoint = null,
) {
  const result = await writeMissingBucket({
    pair: input.pair,
    startIso: input.startIso,
    endIso: input.endIso,
    reason,
    writeGeneratedData: input.writeGeneratedData,
  });
  return {
    ...result,
    reason,
    fromCheckpoint,
    toCheckpoint,
    disclaimer: USDC_DISCLAIMER,
  };
}
