import {
  scanOrderFilledEventsForCheckpointRange,
  scanPoolTransactionsForCheckpointRange,
  sortFillRecords,
} from "./events.mjs";
import {
  BAR_INTERVAL_MINUTES,
  addMinutes,
  compareIso,
  floorIsoToInterval,
} from "./paths.mjs";

export function backfillWindowFromAnchor(records, anchor) {
  assertBackfillAnchor(anchor);
  const historicalRecords = sortFillRecords(
    records.filter((record) => compareIso(record.timestamp, anchor) < 0),
  );
  const oldestRecord = historicalRecords[0] ?? null;
  return {
    records: historicalRecords,
    oldestBucketStart: oldestRecord
      ? bucketStartAtOrBeforeFromAnchor(oldestRecord.timestamp, anchor)
      : null,
    anchor,
    oldestCheckpoint: oldestRecord?.checkpoint ?? null,
  };
}

export function backfillChunkStart(anchor, earliestBucketStart, lookbackHours) {
  assertBackfillAnchor(anchor);
  assertBackfillAnchor(earliestBucketStart);
  const requestedStart = addMinutes(anchor, -lookbackHours * 60);
  return compareIso(requestedStart, earliestBucketStart) < 0
    ? earliestBucketStart
    : requestedStart;
}

export function backfillAnchorForPairState(pairState) {
  const anchor =
    pairState.backfill.oldestCoveredBucketStart ??
    pairState.live.firstCoveredBucketStart;
  if (!anchor) {
    throw new Error(
      "Backfill requires a live collection anchor; run collect until it writes at least one covered 10-minute bucket first",
    );
  }
  assertBackfillAnchor(anchor);
  return anchor;
}

export function assertBackfillAnchor(anchor) {
  const floored = floorIsoToInterval(new Date(anchor), BAR_INTERVAL_MINUTES);
  if (floored !== anchor) {
    throw new Error(
      `Backfill anchor must be a 10-minute UTC bucket start: ${anchor}`,
    );
  }
}

export async function scanBackfillChunk(input) {
  const scanPoolTransactions =
    input.scanPoolTransactionsForCheckpointRange ??
    scanPoolTransactionsForCheckpointRange;
  const scanOrderFilledEvents =
    input.scanOrderFilledEventsForCheckpointRange ??
    scanOrderFilledEventsForCheckpointRange;
  let candidateStart = backfillChunkStart(
    input.anchor,
    input.firstBucketStart,
    input.lookbackHours,
  );

  while (true) {
    const resolved = await input.resolveBackfillCheckpointRange({
      startIso: candidateStart,
      endIso: input.anchor,
    });
    if (resolved.status !== "ok") {
      return {
        status: "unavailable",
        reason: `checkpoint_range_${resolved.reason}`,
      };
    }

    const page = await scanPoolTransactions({
      pair: input.pair,
      eventTypes: input.eventTypes,
      poolId: input.pair.poolId,
      fromCheckpoint: resolved.fromCheckpoint,
      toCheckpoint: resolved.toCheckpoint,
      pageSize: input.pageSize,
      maxPages: input.maxPages,
    });
    if (!page.hasMore) {
      return {
        status: "ok",
        startIso: candidateStart,
        resolved,
        page: { ...page, scanSource: "pool_transactions" },
      };
    }

    const bucketCount = bucketCountBetween(candidateStart, input.anchor);
    if (bucketCount <= 1) {
      return await scanOrderFilledEventFallback({
        input,
        resolved,
        startIso: candidateStart,
        poolStoppedReason: page.stoppedReason,
        scanOrderFilledEvents,
      });
    }

    const nextBucketCount = Math.max(1, Math.floor(bucketCount / 2));
    const requestedStart = backfillChunkStart(
      input.anchor,
      input.firstBucketStart,
      (nextBucketCount * BAR_INTERVAL_MINUTES) / 60,
    );
    if (requestedStart === candidateStart) {
      return await scanOrderFilledEventFallback({
        input,
        resolved,
        startIso: candidateStart,
        poolStoppedReason: page.stoppedReason,
        scanOrderFilledEvents,
      });
    }
    candidateStart = requestedStart;
  }
}

function bucketStartAtOrBeforeFromAnchor(timestamp, anchor) {
  const intervalMs = BAR_INTERVAL_MINUTES * 60_000;
  const anchorMs = Date.parse(anchor);
  const timestampMs = Date.parse(timestamp);
  const intervalsBack = Math.ceil((anchorMs - timestampMs) / intervalMs);
  return addMinutes(anchor, -intervalsBack * BAR_INTERVAL_MINUTES);
}

async function scanOrderFilledEventFallback(input) {
  const records = [];
  let pageCount = 0;
  for (const eventType of input.input.eventTypes) {
    const page = await input.scanOrderFilledEvents({
      pair: input.input.pair,
      eventType,
      poolId: input.input.pair.poolId,
      fromCheckpoint: input.resolved.fromCheckpoint,
      toCheckpoint: input.resolved.toCheckpoint,
      pageSize: input.input.eventPageSize,
      maxPages: input.input.maxEventPages,
      maxRecords: input.input.maxFillRecords,
    });
    records.push(...page.records);
    pageCount += page.pageCount ?? 0;
    if (page.hasMore) {
      return {
        status: "too_dense",
        reason: `order_filled_event_scan_exceeded_bounds:${page.stoppedReason}`,
      };
    }
  }

  return {
    status: "ok",
    startIso: input.startIso,
    resolved: input.resolved,
    page: {
      records,
      transactions: [],
      hasMore: false,
      cursor: null,
      pageCount,
      stoppedReason: null,
      scanSource: "order_filled_events",
      fallbackReason: `pool_transaction_scan_exceeded_bounds:${input.poolStoppedReason}`,
    },
  };
}

function bucketCountBetween(startIso, endIso) {
  const intervalMs = BAR_INTERVAL_MINUTES * 60_000;
  return Math.max(
    0,
    Math.round((Date.parse(endIso) - Date.parse(startIso)) / intervalMs),
  );
}
