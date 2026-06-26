import { sortFillRecords } from "./events.mjs";
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

function bucketStartAtOrBeforeFromAnchor(timestamp, anchor) {
  const intervalMs = BAR_INTERVAL_MINUTES * 60_000;
  const anchorMs = Date.parse(anchor);
  const timestampMs = Date.parse(timestamp);
  const intervalsBack = Math.ceil((anchorMs - timestampMs) / intervalMs);
  return addMinutes(anchor, -intervalsBack * BAR_INTERVAL_MINUTES);
}
