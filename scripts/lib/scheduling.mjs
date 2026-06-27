import {
  DEFAULT_BAR_INTERVAL_MINUTES,
  addMinutes,
  compareIso,
} from "./paths.mjs";
import { DEFAULT_COLLECT_INITIAL_LOOKBACK_MINUTES } from "./config.mjs";

export const LIVE_REPAIR_LOOKBACK_MINUTES = 24 * 60;

export function nextLiveBucketStarts(
  pairState,
  latestStart,
  maxBuckets,
  initialLookbackMinutes = DEFAULT_COLLECT_INITIAL_LOOKBACK_MINUTES,
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  const pendingLiveStarts = [];
  const start = pairState.live.lastQueuedBucketStart
    ? addMinutes(pairState.live.lastQueuedBucketStart, barIntervalMinutes)
    : initialLiveBucketStart(
        latestStart,
        maxBuckets,
        initialLookbackMinutes,
        barIntervalMinutes,
      );
  for (
    let current = start;
    pendingLiveStarts.length < maxBuckets &&
    compareIso(current, latestStart) <= 0;
    current = addMinutes(current, barIntervalMinutes)
  ) {
    pendingLiveStarts.push(current);
  }

  const starts = [...pendingLiveStarts];
  const seen = new Set(starts);
  const retryStarts = (pairState.missingBuckets ?? [])
    .map((bucket) => bucket.start)
    .filter((bucketStart) => compareIso(bucketStart, latestStart) <= 0)
    .filter((bucketStart) => !seen.has(bucketStart))
    .sort(compareIso);

  for (const retryStart of retryStarts) {
    if (starts.length >= maxBuckets) break;
    starts.push(retryStart);
    seen.add(retryStart);
  }

  return starts.sort(compareIso);
}

export function liveRepairBucketStarts(
  pairState,
  latestStart,
  lookbackMinutes = LIVE_REPAIR_LOOKBACK_MINUTES,
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  const anchor = pairState.live.firstCoveredBucketStart;
  if (!anchor) {
    return [];
  }
  const requestedBuckets = Math.max(
    1,
    Math.ceil(lookbackMinutes / barIntervalMinutes),
  );
  const requestedStart = addMinutes(
    latestStart,
    -barIntervalMinutes * (requestedBuckets - 1),
  );
  const start =
    compareIso(requestedStart, anchor) < 0 ? anchor : requestedStart;
  const starts = [];
  for (
    let current = start;
    compareIso(current, latestStart) <= 0;
    current = addMinutes(current, barIntervalMinutes)
  ) {
    starts.push(current);
  }
  return starts;
}

function initialLiveBucketStart(
  latestStart,
  maxBuckets,
  initialLookbackMinutes,
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  const requestedBuckets = Math.max(
    1,
    Math.ceil(initialLookbackMinutes / barIntervalMinutes),
  );
  const bucketCount = Math.min(maxBuckets, requestedBuckets);
  return addMinutes(latestStart, -barIntervalMinutes * (bucketCount - 1));
}
