import { readJson, writeJson } from "./io.mjs";
import {
  DEFAULT_BAR_INTERVAL_MINUTES,
  addMinutes,
  compareIso,
  workflowMissingPath,
} from "./paths.mjs";

export async function loadWorkflowState(
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  const state = await readJson(
    workflowMissingPath(),
    initialWorkflowState(barIntervalMinutes),
  );
  ensureWorkflowInterval(state, barIntervalMinutes);
  return state;
}

export async function saveWorkflowState(state) {
  await writeJson(workflowMissingPath(), {
    ...state,
    updatedAt: new Date().toISOString(),
  });
}

export function pairWorkflowState(state, pairId) {
  state.pairs[pairId] ??= initialPairWorkflowState();
  return state.pairs[pairId];
}

export function initialWorkflowState(
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  return {
    schemaVersion: 1,
    barIntervalMinutes,
    updatedAt: null,
    pairs: {},
  };
}

export function ensureWorkflowInterval(state, barIntervalMinutes) {
  if (state.barIntervalMinutes === undefined) {
    state.barIntervalMinutes = barIntervalMinutes;
  }
  if (state.barIntervalMinutes !== barIntervalMinutes) {
    throw new Error(
      `Workflow state uses ${state.barIntervalMinutes}-minute bars but registry uses ${barIntervalMinutes}-minute bars`,
    );
  }
}

export function initialPairWorkflowState() {
  return {
    live: {
      firstCoveredBucketStart: null,
      lastQueuedBucketStart: null,
      lastCoveredBucketStart: null,
      lastCoveredCheckpoint: null,
    },
    backfill: {
      status: "not_started",
      oldestCoveredBucketStart: null,
      oldestCoveredCheckpoint: null,
      cursor: null,
      stoppedReason: null,
    },
    missingBuckets: [],
  };
}

export function recordMissingBucket(
  pairState,
  startIso,
  reason,
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  const endIso = addMinutes(startIso, barIntervalMinutes);
  const existing = pairState.missingBuckets.find(
    (bucket) => bucket.start === startIso,
  );
  if (existing) {
    existing.reason = reason;
    existing.attempts += 1;
    existing.lastAttemptedAt = new Date().toISOString();
    return;
  }
  pairState.missingBuckets.push({
    start: startIso,
    end: endIso,
    reason,
    attempts: 1,
    lastAttemptedAt: new Date().toISOString(),
  });
  pairState.missingBuckets.sort(
    (a, b) => Date.parse(a.start) - Date.parse(b.start),
  );
}

export function clearMissingBucket(pairState, startIso) {
  pairState.missingBuckets = pairState.missingBuckets.filter(
    (bucket) => bucket.start !== startIso,
  );
}

export function clearMissingBucketsBetween(
  pairState,
  startIso,
  endExclusiveIso,
) {
  pairState.missingBuckets = pairState.missingBuckets.filter(
    (bucket) =>
      compareIso(bucket.start, startIso) < 0 ||
      compareIso(bucket.start, endExclusiveIso) >= 0,
  );
}

export function advanceLiveQueuedBucket(pairState, startIso) {
  if (
    !pairState.live.lastQueuedBucketStart ||
    compareIso(startIso, pairState.live.lastQueuedBucketStart) > 0
  ) {
    pairState.live.lastQueuedBucketStart = startIso;
  }
}

export function recordCoveredLiveBucket(pairState, startIso, result) {
  if (
    !pairState.live.firstCoveredBucketStart ||
    compareIso(startIso, pairState.live.firstCoveredBucketStart) < 0
  ) {
    pairState.live.firstCoveredBucketStart = startIso;
  }

  if (
    !pairState.live.lastCoveredBucketStart ||
    compareIso(startIso, pairState.live.lastCoveredBucketStart) > 0
  ) {
    pairState.live.lastCoveredBucketStart = startIso;
    pairState.live.lastCoveredCheckpoint = result.toCheckpoint;
  }

  if (
    pairState.backfill.status === "not_started" &&
    (!pairState.backfill.oldestCoveredBucketStart ||
      compareIso(startIso, pairState.backfill.oldestCoveredBucketStart) < 0)
  ) {
    pairState.backfill.oldestCoveredBucketStart = startIso;
    pairState.backfill.oldestCoveredCheckpoint = result.fromCheckpoint;
  }
}

export function recordLiveBucketAttempt(
  pairState,
  startIso,
  result,
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  advanceLiveQueuedBucket(pairState, startIso);
  if (result.status !== "missing") {
    recordCoveredLiveBucket(pairState, startIso, result);
  }
  if (result.status === "missing") {
    recordMissingBucket(pairState, startIso, result.reason, barIntervalMinutes);
  } else {
    clearMissingBucket(pairState, startIso);
  }
}
