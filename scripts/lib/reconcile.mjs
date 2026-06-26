import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { readJson } from "./io.mjs";
import { compareIso } from "./paths.mjs";
import { initialPairWorkflowState } from "./state.mjs";

export async function summarizePairBars(input) {
  const dataRoot = input.dataRoot ?? "data";
  const root = join(dataRoot, input.pairId, "bars");
  const files = await listJsonFiles(root);
  const bars = [];

  for (const file of files) {
    const week = await readJson(file);
    for (const bar of week.bars ?? []) {
      if (typeof bar.start === "string") {
        bars.push(bar);
      }
    }
  }

  bars.sort((left, right) => compareIso(left.start, right.start));
  const coveredBars = bars.filter(
    (bar) => bar.status === "filled" || bar.status === "empty",
  );
  const missingBars = bars.filter((bar) => bar.status === "missing");

  return {
    pairId: input.pairId,
    barCount: bars.length,
    coveredCount: coveredBars.length,
    missingCount: missingBars.length,
    firstBarStart: bars[0]?.start ?? null,
    lastBarStart: bars.at(-1)?.start ?? null,
    firstCoveredStart: coveredBars[0]?.start ?? null,
    lastCoveredStart: coveredBars.at(-1)?.start ?? null,
    missingBars,
  };
}

export async function reconcileWorkflowWithData(input) {
  input.workflow.pairs ??= {};
  const summaries = [];

  for (const pair of input.pairs) {
    const data = await summarizePairBars({
      pairId: pair.id,
      dataRoot: input.dataRoot,
    });
    if (data.barCount === 0) {
      summaries.push({
        pairId: pair.id,
        changed: false,
        barCount: 0,
        coveredCount: 0,
        missingCount: 0,
      });
      continue;
    }

    const pairState =
      input.workflow.pairs[pair.id] ?? initialPairWorkflowState();
    const before = JSON.stringify(pairState);
    applyDataSummaryToPairState(pairState, data, input.workflow.updatedAt);
    input.workflow.pairs[pair.id] = pairState;

    summaries.push({
      pairId: pair.id,
      changed: JSON.stringify(pairState) !== before,
      barCount: data.barCount,
      coveredCount: data.coveredCount,
      missingCount: data.missingCount,
      firstBarStart: data.firstBarStart,
      lastBarStart: data.lastBarStart,
      firstCoveredStart: data.firstCoveredStart,
      lastCoveredStart: data.lastCoveredStart,
    });
  }

  return summaries;
}

export function formatReconcileSummary(summary) {
  const verb = summary.changed ? "reconciled" : "checked";
  return `${summary.pairId}: workflow ${verb} from ${summary.barCount} local bars (${summary.coveredCount} covered, ${summary.missingCount} missing)`;
}

function applyDataSummaryToPairState(pairState, data, fallbackAttemptedAt) {
  const previousLastCoveredBucketStart = pairState.live.lastCoveredBucketStart;
  const previousOldestCoveredBucketStart =
    pairState.backfill.oldestCoveredBucketStart;

  pairState.live.firstCoveredBucketStart = data.firstCoveredStart;
  pairState.live.lastQueuedBucketStart = data.lastBarStart;
  pairState.live.lastCoveredBucketStart = data.lastCoveredStart;
  if (previousLastCoveredBucketStart !== data.lastCoveredStart) {
    pairState.live.lastCoveredCheckpoint = null;
  }

  pairState.backfill.oldestCoveredBucketStart = data.firstCoveredStart;
  if (previousOldestCoveredBucketStart !== data.firstCoveredStart) {
    pairState.backfill.oldestCoveredCheckpoint = null;
  }
  if (!data.firstCoveredStart) {
    pairState.backfill.status = "not_started";
    pairState.backfill.oldestCoveredCheckpoint = null;
    pairState.backfill.stoppedReason = null;
  }
  pairState.backfill.cursor = null;
  if (pairState.backfill.status === "running") {
    pairState.backfill.stoppedReason = null;
  }

  const existingMissing = new Map(
    (pairState.missingBuckets ?? []).map((bucket) => [bucket.start, bucket]),
  );
  pairState.missingBuckets = data.missingBars.map((bar) => {
    const existing = existingMissing.get(bar.start);
    return {
      start: bar.start,
      end: bar.end,
      reason: bar.missingReason ?? existing?.reason ?? "collection_failed",
      attempts: existing?.attempts ?? 1,
      lastAttemptedAt:
        existing?.lastAttemptedAt ??
        fallbackAttemptedAt ??
        new Date().toISOString(),
    };
  });
}

async function listJsonFiles(root) {
  const years = await readdir(root, { withFileTypes: true }).catch((error) => {
    if (error?.code === "ENOENT") return [];
    throw error;
  });
  const files = [];
  for (const year of years) {
    if (!year.isDirectory()) continue;
    const yearRoot = join(root, year.name);
    const entries = await readdir(yearRoot, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isFile() && entry.name.endsWith(".json")) {
        files.push(join(yearRoot, entry.name));
      }
    }
  }
  return files.sort();
}
