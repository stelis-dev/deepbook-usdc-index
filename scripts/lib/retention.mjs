import { readdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { readJson, writeJson } from "./io.mjs";
import {
  DEFAULT_BAR_INTERVAL_MINUTES,
  compareIso,
  floorIsoToInterval,
} from "./paths.mjs";

export function retentionCutoffStart(
  referenceIso,
  years,
  barIntervalMinutes = DEFAULT_BAR_INTERVAL_MINUTES,
) {
  if (!Number.isSafeInteger(years) || years <= 0) {
    throw new Error("rollingRetentionYears must be a positive integer");
  }
  const date = new Date(referenceIso);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid retention reference timestamp: ${referenceIso}`);
  }
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return floorIsoToInterval(date, barIntervalMinutes);
}

export async function enforceDataRetention(input) {
  const writeGeneratedData = input.writeGeneratedData ?? true;
  const dataRoot = input.dataRoot ?? "data";
  const barIntervalMinutes =
    input.barIntervalMinutes ??
    input.pairs[0]?.collection?.barIntervalMinutes ??
    DEFAULT_BAR_INTERVAL_MINUTES;
  const summaries = [];
  for (const pair of input.pairs) {
    const years = pair.collection?.rollingRetentionYears;
    const cutoffStart = retentionCutoffStart(
      input.referenceIso,
      years,
      barIntervalMinutes,
    );
    const summary = await prunePairBars({
      dataRoot,
      pairId: pair.id,
      cutoffStart,
      writeGeneratedData,
    });
    const pairState = input.workflow?.pairs?.[pair.id];
    if (writeGeneratedData && pairState) {
      summary.workflowAdjusted = applyWorkflowRetention(
        pairState,
        summary.oldestRetainedBucketStart,
        cutoffStart,
      );
    }
    summaries.push(summary);
  }
  return summaries;
}

export function formatRetentionSummary(summary) {
  const verb = summary.writeGeneratedData ? "pruned" : "would prune";
  return `${summary.pairId}: retention ${verb} ${summary.deletedFiles} files and ${summary.trimmedBars} bars older than ${summary.cutoffStart}${summary.workflowAdjusted ? " (workflow coverage advanced)" : ""}`;
}

async function prunePairBars(input) {
  const root = join(input.dataRoot, input.pairId, "bars");
  const files = await listJsonFiles(root);
  const retainedStarts = [];
  let deletedFiles = 0;
  let trimmedBars = 0;

  for (const file of files) {
    const week = await readJson(file);
    const bars = week.bars ?? [];
    const retainedBars = bars.filter(
      (bar) => compareIso(bar.start, input.cutoffStart) >= 0,
    );
    for (const bar of retainedBars) {
      retainedStarts.push(bar.start);
    }
    if (retainedBars.length === bars.length) {
      continue;
    }
    trimmedBars += bars.length - retainedBars.length;
    if (retainedBars.length === 0) {
      deletedFiles += 1;
      if (input.writeGeneratedData) {
        await rm(file, { force: true });
      }
      continue;
    }
    if (input.writeGeneratedData) {
      await writeJson(file, { ...week, bars: retainedBars });
    }
  }

  retainedStarts.sort(compareIso);
  return {
    pairId: input.pairId,
    cutoffStart: input.cutoffStart,
    deletedFiles,
    trimmedBars,
    oldestRetainedBucketStart: retainedStarts[0] ?? null,
    writeGeneratedData: input.writeGeneratedData,
    workflowAdjusted: false,
  };
}

function applyWorkflowRetention(
  pairState,
  oldestRetainedBucketStart,
  cutoffStart,
) {
  let adjusted = false;
  if (!oldestRetainedBucketStart) {
    if (
      pairState.live.lastCoveredBucketStart &&
      compareIso(pairState.live.lastCoveredBucketStart, cutoffStart) < 0
    ) {
      pairState.live.firstCoveredBucketStart = null;
      pairState.live.lastQueuedBucketStart = null;
      pairState.live.lastCoveredBucketStart = null;
      pairState.live.lastCoveredCheckpoint = null;
      pairState.backfill.status = "not_started";
      pairState.backfill.oldestCoveredBucketStart = null;
      pairState.backfill.oldestCoveredCheckpoint = null;
      pairState.backfill.cursor = null;
      pairState.backfill.stoppedReason = null;
      pairState.missingBuckets = [];
      adjusted = true;
    }
    return adjusted;
  }

  const liveAdvanced = advanceIsoField(
    pairState.live,
    "firstCoveredBucketStart",
    oldestRetainedBucketStart,
  );
  const backfillAdvanced = advanceIsoField(
    pairState.backfill,
    "oldestCoveredBucketStart",
    oldestRetainedBucketStart,
  );
  adjusted = liveAdvanced || backfillAdvanced;
  if (backfillAdvanced) {
    pairState.backfill.oldestCoveredCheckpoint = null;
  }
  const beforeMissingCount = pairState.missingBuckets.length;
  pairState.missingBuckets = pairState.missingBuckets.filter(
    (bucket) => compareIso(bucket.start, oldestRetainedBucketStart) >= 0,
  );
  return adjusted || pairState.missingBuckets.length !== beforeMissingCount;
}

function advanceIsoField(object, key, floor) {
  if (!object[key] || compareIso(object[key], floor) >= 0) {
    return false;
  }
  object[key] = floor;
  return true;
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
