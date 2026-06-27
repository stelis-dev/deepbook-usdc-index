import {
  barIntervalMinutesFromPairs,
  collectInitialLookbackMinutesFromEnv,
  collectMaxBucketsFromEnv,
  enabledPairs,
  loadRegistry,
  liveRunModeFromInput,
  parseArgs,
  repairLiveLookbackHoursFromInput,
  shouldWriteGeneratedData,
  writeModeLabel,
} from "./lib/config.mjs";
import {
  createLiveCollectionContext,
  runLiveBucketJob,
} from "./lib/live-runner.mjs";
import { latestClosedBucketStart } from "./lib/paths.mjs";
import {
  liveRepairBucketStarts,
  nextLiveBucketStarts,
} from "./lib/scheduling.mjs";
import { loadWorkflowState, saveWorkflowState } from "./lib/state.mjs";
import {
  enforceDataRetention,
  formatRetentionSummary,
} from "./lib/retention.mjs";
import {
  clearResolvedMissingBuckets,
  formatMissingCleanupSummary,
} from "./lib/missing.mjs";
import {
  formatReconcileSummary,
  reconcileWorkflowWithData,
} from "./lib/reconcile.mjs";

const args = parseArgs();
const registry = await loadRegistry();
const writeGeneratedData = shouldWriteGeneratedData(args);
console.log(`mode: ${writeModeLabel(writeGeneratedData)}`);
const liveRunMode = liveRunModeFromInput(args);
console.log(`live-run-mode: ${liveRunMode}`);
const repairLookbackHours =
  liveRunMode === "repair" ? repairLiveLookbackHoursFromInput(args) : null;
if (repairLookbackHours !== null) {
  console.log(`repair-lookback-hours: ${repairLookbackHours}`);
}
const pairs = enabledPairs(registry, args.pair);
const barIntervalMinutes = barIntervalMinutesFromPairs(pairs);
console.log(`bar-interval-minutes: ${barIntervalMinutes}`);
const workflow = await loadWorkflowState(barIntervalMinutes);
const maxBuckets = collectMaxBucketsFromEnv();
const initialLookbackMinutes = collectInitialLookbackMinutesFromEnv();
const latestClosedStart = latestClosedBucketStart(
  new Date(),
  barIntervalMinutes,
);

if (writeGeneratedData) {
  const reconcileSummaries = await reconcileWorkflowWithData({
    pairs,
    workflow,
  });
  for (const summary of reconcileSummaries) {
    if (summary.changed) {
      console.log(formatReconcileSummary(summary));
    }
  }

  const missingCleanupSummaries = await clearResolvedMissingBuckets({
    pairs,
    workflow,
  });
  for (const summary of missingCleanupSummaries) {
    if (summary.cleared > 0) {
      console.log(formatMissingCleanupSummary(summary));
    }
  }
}

const liveContext = await createLiveCollectionContext(registry);

await runLiveBucketJob({
  pairs,
  workflow,
  writeGeneratedData,
  ...liveContext,
  bucketStartsForPair: (pairState) => {
    if (liveRunMode === "repair") {
      return liveRepairBucketStarts(
        pairState,
        latestClosedStart,
        repairLookbackHours * 60,
        barIntervalMinutes,
      );
    }
    return nextLiveBucketStarts(
      pairState,
      latestClosedStart,
      maxBuckets,
      initialLookbackMinutes,
      barIntervalMinutes,
    );
  },
  emptyMessage:
    liveRunMode === "repair"
      ? `no anchored closed ${barIntervalMinutes}-minute UTC buckets to repair`
      : `no closed ${barIntervalMinutes}-minute UTC buckets to collect`,
  writeVerb: liveRunMode === "repair" ? "repaired" : "collected",
  dryRunVerb: liveRunMode === "repair" ? "would repair" : "would collect",
  barIntervalMinutes,
});

if (writeGeneratedData) {
  const retentionSummaries = await enforceDataRetention({
    pairs,
    workflow,
    referenceIso: latestClosedStart,
    writeGeneratedData,
    barIntervalMinutes,
  });
  for (const summary of retentionSummaries) {
    if (summary.deletedFiles > 0 || summary.trimmedBars > 0) {
      console.log(formatRetentionSummary(summary));
    }
  }
  await saveWorkflowState(workflow);
}
