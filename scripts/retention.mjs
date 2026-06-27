import {
  barIntervalMinutesFromPairs,
  enabledPairs,
  loadRegistry,
  parseArgs,
  shouldWriteGeneratedData,
  writeModeLabel,
} from "./lib/config.mjs";
import { latestClosedBucketStart } from "./lib/paths.mjs";
import { loadWorkflowState, saveWorkflowState } from "./lib/state.mjs";
import {
  enforceDataRetention,
  formatRetentionSummary,
} from "./lib/retention.mjs";

const args = parseArgs();
const registry = await loadRegistry();
const writeGeneratedData = shouldWriteGeneratedData(args);
console.log(`mode: ${writeModeLabel(writeGeneratedData)}`);
const pairs = enabledPairs(registry, args.pair);
const barIntervalMinutes = barIntervalMinutesFromPairs(pairs);
console.log(`bar-interval-minutes: ${barIntervalMinutes}`);
const workflow = await loadWorkflowState(barIntervalMinutes);
const latestClosedStart = latestClosedBucketStart(
  new Date(),
  barIntervalMinutes,
);

const summaries = await enforceDataRetention({
  pairs,
  workflow,
  referenceIso: latestClosedStart,
  writeGeneratedData,
  barIntervalMinutes,
});

for (const summary of summaries) {
  console.log(formatRetentionSummary(summary));
}

if (writeGeneratedData) {
  await saveWorkflowState(workflow);
}
