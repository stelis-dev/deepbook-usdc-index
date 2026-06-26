import {
  enabledPairs,
  loadRegistry,
  parseArgs,
  shouldWriteGeneratedData,
  writeModeLabel,
} from "./lib/config.mjs";
import { BAR_INTERVAL_MINUTES, latestClosedBucketStart } from "./lib/paths.mjs";
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
const workflow = await loadWorkflowState();
const latestClosedStart = latestClosedBucketStart(
  new Date(),
  BAR_INTERVAL_MINUTES,
);

const summaries = await enforceDataRetention({
  pairs,
  workflow,
  referenceIso: latestClosedStart,
  writeGeneratedData,
});

for (const summary of summaries) {
  console.log(formatRetentionSummary(summary));
}

if (writeGeneratedData) {
  await saveWorkflowState(workflow);
}
