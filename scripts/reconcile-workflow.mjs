import {
  enabledPairs,
  loadRegistry,
  parseArgs,
  shouldWriteGeneratedData,
  writeModeLabel,
} from "./lib/config.mjs";
import { loadWorkflowState, saveWorkflowState } from "./lib/state.mjs";
import {
  formatReconcileSummary,
  reconcileWorkflowWithData,
} from "./lib/reconcile.mjs";

const args = parseArgs();
const registry = await loadRegistry();
const writeGeneratedData = shouldWriteGeneratedData(args);
console.log(`mode: ${writeModeLabel(writeGeneratedData)}`);
const pairs = enabledPairs(registry, args.pair);
const workflow = await loadWorkflowState();

const summaries = await reconcileWorkflowWithData({
  pairs,
  workflow,
});

for (const summary of summaries) {
  console.log(formatReconcileSummary(summary));
}

if (writeGeneratedData && summaries.some((summary) => summary.changed)) {
  await saveWorkflowState(workflow);
}
