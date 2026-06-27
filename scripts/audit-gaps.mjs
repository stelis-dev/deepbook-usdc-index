import { auditGeneratedCoverage, formatAuditReport } from "./lib/audit.mjs";
import {
  barIntervalMinutesFromPairs,
  enabledPairs,
  loadRegistry,
} from "./lib/config.mjs";
import { loadWorkflowState } from "./lib/state.mjs";

const registry = await loadRegistry();
const pairs = enabledPairs(registry);
const barIntervalMinutes = barIntervalMinutesFromPairs(pairs);
const workflow = await loadWorkflowState(barIntervalMinutes);
const result = await auditGeneratedCoverage({
  pairs,
  workflow,
  barIntervalMinutes,
});

const report = formatAuditReport(result);
if (result.ok) {
  console.log(report);
} else {
  console.error(report);
  process.exitCode = 1;
}
