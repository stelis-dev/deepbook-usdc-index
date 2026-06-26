import { auditGeneratedCoverage, formatAuditReport } from "./lib/audit.mjs";
import { enabledPairs, loadRegistry } from "./lib/config.mjs";
import { loadWorkflowState } from "./lib/state.mjs";

const registry = await loadRegistry();
const workflow = await loadWorkflowState();
const result = await auditGeneratedCoverage({
  pairs: enabledPairs(registry),
  workflow,
});

const report = formatAuditReport(result);
if (result.ok) {
  console.log(report);
} else {
  console.error(report);
  process.exitCode = 1;
}
