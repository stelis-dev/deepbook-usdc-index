import { readJson } from "./io.mjs";
import { barsWeekPath } from "./paths.mjs";

export async function clearResolvedMissingBuckets(input) {
  const readBarsFile = input.readBarsFile ?? readJsonWithMissingFallback;
  const summaries = [];

  for (const pair of input.pairs) {
    const pairState = input.workflow.pairs?.[pair.id];
    if (!pairState?.missingBuckets?.length) {
      summaries.push({ pairId: pair.id, cleared: 0, remaining: 0 });
      continue;
    }

    const weekCache = new Map();
    const remaining = [];
    let cleared = 0;
    for (const bucket of pairState.missingBuckets) {
      const barsPath = barsWeekPath(pair.id, bucket.start);
      const weekFile = await cachedWeekFile(weekCache, readBarsFile, barsPath);
      const resolved = (weekFile?.bars ?? []).some(
        (bar) =>
          bar.start === bucket.start &&
          (bar.status === "filled" || bar.status === "empty"),
      );
      if (resolved) {
        cleared += 1;
      } else {
        remaining.push(bucket);
      }
    }

    pairState.missingBuckets = remaining;
    summaries.push({ pairId: pair.id, cleared, remaining: remaining.length });
  }

  return summaries;
}

export function formatMissingCleanupSummary(summary) {
  return `${summary.pairId}: cleared ${summary.cleared} resolved missing bucket${summary.cleared === 1 ? "" : "s"} from workflow state`;
}

async function cachedWeekFile(cache, readBarsFile, barsPath) {
  if (!cache.has(barsPath)) {
    cache.set(barsPath, readBarsFile(barsPath));
  }
  return await cache.get(barsPath);
}

async function readJsonWithMissingFallback(path) {
  return await readJson(path, null);
}
