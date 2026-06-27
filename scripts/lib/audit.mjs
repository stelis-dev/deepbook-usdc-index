import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { readJson } from "./io.mjs";
import { summarizePairBars } from "./reconcile.mjs";
import {
  DEFAULT_BAR_INTERVAL_MINUTES,
  addMinutes,
  barsWeekPath,
  bucketStartsBetween,
  compareIso,
  floorIsoToInterval,
} from "./paths.mjs";

export async function auditGeneratedCoverage(input) {
  const barIntervalMinutes =
    input.barIntervalMinutes ??
    input.pairs[0]?.collection?.barIntervalMinutes ??
    DEFAULT_BAR_INTERVAL_MINUTES;
  const readBarsFile = input.readBarsFile ?? readJson;
  const scanTempFiles = input.scanTempFiles ?? true;
  const weekCache = new Map();
  const issues = [];
  const knownMissing = [];
  const summaries = [];
  const dataRoot = input.dataRoot ?? "data";
  const scanLocalDataSummary =
    input.dataRoot !== undefined || input.readBarsFile === undefined;

  for (const pair of input.pairs) {
    const pairState = input.workflow.pairs?.[pair.id];
    const startIso = coverageStart(pairState);
    const endIso = coverageEnd(pairState);
    const dataSummary = scanLocalDataSummary
      ? await summarizePairBars({
          pairId: pair.id,
          dataRoot,
        })
      : emptyDataSummary(pair.id);
    if (!pairState || !startIso || !endIso) {
      summaries.push(emptySummary(pair.id));
      if (dataSummary.barCount > 0 && !pairState) {
        issues.push({
          pairId: pair.id,
          start: dataSummary.firstBarStart,
          type: "data_without_workflow_state",
        });
      } else if (dataSummary.coveredCount > 0) {
        issues.push({
          pairId: pair.id,
          start: dataSummary.firstCoveredStart,
          type: "data_without_workflow_coverage",
        });
      }
      continue;
    }

    const summary = emptySummary(pair.id, startIso, endIso);
    summaries.push(summary);

    if (
      !isAligned(startIso, barIntervalMinutes) ||
      !isAligned(endIso, barIntervalMinutes)
    ) {
      issues.push({
        pairId: pair.id,
        start: startIso,
        type: "unaligned_coverage_range",
      });
      continue;
    }
    if (compareIso(startIso, endIso) > 0) {
      issues.push({
        pairId: pair.id,
        start: startIso,
        end: endIso,
        type: "invalid_coverage_range",
      });
      continue;
    }
    if (
      dataSummary.firstBarStart &&
      compareIso(dataSummary.firstBarStart, startIso) < 0
    ) {
      issues.push({
        pairId: pair.id,
        start: dataSummary.firstBarStart,
        type: "data_before_workflow_coverage",
      });
    }
    if (
      dataSummary.lastBarStart &&
      compareIso(dataSummary.lastBarStart, endIso) > 0
    ) {
      issues.push({
        pairId: pair.id,
        start: dataSummary.lastBarStart,
        type: "data_after_workflow_coverage",
      });
    }

    const trackedMissing = new Set(
      (pairState.missingBuckets ?? []).map((bucket) => bucket.start),
    );
    for (const start of bucketStartsBetween(
      startIso,
      addMinutes(endIso, barIntervalMinutes),
      barIntervalMinutes,
    )) {
      const day = daySummary(summary, start);
      day.expected += 1;
      summary.expected += 1;

      const barsPath = barsWeekPath(pair.id, start);
      const weekFile = await cachedWeekFile(weekCache, readBarsFile, barsPath);
      if (!weekFile) {
        day.absent += 1;
        summary.absent += 1;
        issues.push({ pairId: pair.id, start, type: "absent_week_file" });
        continue;
      }

      const matches = (weekFile.bars ?? []).filter(
        (bar) => bar.start === start,
      );
      if (matches.length === 0) {
        day.absent += 1;
        summary.absent += 1;
        issues.push({ pairId: pair.id, start, type: "absent_bar" });
        continue;
      }
      if (matches.length > 1) {
        issues.push({ pairId: pair.id, start, type: "duplicate_bar" });
      }

      const bar = matches[0];
      if (bar.end !== addMinutes(start, barIntervalMinutes)) {
        issues.push({
          pairId: pair.id,
          start,
          type: "invalid_bar_end",
          actual: bar.end,
        });
      }

      if (bar.status === "filled") {
        day.filled += 1;
        summary.filled += 1;
      } else if (bar.status === "empty") {
        day.empty += 1;
        summary.empty += 1;
      } else if (bar.status === "missing") {
        day.missing += 1;
        summary.missing += 1;
        knownMissing.push({ pairId: pair.id, start });
        if (!trackedMissing.has(start)) {
          issues.push({
            pairId: pair.id,
            start,
            type: "missing_bar_not_tracked",
          });
        }
      } else {
        issues.push({
          pairId: pair.id,
          start,
          type: "invalid_bar_status",
          actual: bar.status,
        });
      }

      if (bar.status !== "missing" && trackedMissing.has(start)) {
        issues.push({
          pairId: pair.id,
          start,
          type: "stale_missing_state",
        });
      }
    }
  }

  if (scanTempFiles) {
    for (const path of await findTempFiles(dataRoot)) {
      issues.push({
        pairId: "*",
        start: null,
        type: "temp_file",
        path,
      });
    }
  }

  return {
    ok: issues.length === 0,
    issues,
    knownMissing,
    summaries,
    barIntervalMinutes,
  };
}

export function formatAuditReport(result, options = {}) {
  const maxIssues = options.maxIssues ?? 50;
  const lines = [];
  const expected = result.summaries.reduce(
    (total, summary) => total + summary.expected,
    0,
  );
  const missing = result.summaries.reduce(
    (total, summary) => total + summary.missing,
    0,
  );
  const absent = result.summaries.reduce(
    (total, summary) => total + summary.absent,
    0,
  );
  lines.push(
    `coverage audit: ${result.ok ? "ok" : "failed"} (${expected} expected ${result.barIntervalMinutes}-minute bars, ${missing} known missing, ${absent} absent)`,
  );

  for (const summary of result.summaries) {
    if (summary.expected === 0) {
      lines.push(`${summary.pairId}: no covered range`);
      continue;
    }
    lines.push(
      `${summary.pairId}: ${summary.start}..${addMinutes(summary.end, result.barIntervalMinutes)} expected=${summary.expected} filled=${summary.filled} empty=${summary.empty} missing=${summary.missing} absent=${summary.absent}`,
    );
    for (const day of summary.days.filter(
      (item) => item.missing > 0 || item.absent > 0,
    )) {
      lines.push(
        `  ${day.date}: expected=${day.expected} missing=${day.missing} absent=${day.absent}`,
      );
    }
  }

  if (result.issues.length > 0) {
    lines.push(
      `issues (${Math.min(result.issues.length, maxIssues)}/${result.issues.length}):`,
    );
    for (const issue of result.issues.slice(0, maxIssues)) {
      lines.push(
        `  ${issueLabel(issue)}: ${issue.type}${issue.actual ? ` (${issue.actual})` : ""}`,
      );
    }
  }

  return lines.join("\n");
}

function coverageStart(pairState) {
  if (!pairState) return null;
  return [
    pairState.backfill?.oldestCoveredBucketStart,
    pairState.live?.firstCoveredBucketStart,
    ...(pairState.missingBuckets ?? []).map((bucket) => bucket.start),
  ]
    .filter(Boolean)
    .sort(compareIso)[0];
}

function coverageEnd(pairState) {
  if (!pairState) return null;
  return (
    pairState.live?.lastQueuedBucketStart ??
    pairState.live?.lastCoveredBucketStart
  );
}

function emptySummary(pairId, start = null, end = null) {
  return {
    pairId,
    start,
    end,
    expected: 0,
    filled: 0,
    empty: 0,
    missing: 0,
    absent: 0,
    days: [],
  };
}

function emptyDataSummary(pairId) {
  return {
    pairId,
    barCount: 0,
    coveredCount: 0,
    missingCount: 0,
    firstBarStart: null,
    lastBarStart: null,
    firstCoveredStart: null,
    lastCoveredStart: null,
    missingBars: [],
  };
}

function daySummary(summary, start) {
  const date = start.slice(0, 10);
  let day = summary.days.find((candidate) => candidate.date === date);
  if (!day) {
    day = {
      date,
      expected: 0,
      filled: 0,
      empty: 0,
      missing: 0,
      absent: 0,
    };
    summary.days.push(day);
  }
  return day;
}

function isAligned(iso, barIntervalMinutes) {
  return floorIsoToInterval(new Date(iso), barIntervalMinutes) === iso;
}

async function cachedWeekFile(cache, readBarsFile, path) {
  if (!cache.has(path)) {
    cache.set(
      path,
      readBarsFile(path, null).catch((error) => {
        if (error?.code === "ENOENT") return null;
        throw error;
      }),
    );
  }
  return await cache.get(path);
}

async function findTempFiles(root) {
  const files = [];
  await walkTempFiles(root, files);
  return files.sort();
}

async function walkTempFiles(directory, files) {
  const entries = await readdir(directory, { withFileTypes: true }).catch(
    (error) => {
      if (error?.code === "ENOENT") return [];
      throw error;
    },
  );
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await walkTempFiles(path, files);
    } else if (entry.isFile() && entry.name.endsWith(".tmp")) {
      files.push(path);
    }
  }
}

function issueLabel(issue) {
  if (issue.path) return issue.path;
  return `${issue.pairId} ${issue.start}`;
}
