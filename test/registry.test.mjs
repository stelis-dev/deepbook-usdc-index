import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  barsWeekPath,
  latestClosedBucketStart,
  utcIsoWeek,
} from "../scripts/lib/paths.mjs";
import { buildBar, mergeWeeklyBars } from "../scripts/lib/ohlc.mjs";
import { writeCoveredBucketRange } from "../scripts/lib/buckets.mjs";
import { createBucketCheckpointRangeResolver } from "../scripts/lib/checkpoints.mjs";
import {
  backfillAnchorForPairState,
  backfillChunkStart,
  backfillWindowFromAnchor,
} from "../scripts/lib/backfill.mjs";
import { auditGeneratedCoverage } from "../scripts/lib/audit.mjs";
import {
  liveRepairBucketStarts,
  nextLiveBucketStarts,
} from "../scripts/lib/scheduling.mjs";
import {
  clearMissingBucketsBetween,
  initialPairWorkflowState,
  recordLiveBucketAttempt,
} from "../scripts/lib/state.mjs";
import {
  liveRunModeFromInput,
  repairLiveLookbackHoursFromInput,
} from "../scripts/lib/config.mjs";
import {
  enforceDataRetention,
  retentionCutoffStart,
} from "../scripts/lib/retention.mjs";
import { clearResolvedMissingBuckets } from "../scripts/lib/missing.mjs";
import { reconcileWorkflowWithData } from "../scripts/lib/reconcile.mjs";

const registry = JSON.parse(await readFile("registry/pairs.json", "utf8"));
const suiUsdc = registry.pairs.find((pair) => pair.id === "SUI_USDC");

test("registry pins canonical USDC and does not describe USD", () => {
  assert.equal(registry.network, "sui:mainnet");
  assert.equal(registry.quoteAsset.symbol, "USDC");
  assert.equal(registry.quoteAsset.decimals, 6);
  assert.match(
    registry.quoteAsset.coinType,
    /^0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC$/,
  );
  assert.match(registry.quoteAsset.disclaimer, /not fiat USD/i);
  assert.match(
    registry.quoteAsset.disclaimer,
    /not guarantee a USDC\/USD peg/i,
  );
});

test("registered pairs are direct USDC pairs with unique ids and pools", () => {
  const ids = new Set();
  const pools = new Set();
  for (const pair of registry.pairs) {
    assert.equal(pair.enabled, true);
    assert.match(pair.id, /^[A-Z0-9]+_USDC$/);
    assert.equal(pair.quoteAsset, "USDC");
    assert.equal(pair.priceConvention, "USDC_PER_BASE");
    assert.equal(pair.collection.barIntervalMinutes, 10);
    assert.equal(ids.has(pair.id), false, `duplicate id ${pair.id}`);
    assert.equal(
      pools.has(pair.poolId),
      false,
      `duplicate pool ${pair.poolId}`,
    );
    ids.add(pair.id);
    pools.add(pair.poolId);
  }
});

test("README states the public UTC weekly read path without USD, route, or P&L claims", async () => {
  const readme = await readFile("README.md", "utf8");
  assert.match(
    readme,
    /data\/<PAIR>\/bars\/<ISO_WEEK_YEAR>\/W<ISO_WEEK>\.json/,
  );
  assert.match(readme, /10-minute UTC/i);
  assert.match(readme, /Direct file URLs cannot list directories/i);
  assert.match(readme, /not fiat USD/i);
  assert.match(readme, /not.*P&L/i);
  assert.match(readme, /best route or best price claim/i);
});

test("registry uses the DeepBook order_info original package for OrderFilled events", () => {
  assert.deepEqual(registry.eventSources.orderInfoPackageIds, [
    "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809",
  ]);
  assert.deepEqual(registry.eventSources.orderFilledEventTypes, [
    "0x2c8d603bc51326b8c13cef9dd07031a408a48dddb541963357661df5d3204809::order_info::OrderFilled",
  ]);
});

test("README states history is scoped to the registered pool object", async () => {
  const readme = await readFile("README.md", "utf8");
  assert.match(readme, /currently registered pool object/i);
  assert.match(readme, /older object must be registered separately/i);
});

test("backfill workflow is scheduled, manually dispatchable, and not transaction bounded", async () => {
  const workflow = await readFile(".github/workflows/backfill.yml", "utf8");
  assert.match(workflow, /cron: "22,52 \* \* \* \*"/);
  assert.match(workflow, /workflow_dispatch/);
  assert.match(workflow, /timeout-minutes: 25/);
  assert.match(workflow, /BACKFILL_LOOKBACK_HOURS_PER_RUN: "168"/);
  assert.match(workflow, /BACKFILL_MAX_TRANSACTION_PAGES_PER_WINDOW: "80"/);
  assert.match(workflow, /actions\/checkout@v7/);
  assert.match(workflow, /actions\/setup-node@v6/);
  assert.match(workflow, /npm run audit:gaps/);
  assert.doesNotMatch(workflow, /actions\/(?:checkout|setup-node)@v4/);
  assert.doesNotMatch(workflow, /max-transactions|BACKFILL_MAX_TRANSACTIONS/);
});

test("collect workflow schedules live collection without repair inputs", async () => {
  const workflow = await readFile(".github/workflows/collect.yml", "utf8");
  assert.match(workflow, /cron: "7,37 \* \* \* \*"/);
  assert.match(workflow, /workflow_dispatch/);
  assert.match(workflow, /LIVE_RUN_MODE: collect/);
  assert.match(workflow, /actions\/checkout@v7/);
  assert.match(workflow, /actions\/setup-node@v6/);
  assert.match(workflow, /npm run collect/);
  assert.match(workflow, /npm run audit:gaps/);
  assert.doesNotMatch(workflow, /cron: "47 3 \* \* \*"/);
  assert.doesNotMatch(workflow, /actions\/(?:checkout|setup-node)@v4/);
  assert.doesNotMatch(
    workflow,
    /github\.event\.inputs|lookback_hours|--pair|pair_id/,
  );
  assert.doesNotMatch(workflow, /cron: "\*\/30 \* \* \* \*"/);
});

test("repair workflow runs anchored live repair in 6-hour units", async () => {
  const workflow = await readFile(".github/workflows/repair.yml", "utf8");
  assert.match(workflow, /cron: "47 3 \* \* \*"/);
  assert.match(workflow, /workflow_dispatch/);
  assert.match(workflow, /lookback_hours/);
  assert.match(
    workflow,
    /options:\s*\n\s*- "6"\s*\n\s*- "12"\s*\n\s*- "18"\s*\n\s*- "24"/,
  );
  assert.match(workflow, /LIVE_RUN_MODE: repair/);
  assert.match(workflow, /REPAIR_LIVE_LOOKBACK_HOURS/);
  assert.match(workflow, /actions\/checkout@v7/);
  assert.match(workflow, /actions\/setup-node@v6/);
  assert.match(workflow, /npm run collect/);
  assert.match(workflow, /npm run audit:gaps/);
  assert.doesNotMatch(workflow, /actions\/(?:checkout|setup-node)@v4/);
  assert.doesNotMatch(workflow, /github\.event\.inputs|--pair|pair_id/);
});

test("live run mode input is restricted to supported modes", () => {
  assert.equal(liveRunModeFromInput({ mode: "collect" }), "collect");
  assert.equal(liveRunModeFromInput({ mode: "repair" }), "repair");
  assert.throws(
    () => liveRunModeFromInput({ mode: "backfill" }),
    /one of: collect, repair/,
  );
});

test("repair live lookback input is restricted to supported windows", () => {
  assert.equal(repairLiveLookbackHoursFromInput({ hours: "6" }), 6);
  assert.equal(repairLiveLookbackHoursFromInput({ hours: "12" }), 12);
  assert.equal(repairLiveLookbackHoursFromInput({ hours: "18" }), 18);
  assert.equal(repairLiveLookbackHoursFromInput({ hours: "24" }), 24);
  assert.throws(
    () => repairLiveLookbackHoursFromInput({ hours: "30" }),
    /one of: 6, 12, 18, 24/,
  );
});

test("collect reuses checkpoint range resolution for identical buckets", async () => {
  const checkpointCaches = [];
  let calls = 0;
  const resolver = createBucketCheckpointRangeResolver({
    client: {},
    retainedRange: {
      first: { sequenceNumber: "1" },
      last: { sequenceNumber: "9" },
    },
    maxCheckpointQueries: 80,
    resolveRange: async (client, input) => {
      assert.deepEqual(client, {});
      calls += 1;
      checkpointCaches.push(input.checkpointCache);
      return {
        status: "ok",
        fromCheckpoint: `from:${input.startIso}`,
        toCheckpoint: `to:${input.endIso}`,
      };
    },
  });

  const first = await resolver({
    startIso: "2026-06-27T16:40:00.000Z",
    endIso: "2026-06-27T16:50:00.000Z",
  });
  const second = await resolver({
    startIso: "2026-06-27T16:40:00.000Z",
    endIso: "2026-06-27T16:50:00.000Z",
  });
  const third = await resolver({
    startIso: "2026-06-27T16:50:00.000Z",
    endIso: "2026-06-27T17:00:00.000Z",
  });

  assert.equal(first, second);
  assert.notEqual(first, third);
  assert.equal(calls, 2);
  assert.equal(checkpointCaches[0], checkpointCaches[1]);
});

test("UTC ISO week paths are deterministic and do not require directory listing", () => {
  assert.deepEqual(utcIsoWeek("2026-06-27T14:20:00.000Z"), {
    weekYear: 2026,
    week: 26,
    startsAt: "2026-06-22T00:00:00.000Z",
    endsAt: "2026-06-29T00:00:00.000Z",
  });
  assert.equal(
    barsWeekPath("SUI_USDC", "2026-06-27T14:20:00.000Z"),
    "data/SUI_USDC/bars/2026/W26.json",
  );
});

test("weekly bars distinguish filled and empty 10-minute UTC buckets", () => {
  const record = {
    timestamp: "2026-06-27T14:21:00.000Z",
    eventSequenceNumber: "1",
    baseQuantityAtomic: "2000",
    quoteQuantityAtomic: "1000",
    price: { decimal: "0.5" },
  };
  const filled = buildBar(suiUsdc, [record], {
    startIso: "2026-06-27T14:20:00.000Z",
    endIso: "2026-06-27T14:30:00.000Z",
  });
  assert.equal(filled.status, "filled");
  assert.equal(filled.eventCount, 1);
  assert.deepEqual(Object.keys(filled).sort(), [
    "baseVolumeAtomic",
    "close",
    "end",
    "eventCount",
    "high",
    "low",
    "open",
    "quoteVolumeAtomic",
    "start",
    "status",
  ]);

  const empty = buildBar(suiUsdc, [], {
    startIso: "2026-06-27T14:30:00.000Z",
    endIso: "2026-06-27T14:40:00.000Z",
  });
  assert.equal(empty.status, "empty");
  assert.equal(empty.eventCount, 0);
  assert.deepEqual(Object.keys(empty).sort(), [
    "baseVolumeAtomic",
    "close",
    "end",
    "eventCount",
    "high",
    "low",
    "open",
    "quoteVolumeAtomic",
    "start",
    "status",
  ]);

  const merged = mergeWeeklyBars(null, suiUsdc, filled.start, empty);
  assert.equal(merged.week.weekYear, 2026);
  assert.equal(merged.week.week, 26);
  assert.equal(merged.barIntervalMinutes, 10);
  assert.equal(merged.bars.length, 1);
});

test("workflow state separates queued buckets from covered anchors", () => {
  const state = initialPairWorkflowState();
  assert.equal(state.live.firstCoveredBucketStart, null);
  assert.equal(state.live.lastQueuedBucketStart, null);
  assert.equal(state.live.lastCoveredBucketStart, null);
  assert.equal(state.live.lastCoveredCheckpoint, null);
  assert.deepEqual(Object.keys(state.live).sort(), [
    "firstCoveredBucketStart",
    "lastCoveredBucketStart",
    "lastCoveredCheckpoint",
    "lastQueuedBucketStart",
  ]);
  assert.equal(state.backfill.oldestCoveredBucketStart, null);
  assert.equal(state.backfill.oldestCoveredCheckpoint, null);
});

test("backfill cannot run before live collection creates an anchor", () => {
  assert.throws(
    () => backfillAnchorForPairState(initialPairWorkflowState()),
    /requires a live collection anchor/i,
  );

  const state = initialPairWorkflowState();
  state.live.firstCoveredBucketStart = "2026-06-27T16:50:00.000Z";
  assert.equal(backfillAnchorForPairState(state), "2026-06-27T16:50:00.000Z");
});

test("backfill chunk starts one week before the previous covered anchor", () => {
  const state = initialPairWorkflowState();
  state.live.firstCoveredBucketStart = "2026-06-27T16:50:00.000Z";
  assert.equal(backfillAnchorForPairState(state), "2026-06-27T16:50:00.000Z");
  assert.equal(
    backfillChunkStart(
      backfillAnchorForPairState(state),
      "2026-06-01T00:00:00.000Z",
      168,
    ),
    "2026-06-20T16:50:00.000Z",
  );

  state.backfill.oldestCoveredBucketStart = "2026-06-26T16:50:00.000Z";
  assert.equal(backfillAnchorForPairState(state), "2026-06-26T16:50:00.000Z");
  assert.equal(
    backfillChunkStart(
      backfillAnchorForPairState(state),
      "2026-06-01T00:00:00.000Z",
      168,
    ),
    "2026-06-19T16:50:00.000Z",
  );
});

test("first live collection covers the 30-minute window before scheduled collection", () => {
  const state = initialPairWorkflowState();
  assert.deepEqual(
    nextLiveBucketStarts(state, "2026-06-27T16:50:00.000Z", 12, 30),
    [
      "2026-06-27T16:30:00.000Z",
      "2026-06-27T16:40:00.000Z",
      "2026-06-27T16:50:00.000Z",
    ],
  );

  state.live.lastQueuedBucketStart = "2026-06-27T16:50:00.000Z";
  assert.deepEqual(
    nextLiveBucketStarts(state, "2026-06-27T17:20:00.000Z", 12, 30),
    [
      "2026-06-27T17:00:00.000Z",
      "2026-06-27T17:10:00.000Z",
      "2026-06-27T17:20:00.000Z",
    ],
  );
});

test("live collection catch-up is bounded by the configured bucket cap", () => {
  const state = initialPairWorkflowState();
  state.live.lastQueuedBucketStart = "2026-06-27T16:50:00.000Z";

  const starts = nextLiveBucketStarts(
    state,
    "2026-06-27T18:50:00.000Z",
    12,
    30,
  );
  assert.equal(starts.length, 12);
  assert.equal(starts[0], "2026-06-27T17:00:00.000Z");
  assert.equal(starts.at(-1), "2026-06-27T18:50:00.000Z");

  const delayed = nextLiveBucketStarts(
    state,
    "2026-06-27T19:20:00.000Z",
    12,
    30,
  );
  assert.equal(delayed.length, 12);
  assert.equal(delayed[0], "2026-06-27T17:00:00.000Z");
  assert.equal(delayed.at(-1), "2026-06-27T18:50:00.000Z");
});

test("workflow start jitter still produces UTC 10-minute bucket starts", () => {
  assert.equal(
    latestClosedBucketStart(new Date("2026-06-27T17:07:31.123Z"), 10),
    "2026-06-27T16:50:00.000Z",
  );
  assert.equal(
    latestClosedBucketStart(new Date("2026-06-27T17:29:59.999Z"), 10),
    "2026-06-27T17:10:00.000Z",
  );
  assert.equal(
    latestClosedBucketStart(new Date("2026-06-27T17:30:00.000Z"), 10),
    "2026-06-27T17:20:00.000Z",
  );

  const state = initialPairWorkflowState();
  assert.deepEqual(
    nextLiveBucketStarts(
      state,
      latestClosedBucketStart(new Date("2026-06-27T17:07:31.123Z"), 10),
      12,
      30,
    ),
    [
      "2026-06-27T16:30:00.000Z",
      "2026-06-27T16:40:00.000Z",
      "2026-06-27T16:50:00.000Z",
    ],
  );
});

test("manual live collection only queues uncollected closed UTC buckets", () => {
  const state = initialPairWorkflowState();
  state.live.lastQueuedBucketStart = "2026-06-27T16:50:00.000Z";

  assert.deepEqual(
    nextLiveBucketStarts(
      state,
      latestClosedBucketStart(new Date("2026-06-27T17:07:31.123Z"), 10),
      12,
      30,
    ),
    [],
  );

  assert.deepEqual(
    nextLiveBucketStarts(
      state,
      latestClosedBucketStart(new Date("2026-06-27T17:15:00.000Z"), 10),
      12,
      30,
    ),
    ["2026-06-27T17:00:00.000Z"],
  );
});

test("live 24h repair never scans before the live anchor", () => {
  const state = initialPairWorkflowState();
  assert.deepEqual(
    liveRepairBucketStarts(state, "2026-06-27T17:00:00.000Z"),
    [],
  );

  state.live.firstCoveredBucketStart = "2026-06-27T16:40:00.000Z";
  assert.deepEqual(
    liveRepairBucketStarts(state, "2026-06-27T17:00:00.000Z", 24 * 60),
    [
      "2026-06-27T16:40:00.000Z",
      "2026-06-27T16:50:00.000Z",
      "2026-06-27T17:00:00.000Z",
    ],
  );

  state.live.firstCoveredBucketStart = "2026-06-26T00:00:00.000Z";
  const starts = liveRepairBucketStarts(
    state,
    "2026-06-27T17:00:00.000Z",
    24 * 60,
  );
  assert.equal(starts.length, 144);
  assert.equal(starts[0], "2026-06-26T17:10:00.000Z");
  assert.equal(starts.at(-1), "2026-06-27T17:00:00.000Z");
});

test("coverage audit accepts filled empty and tracked missing buckets", async () => {
  const state = initialPairWorkflowState();
  state.live.firstCoveredBucketStart = "2026-06-27T16:30:00.000Z";
  state.live.lastQueuedBucketStart = "2026-06-27T16:50:00.000Z";
  state.missingBuckets.push({
    start: "2026-06-27T16:50:00.000Z",
    end: "2026-06-27T17:00:00.000Z",
    reason: "temporary_graphql_error",
    attempts: 1,
    lastAttemptedAt: "2026-06-27T17:00:00.000Z",
  });

  const result = await auditGeneratedCoverage({
    pairs: [{ id: "SUI_USDC" }],
    workflow: { pairs: { SUI_USDC: state } },
    scanTempFiles: false,
    readBarsFile: async () => ({
      bars: [
        {
          start: "2026-06-27T16:30:00.000Z",
          end: "2026-06-27T16:40:00.000Z",
          status: "filled",
        },
        {
          start: "2026-06-27T16:40:00.000Z",
          end: "2026-06-27T16:50:00.000Z",
          status: "empty",
        },
        {
          start: "2026-06-27T16:50:00.000Z",
          end: "2026-06-27T17:00:00.000Z",
          status: "missing",
        },
      ],
    }),
  });

  assert.equal(result.ok, true);
  assert.equal(result.issues.length, 0);
  assert.equal(result.knownMissing.length, 1);
  assert.equal(result.summaries[0].expected, 3);
});

test("coverage audit fails when covered buckets are absent or state is stale", async () => {
  const state = initialPairWorkflowState();
  state.live.firstCoveredBucketStart = "2026-06-27T16:30:00.000Z";
  state.live.lastQueuedBucketStart = "2026-06-27T16:50:00.000Z";
  state.missingBuckets.push({
    start: "2026-06-27T16:40:00.000Z",
    end: "2026-06-27T16:50:00.000Z",
    reason: "temporary_graphql_error",
    attempts: 1,
    lastAttemptedAt: "2026-06-27T17:00:00.000Z",
  });

  const result = await auditGeneratedCoverage({
    pairs: [{ id: "SUI_USDC" }],
    workflow: { pairs: { SUI_USDC: state } },
    scanTempFiles: false,
    readBarsFile: async () => ({
      bars: [
        {
          start: "2026-06-27T16:30:00.000Z",
          end: "2026-06-27T16:40:00.000Z",
          status: "filled",
        },
        {
          start: "2026-06-27T16:40:00.000Z",
          end: "2026-06-27T16:50:00.000Z",
          status: "empty",
        },
      ],
    }),
  });

  assert.equal(result.ok, false);
  assert.deepEqual(result.issues.map((issue) => issue.type).sort(), [
    "absent_bar",
    "stale_missing_state",
  ]);
});

test("coverage audit fails when generated temp files remain", async () => {
  const root = await mkdtemp(join(tmpdir(), "deepbook-audit-"));
  try {
    const directory = join(root, "SUI_USDC", "bars", "2026");
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, "W26.json.tmp"), "{}\n");

    const result = await auditGeneratedCoverage({
      pairs: [{ id: "SUI_USDC" }],
      workflow: { pairs: {} },
      dataRoot: root,
    });

    assert.equal(result.ok, false);
    assert.deepEqual(
      result.issues.map((issue) => issue.type),
      ["temp_file"],
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("live collection retries missing closed buckets without moving the frontier backward", () => {
  const state = initialPairWorkflowState();
  state.live.lastQueuedBucketStart = "2026-06-27T16:50:00.000Z";
  state.live.firstCoveredBucketStart = "2026-06-27T16:30:00.000Z";
  state.live.lastCoveredBucketStart = "2026-06-27T16:50:00.000Z";
  state.backfill.oldestCoveredBucketStart = "2026-06-27T16:30:00.000Z";
  state.missingBuckets.push({
    start: "2026-06-27T16:40:00.000Z",
    end: "2026-06-27T16:50:00.000Z",
    reason: "temporary_graphql_error",
    attempts: 1,
    lastAttemptedAt: "2026-06-27T17:00:00.000Z",
  });

  assert.deepEqual(
    nextLiveBucketStarts(state, "2026-06-27T17:00:00.000Z", 3, 30),
    ["2026-06-27T16:40:00.000Z", "2026-06-27T17:00:00.000Z"],
  );

  recordLiveBucketAttempt(state, "2026-06-27T17:00:00.000Z", {
    status: "filled",
    fromCheckpoint: "12",
    toCheckpoint: "13",
  });
  recordLiveBucketAttempt(state, "2026-06-27T16:40:00.000Z", {
    status: "filled",
    fromCheckpoint: "10",
    toCheckpoint: "11",
  });
  assert.equal(state.live.lastQueuedBucketStart, "2026-06-27T17:00:00.000Z");
  assert.equal(state.live.firstCoveredBucketStart, "2026-06-27T16:30:00.000Z");
  assert.equal(state.live.lastCoveredBucketStart, "2026-06-27T17:00:00.000Z");
  assert.deepEqual(state.missingBuckets, []);
});

test("backfill covered range clears tracked missing buckets in that range", () => {
  const state = initialPairWorkflowState();
  for (const [start, end] of [
    ["2026-06-27T16:20:00.000Z", "2026-06-27T16:30:00.000Z"],
    ["2026-06-27T16:30:00.000Z", "2026-06-27T16:40:00.000Z"],
    ["2026-06-27T16:40:00.000Z", "2026-06-27T16:50:00.000Z"],
    ["2026-06-27T16:50:00.000Z", "2026-06-27T17:00:00.000Z"],
  ]) {
    state.missingBuckets.push({
      start,
      end,
      reason: "temporary_graphql_error",
      attempts: 1,
      lastAttemptedAt: "2026-06-27T17:00:00.000Z",
    });
  }

  clearMissingBucketsBetween(
    state,
    "2026-06-27T16:30:00.000Z",
    "2026-06-27T16:50:00.000Z",
  );

  assert.deepEqual(
    state.missingBuckets.map((bucket) => bucket.start),
    ["2026-06-27T16:20:00.000Z", "2026-06-27T16:50:00.000Z"],
  );
});

test("resolved local bars are removed from missing workflow state without rereading chain data", async () => {
  const state = initialPairWorkflowState();
  for (const [start, end] of [
    ["2026-06-27T16:20:00.000Z", "2026-06-27T16:30:00.000Z"],
    ["2026-06-27T16:30:00.000Z", "2026-06-27T16:40:00.000Z"],
    ["2026-06-27T16:40:00.000Z", "2026-06-27T16:50:00.000Z"],
  ]) {
    state.missingBuckets.push({
      start,
      end,
      reason: "temporary_graphql_error",
      attempts: 1,
      lastAttemptedAt: "2026-06-27T17:00:00.000Z",
    });
  }

  const [summary] = await clearResolvedMissingBuckets({
    pairs: [suiUsdc],
    workflow: { pairs: { SUI_USDC: state } },
    readBarsFile: async () => ({
      bars: [
        {
          start: "2026-06-27T16:20:00.000Z",
          status: "empty",
        },
        {
          start: "2026-06-27T16:30:00.000Z",
          status: "missing",
        },
      ],
    }),
  });

  assert.deepEqual(summary, {
    pairId: "SUI_USDC",
    cleared: 1,
    remaining: 2,
  });
  assert.deepEqual(
    state.missingBuckets.map((bucket) => bucket.start),
    ["2026-06-27T16:30:00.000Z", "2026-06-27T16:40:00.000Z"],
  );
});

test("workflow state can be reconciled from local public bars", async () => {
  const root = await mkdtemp(join(tmpdir(), "deepbook-reconcile-"));
  try {
    const directory = join(root, "SUI_USDC", "bars", "2026");
    await mkdir(directory, { recursive: true });
    await writeFile(
      join(directory, "W26.json"),
      `${JSON.stringify({
        bars: [
          {
            start: "2026-06-27T16:20:00.000Z",
            end: "2026-06-27T16:30:00.000Z",
            status: "missing",
            missingReason: "checkpoint_resolution_no_checkpoint_in_time_range",
          },
          {
            start: "2026-06-27T16:30:00.000Z",
            end: "2026-06-27T16:40:00.000Z",
            status: "empty",
          },
          {
            start: "2026-06-27T16:40:00.000Z",
            end: "2026-06-27T16:50:00.000Z",
            status: "filled",
          },
        ],
      })}\n`,
    );

    const workflow = {
      schemaVersion: 1,
      barIntervalMinutes: 10,
      updatedAt: "2026-06-27T17:00:00.000Z",
      pairs: {},
    };
    const [summary] = await reconcileWorkflowWithData({
      pairs: [suiUsdc],
      workflow,
      dataRoot: root,
    });

    assert.equal(summary.changed, true);
    assert.equal(
      workflow.pairs.SUI_USDC.live.firstCoveredBucketStart,
      "2026-06-27T16:30:00.000Z",
    );
    assert.equal(
      workflow.pairs.SUI_USDC.live.lastQueuedBucketStart,
      "2026-06-27T16:40:00.000Z",
    );
    assert.equal(
      workflow.pairs.SUI_USDC.live.lastCoveredBucketStart,
      "2026-06-27T16:40:00.000Z",
    );
    assert.equal(workflow.pairs.SUI_USDC.live.lastCoveredCheckpoint, null);
    assert.equal(workflow.pairs.SUI_USDC.backfill.status, "not_started");
    assert.equal(
      workflow.pairs.SUI_USDC.backfill.oldestCoveredBucketStart,
      "2026-06-27T16:30:00.000Z",
    );
    assert.equal(
      workflow.pairs.SUI_USDC.missingBuckets[0].reason,
      "checkpoint_resolution_no_checkpoint_in_time_range",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("coverage audit fails when local data has no workflow source of truth", async () => {
  const root = await mkdtemp(join(tmpdir(), "deepbook-audit-sot-"));
  try {
    const directory = join(root, "SUI_USDC", "bars", "2026");
    await mkdir(directory, { recursive: true });
    await writeFile(
      join(directory, "W26.json"),
      `${JSON.stringify({
        bars: [
          {
            start: "2026-06-27T16:30:00.000Z",
            end: "2026-06-27T16:40:00.000Z",
            status: "empty",
          },
        ],
      })}\n`,
    );

    const result = await auditGeneratedCoverage({
      pairs: [{ id: "SUI_USDC" }],
      workflow: { pairs: {} },
      dataRoot: root,
      scanTempFiles: false,
    });

    assert.equal(result.ok, false);
    assert.deepEqual(
      result.issues.map((issue) => issue.type),
      ["data_without_workflow_state"],
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("coverage audit fails when workflow coverage does not include local data", async () => {
  const root = await mkdtemp(join(tmpdir(), "deepbook-audit-range-"));
  try {
    const directory = join(root, "SUI_USDC", "bars", "2026");
    await mkdir(directory, { recursive: true });
    await writeFile(
      join(directory, "W26.json"),
      `${JSON.stringify({
        bars: [
          {
            start: "2026-06-27T16:30:00.000Z",
            end: "2026-06-27T16:40:00.000Z",
            status: "empty",
          },
          {
            start: "2026-06-27T16:40:00.000Z",
            end: "2026-06-27T16:50:00.000Z",
            status: "empty",
          },
        ],
      })}\n`,
    );
    const state = initialPairWorkflowState();
    state.live.firstCoveredBucketStart = "2026-06-27T16:30:00.000Z";
    state.live.lastQueuedBucketStart = "2026-06-27T16:30:00.000Z";
    state.live.lastCoveredBucketStart = "2026-06-27T16:30:00.000Z";

    const result = await auditGeneratedCoverage({
      pairs: [{ id: "SUI_USDC" }],
      workflow: { pairs: { SUI_USDC: state } },
      dataRoot: root,
      scanTempFiles: false,
    });

    assert.equal(result.ok, false);
    assert.equal(
      result.issues.some(
        (issue) => issue.type === "data_after_workflow_coverage",
      ),
      true,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("backfill window steps backward from the live anchor", () => {
  const window = backfillWindowFromAnchor(
    [
      {
        timestamp: "2026-06-27T16:35:00.000Z",
        checkpoint: "100",
        eventSequenceNumber: "2",
        transactionDigest: "0xbbb",
      },
      {
        timestamp: "2026-06-27T16:05:00.000Z",
        checkpoint: "90",
        eventSequenceNumber: "1",
        transactionDigest: "0xaaa",
      },
      {
        timestamp: "2026-06-27T16:50:00.000Z",
        checkpoint: "110",
        eventSequenceNumber: "3",
        transactionDigest: "0xccc",
      },
    ],
    "2026-06-27T16:50:00.000Z",
  );

  assert.equal(window.oldestBucketStart, "2026-06-27T16:00:00.000Z");
  assert.equal(window.anchor, "2026-06-27T16:50:00.000Z");
  assert.equal(window.oldestCheckpoint, "90");
  assert.deepEqual(
    window.records.map((record) => record.transactionDigest),
    ["0xaaa", "0xbbb"],
  );
});

test("candle OHLC is chronological inside each 10-minute bucket", () => {
  const bar = buildBar(
    suiUsdc,
    [
      {
        timestamp: "2026-06-27T16:08:00.000Z",
        eventSequenceNumber: "3",
        baseQuantityAtomic: "100",
        quoteQuantityAtomic: "200",
        price: { decimal: "2" },
      },
      {
        timestamp: "2026-06-27T16:02:00.000Z",
        eventSequenceNumber: "1",
        baseQuantityAtomic: "100",
        quoteQuantityAtomic: "500",
        price: { decimal: "5" },
      },
      {
        timestamp: "2026-06-27T16:05:00.000Z",
        eventSequenceNumber: "2",
        baseQuantityAtomic: "100",
        quoteQuantityAtomic: "100",
        price: { decimal: "1" },
      },
    ],
    {
      startIso: "2026-06-27T16:00:00.000Z",
      endIso: "2026-06-27T16:10:00.000Z",
    },
  );

  assert.equal(bar.open, "5");
  assert.equal(bar.high, "5");
  assert.equal(bar.low, "1");
  assert.equal(bar.close, "2");
  assert.equal(bar.baseVolumeAtomic, "300");
  assert.equal(bar.quoteVolumeAtomic, "800");
});

test("covered range writer emits empty 10-minute buckets between filled buckets", async () => {
  const record = {
    timestamp: "2026-06-27T14:21:00.000Z",
    eventSequenceNumber: "1",
    checkpoint: "100",
    transactionDigest: "0xabc",
    baseQuantityAtomic: "2000",
    quoteQuantityAtomic: "1000",
    price: { decimal: "0.5" },
  };
  const results = await writeCoveredBucketRange({
    pair: suiUsdc,
    records: [record],
    startIso: "2026-06-27T14:20:00.000Z",
    endExclusiveIso: "2026-06-27T14:40:00.000Z",
    writeGeneratedData: false,
  });
  assert.equal(results.length, 2);
  assert.equal(results[0].status, "filled");
  assert.equal(results[1].status, "empty");
  assert.deepEqual(Object.keys(results[0]).sort(), ["records", "status"]);
  assert.deepEqual(Object.keys(results[1]).sort(), ["records", "status"]);
});

test("retention prunes old weekly bars and advances workflow coverage", async () => {
  const root = await mkdtemp(join(tmpdir(), "deepbook-retention-"));
  try {
    const pair = {
      ...suiUsdc,
      collection: { ...suiUsdc.collection, rollingRetentionYears: 2 },
    };
    const oldDirectory = join(root, "SUI_USDC", "bars", "2023");
    const keptDirectory = join(root, "SUI_USDC", "bars", "2024");
    await mkdir(oldDirectory, { recursive: true });
    await mkdir(keptDirectory, { recursive: true });
    const oldFile = join(oldDirectory, "W52.json");
    const keptFile = join(keptDirectory, "W26.json");
    const bar = (start, end) => ({
      start,
      end,
      status: "empty",
      eventCount: 0,
      open: null,
      high: null,
      low: null,
      close: null,
      baseVolumeAtomic: "0",
      quoteVolumeAtomic: "0",
    });
    await writeFile(
      oldFile,
      `${JSON.stringify(
        {
          schemaVersion: 1,
          pairId: "SUI_USDC",
          week: {
            weekYear: 2023,
            week: 52,
            startsAt: "2023-12-25T00:00:00.000Z",
            endsAt: "2024-01-01T00:00:00.000Z",
            timeZone: "UTC",
          },
          barIntervalMinutes: 10,
          priceConvention: "USDC_PER_BASE",
          disclaimer: registry.quoteAsset.disclaimer,
          bars: [bar("2023-12-25T00:00:00.000Z", "2023-12-25T00:10:00.000Z")],
        },
        null,
        2,
      )}\n`,
    );
    await writeFile(
      keptFile,
      `${JSON.stringify(
        {
          schemaVersion: 1,
          pairId: "SUI_USDC",
          week: {
            weekYear: 2024,
            week: 26,
            startsAt: "2024-06-24T00:00:00.000Z",
            endsAt: "2024-07-01T00:00:00.000Z",
            timeZone: "UTC",
          },
          barIntervalMinutes: 10,
          priceConvention: "USDC_PER_BASE",
          disclaimer: registry.quoteAsset.disclaimer,
          bars: [
            bar("2024-06-27T16:40:00.000Z", "2024-06-27T16:50:00.000Z"),
            bar("2024-06-27T16:50:00.000Z", "2024-06-27T17:00:00.000Z"),
            bar("2024-06-27T17:00:00.000Z", "2024-06-27T17:10:00.000Z"),
          ],
        },
        null,
        2,
      )}\n`,
    );

    const workflow = {
      pairs: {
        SUI_USDC: {
          live: {
            firstCoveredBucketStart: "2024-06-27T16:40:00.000Z",
            lastQueuedBucketStart: "2024-06-27T17:00:00.000Z",
            lastCoveredBucketStart: "2024-06-27T17:00:00.000Z",
            lastCoveredCheckpoint: "12",
          },
          backfill: {
            status: "running",
            oldestCoveredBucketStart: "2023-12-25T00:00:00.000Z",
            oldestCoveredCheckpoint: "1",
            cursor: null,
            stoppedReason: null,
          },
          missingBuckets: [
            {
              start: "2024-06-27T16:40:00.000Z",
              end: "2024-06-27T16:50:00.000Z",
              reason: "temporary_graphql_error",
              attempts: 1,
              lastAttemptedAt: "2026-06-27T17:00:00.000Z",
            },
            {
              start: "2024-06-27T17:00:00.000Z",
              end: "2024-06-27T17:10:00.000Z",
              reason: "temporary_graphql_error",
              attempts: 1,
              lastAttemptedAt: "2026-06-27T17:00:00.000Z",
            },
          ],
        },
      },
    };

    assert.equal(
      retentionCutoffStart("2026-06-27T16:50:00.000Z", 2),
      "2024-06-27T16:50:00.000Z",
    );
    const [summary] = await enforceDataRetention({
      pairs: [pair],
      workflow,
      referenceIso: "2026-06-27T16:50:00.000Z",
      dataRoot: root,
    });

    assert.equal(summary.deletedFiles, 1);
    assert.equal(summary.trimmedBars, 2);
    assert.equal(summary.oldestRetainedBucketStart, "2024-06-27T16:50:00.000Z");
    assert.equal(
      workflow.pairs.SUI_USDC.live.firstCoveredBucketStart,
      "2024-06-27T16:50:00.000Z",
    );
    assert.equal(
      workflow.pairs.SUI_USDC.backfill.oldestCoveredBucketStart,
      "2024-06-27T16:50:00.000Z",
    );
    assert.equal(
      workflow.pairs.SUI_USDC.backfill.oldestCoveredCheckpoint,
      null,
    );
    assert.deepEqual(
      workflow.pairs.SUI_USDC.missingBuckets.map((bucket) => bucket.start),
      ["2024-06-27T17:00:00.000Z"],
    );
    await assert.rejects(() => readFile(oldFile, "utf8"), { code: "ENOENT" });
    const retained = JSON.parse(await readFile(keptFile, "utf8"));
    assert.deepEqual(
      retained.bars.map((item) => item.start),
      ["2024-06-27T16:50:00.000Z", "2024-06-27T17:00:00.000Z"],
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
