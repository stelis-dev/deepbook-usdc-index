import {
  backfillLookbackHoursFromEnv,
  backfillMaxTransactionPagesFromEnv,
  enabledPairs,
  loadRegistry,
  parseArgs,
  shouldWriteGeneratedData,
  writeModeLabel,
} from "./lib/config.mjs";
import {
  createBucketCheckpointRangeResolver,
  createGrpcClient,
  grpcCheckpointRange,
  intersectCheckpointRanges,
} from "./lib/checkpoints.mjs";
import {
  availableTransactionRange,
  firstPoolTransaction,
  scanPoolTransactionsForCheckpointRange,
} from "./lib/events.mjs";
import {
  backfillAnchorForPairState,
  backfillChunkStart,
} from "./lib/backfill.mjs";
import { writeCoveredBucketRange } from "./lib/buckets.mjs";
import {
  BAR_INTERVAL_MINUTES,
  compareIso,
  floorIsoToInterval,
  latestClosedBucketStart,
} from "./lib/paths.mjs";
import {
  clearMissingBucketsBetween,
  loadWorkflowState,
  pairWorkflowState,
  saveWorkflowState,
} from "./lib/state.mjs";
import {
  enforceDataRetention,
  formatRetentionSummary,
  retentionCutoffStart,
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
const pairs = enabledPairs(registry, args.pair);
const eventTypes = registry.eventSources.orderFilledEventTypes;
const workflow = await loadWorkflowState();
const lookbackHours = backfillLookbackHoursFromEnv();
const maxTransactionPages = backfillMaxTransactionPagesFromEnv();
const transactionPageSize = Number(
  process.env.GRAPHQL_TRANSACTION_PAGE_SIZE ?? 50,
);
const grpcClient = createGrpcClient();
const transactionRange = await availableTransactionRange();
const grpcRange = await grpcCheckpointRange(grpcClient);
const retainedRange = intersectCheckpointRanges(transactionRange, grpcRange);
const resolveBackfillCheckpointRange = createBucketCheckpointRangeResolver({
  client: grpcClient,
  retainedRange,
  maxCheckpointQueries: Number(process.env.MAX_CHECKPOINT_QUERIES ?? 80),
});
const latestClosedStart = latestClosedBucketStart(
  new Date(),
  BAR_INTERVAL_MINUTES,
);

console.log(
  `backfill-window-hours: ${lookbackHours}, max-transaction-pages: ${maxTransactionPages}`,
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

for (const pair of pairs) {
  const pairState = pairWorkflowState(workflow, pair.id);
  if (pairState.backfill.status === "complete") {
    console.log(`${pair.id}: backfill already complete`);
    continue;
  }
  const anchor = backfillAnchorForPairState(pairState);
  const firstTransaction = await firstPoolTransaction(pair.poolId);
  if (!firstTransaction) {
    if (writeGeneratedData) {
      pairState.backfill.cursor = null;
      pairState.backfill.status = "complete";
      pairState.backfill.stoppedReason = "no_pool_transactions";
    }
    console.log(`${pair.id}: no pool transactions to backfill`);
    continue;
  }
  const firstBucketStart = floorIsoToInterval(
    new Date(firstTransaction.timestamp),
    BAR_INTERVAL_MINUTES,
  );
  const retentionStart = retentionCutoffStart(
    latestClosedStart,
    pair.collection.rollingRetentionYears,
  );
  const earliestBucketStart =
    compareIso(retentionStart, firstBucketStart) > 0
      ? retentionStart
      : firstBucketStart;
  const completeReason =
    compareIso(retentionStart, firstBucketStart) > 0
      ? "retention_floor_reached"
      : "no_older_pair_events";
  if (compareIso(anchor, earliestBucketStart) <= 0) {
    if (writeGeneratedData) {
      pairState.backfill.cursor = null;
      pairState.backfill.status = "complete";
      pairState.backfill.stoppedReason = completeReason;
    }
    console.log(`${pair.id}: backfill reached ${completeReason}`);
    continue;
  }

  const chunk = await scanBackfillChunk({
    pair,
    eventTypes,
    anchor,
    firstBucketStart: earliestBucketStart,
    lookbackHours,
    pageSize: transactionPageSize,
    maxPages: maxTransactionPages,
    resolveBackfillCheckpointRange,
  });

  if (chunk.status === "unavailable") {
    if (writeGeneratedData) {
      pairState.backfill.cursor = null;
      pairState.backfill.status = "complete";
      pairState.backfill.stoppedReason = chunk.reason;
    }
    console.log(`${pair.id}: completed backfill because ${chunk.reason}`);
    continue;
  }

  if (chunk.status === "too_dense") {
    if (writeGeneratedData) {
      pairState.backfill.status = "stopped";
      pairState.backfill.stoppedReason = `pool_transaction_scan_exceeded_bounds:${chunk.reason}`;
    }
    console.log(
      `${pair.id}: stopped backfill because a 10-minute window exceeded transaction scan bounds`,
    );
    continue;
  }

  const records = recordsInWindow(chunk.page.records, chunk.startIso, anchor);
  if (writeGeneratedData) {
    await writeCoveredBucketRange({
      pair,
      records,
      startIso: chunk.startIso,
      endExclusiveIso: anchor,
      writeGeneratedData,
    });
    clearMissingBucketsBetween(pairState, chunk.startIso, anchor);
    pairState.backfill.oldestCoveredBucketStart = chunk.startIso;
    pairState.backfill.oldestCoveredCheckpoint = chunk.resolved.fromCheckpoint;
    pairState.backfill.cursor = null;
    pairState.backfill.status =
      chunk.startIso === earliestBucketStart ? "complete" : "running";
    pairState.backfill.stoppedReason =
      chunk.startIso === earliestBucketStart ? completeReason : null;
  }
  const oldest = oldestByCheckpoint(chunk.page.transactions);
  const newest = newestByCheckpoint(chunk.page.transactions);
  console.log(
    `${pair.id}: ${writeGeneratedData ? "backfilled" : "would backfill"} ${records.length} fill events from ${chunk.page.transactions.length} pool transactions for ${chunk.startIso}..${anchor}${oldest && newest ? ` (${oldest.timestamp}..${newest.timestamp})` : ""}`,
  );
}

if (writeGeneratedData) {
  const retentionSummaries = await enforceDataRetention({
    pairs,
    workflow,
    referenceIso: latestClosedStart,
    writeGeneratedData,
  });
  for (const summary of retentionSummaries) {
    if (summary.deletedFiles > 0 || summary.trimmedBars > 0) {
      console.log(formatRetentionSummary(summary));
    }
  }
  await saveWorkflowState(workflow);
}

function oldestByCheckpoint(transactions) {
  return (
    [...transactions].sort((a, b) =>
      BigInt(a.checkpoint) < BigInt(b.checkpoint)
        ? -1
        : BigInt(a.checkpoint) > BigInt(b.checkpoint)
          ? 1
          : 0,
    )[0] ?? null
  );
}

function newestByCheckpoint(transactions) {
  return (
    [...transactions].sort((a, b) =>
      BigInt(a.checkpoint) > BigInt(b.checkpoint)
        ? -1
        : BigInt(a.checkpoint) < BigInt(b.checkpoint)
          ? 1
          : 0,
    )[0] ?? null
  );
}

async function scanBackfillChunk(input) {
  let candidateStart = backfillChunkStart(
    input.anchor,
    input.firstBucketStart,
    input.lookbackHours,
  );

  while (true) {
    const resolved = await input.resolveBackfillCheckpointRange({
      startIso: candidateStart,
      endIso: input.anchor,
    });
    if (resolved.status !== "ok") {
      return {
        status: "unavailable",
        reason: `checkpoint_range_${resolved.reason}`,
      };
    }

    const page = await scanPoolTransactionsForCheckpointRange({
      pair: input.pair,
      eventTypes: input.eventTypes,
      poolId: input.pair.poolId,
      fromCheckpoint: resolved.fromCheckpoint,
      toCheckpoint: resolved.toCheckpoint,
      pageSize: input.pageSize,
      maxPages: input.maxPages,
    });
    if (!page.hasMore) {
      return {
        status: "ok",
        startIso: candidateStart,
        resolved,
        page,
      };
    }

    const bucketCount = bucketCountBetween(candidateStart, input.anchor);
    if (bucketCount <= 1) {
      return {
        status: "too_dense",
        reason: page.stoppedReason,
      };
    }

    const nextBucketCount = Math.max(1, Math.floor(bucketCount / 2));
    const requestedStart = backfillChunkStart(
      input.anchor,
      input.firstBucketStart,
      (nextBucketCount * BAR_INTERVAL_MINUTES) / 60,
    );
    if (requestedStart === candidateStart) {
      return {
        status: "too_dense",
        reason: page.stoppedReason,
      };
    }
    candidateStart = requestedStart;
  }
}

function bucketCountBetween(startIso, endIso) {
  const intervalMs = BAR_INTERVAL_MINUTES * 60_000;
  return Math.max(
    0,
    Math.round((Date.parse(endIso) - Date.parse(startIso)) / intervalMs),
  );
}

function recordsInWindow(records, startIso, endIso) {
  return records.filter(
    (record) =>
      compareIso(record.timestamp, startIso) >= 0 &&
      compareIso(record.timestamp, endIso) < 0,
  );
}
