import {
  createBucketCheckpointRangeResolver,
  createGrpcClient,
  grpcCheckpointRange,
  intersectCheckpointRanges,
} from "./checkpoints.mjs";
import { availableEventRange, firstPoolTransaction } from "./events.mjs";
import { collectLiveBucket } from "./live-collect.mjs";
import { DEFAULT_BAR_INTERVAL_MINUTES, addMinutes } from "./paths.mjs";
import { pairWorkflowState, recordLiveBucketAttempt } from "./state.mjs";

export async function createLiveCollectionContext(registry) {
  const grpcClient = createGrpcClient();
  const eventType = registry.eventSources.orderFilledEventTypes[0];
  const graphqlRange = await availableEventRange(eventType);
  const grpcRange = await grpcCheckpointRange(grpcClient);
  const retainedRange = intersectCheckpointRanges(graphqlRange, grpcRange);
  return {
    eventType,
    resolveBucketCheckpointRange: createBucketCheckpointRangeResolver({
      client: grpcClient,
      retainedRange,
      maxCheckpointQueries: Number(process.env.MAX_CHECKPOINT_QUERIES ?? 80),
    }),
  };
}

export async function runLiveBucketJob(input) {
  const barIntervalMinutes =
    input.barIntervalMinutes ?? DEFAULT_BAR_INTERVAL_MINUTES;
  for (const pair of input.pairs) {
    const pairState = pairWorkflowState(input.workflow, pair.id);
    const starts = input.bucketStartsForPair(pairState, pair);
    if (starts.length === 0) {
      console.log(`${pair.id}: ${input.emptyMessage}`);
      continue;
    }

    const firstTransaction = await firstPoolTransaction(pair.poolId);
    for (const startIso of starts) {
      const endIso = addMinutes(startIso, barIntervalMinutes);
      const resolvedCheckpointRange = await input.resolveBucketCheckpointRange({
        startIso,
        endIso,
      });
      const result = await collectLiveBucket({
        pair,
        eventType: input.eventType,
        firstTransaction,
        resolvedCheckpointRange,
        startIso,
        endIso,
        writeGeneratedData: input.writeGeneratedData,
      });
      if (input.writeGeneratedData) {
        recordLiveBucketAttempt(
          pairState,
          startIso,
          result,
          barIntervalMinutes,
        );
      }
      console.log(
        `${pair.id}: ${input.writeGeneratedData ? input.writeVerb : input.dryRunVerb} ${result.records.length} events for ${startIso}..${endIso} (${result.status})`,
      );
    }
  }
}
