import { setTimeout as sleep } from "node:timers/promises";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { DEFAULT_GRPC_URL } from "./config.mjs";

const GRPC_LOWEST_CHECKPOINT_SAFETY_MARGIN = 1000n;

export function createGrpcClient() {
  return new SuiGrpcClient({
    baseUrl: process.env.SUI_GRPC_URL ?? DEFAULT_GRPC_URL,
    network: "mainnet",
  });
}

export async function latestCheckpoint(client) {
  const range = await grpcCheckpointRange(client);
  return range.last;
}

export async function grpcCheckpointRange(client) {
  const response = await client.ledgerService.getServiceInfo({}).response;
  if (
    typeof response.checkpointHeight !== "bigint" ||
    !response.timestamp ||
    typeof response.lowestAvailableCheckpoint !== "bigint"
  ) {
    throw new Error(
      "Sui gRPC service info returned no retained checkpoint range",
    );
  }
  return {
    first: {
      sequenceNumber: (
        response.lowestAvailableCheckpoint +
        GRPC_LOWEST_CHECKPOINT_SAFETY_MARGIN
      ).toString(),
      timestamp: null,
    },
    last: {
      sequenceNumber: response.checkpointHeight.toString(),
      timestamp: isoFromGrpcTimestamp(response.timestamp),
    },
  };
}

export function intersectCheckpointRanges(left, right) {
  const first = maxCheckpoint(
    left.first.sequenceNumber,
    right.first.sequenceNumber,
  );
  const last = minCheckpoint(
    left.last.sequenceNumber,
    right.last.sequenceNumber,
  );
  if (BigInt(first) > BigInt(last)) {
    throw new Error(
      "Sui gRPC and GraphQL retained checkpoint ranges do not overlap",
    );
  }
  return {
    first: { sequenceNumber: first, timestamp: null },
    last: { sequenceNumber: last, timestamp: null },
  };
}

export async function checkpointBySequence(client, sequenceNumber) {
  assertCheckpointNumber(sequenceNumber);
  const response = await retryResourceExhausted(
    () =>
      client.ledgerService.getCheckpoint({
        checkpointId: {
          oneofKind: "sequenceNumber",
          sequenceNumber: BigInt(sequenceNumber),
        },
        readMask: { paths: ["sequence_number", "summary.timestamp"] },
      }).response,
  );
  const checkpoint = response.checkpoint;
  if (
    !checkpoint ||
    typeof checkpoint.sequenceNumber !== "bigint" ||
    !checkpoint.summary?.timestamp
  ) {
    throw new Error(
      `Sui gRPC checkpoint lookup returned an unexpected shape for ${sequenceNumber}`,
    );
  }
  return {
    sequenceNumber: checkpoint.sequenceNumber.toString(),
    timestamp: isoFromGrpcTimestamp(checkpoint.summary.timestamp),
  };
}

export function createBucketCheckpointRangeResolver(input) {
  const bucketCache = new Map();
  const checkpointCache = new Map();
  const resolveRange = input.resolveRange ?? resolveCheckpointRangeForTime;
  return async function resolveBucketCheckpointRange({ startIso, endIso }) {
    const key = `${startIso}\0${endIso}`;
    if (!bucketCache.has(key)) {
      bucketCache.set(
        key,
        resolveRange(input.client, {
          startIso,
          endIso,
          retainedFirstCheckpoint: input.retainedRange.first.sequenceNumber,
          retainedLastCheckpoint: input.retainedRange.last.sequenceNumber,
          maxCheckpointQueries: input.maxCheckpointQueries,
          checkpointCache,
        }),
      );
    }
    return await bucketCache.get(key);
  };
}

export async function resolveCheckpointRangeForTime(client, input) {
  const startMs = Date.parse(input.startIso);
  const endMs = Date.parse(input.endIso);
  if (Number.isNaN(startMs) || Number.isNaN(endMs) || startMs > endMs) {
    throw new Error("Invalid checkpoint time range");
  }
  const firstSequence = numberFromCheckpoint(input.retainedFirstCheckpoint);
  const lastSequence = numberFromCheckpoint(input.retainedLastCheckpoint);
  if (firstSequence > lastSequence) {
    return {
      status: "empty",
      reason: "retained_range_inverted",
      queryCount: 0,
    };
  }
  let queryCount = 0;
  const cache = input.checkpointCache ?? new Map();
  const getCheckpoint = async (sequenceNumber) => {
    if (cache.has(sequenceNumber)) {
      return await cache.get(sequenceNumber);
    }
    queryCount += 1;
    if (queryCount > input.maxCheckpointQueries) {
      throw new Error(
        `Checkpoint resolver exceeded query limit ${input.maxCheckpointQueries}`,
      );
    }
    const checkpointRequest = checkpointBySequence(
      client,
      sequenceNumber,
    ).catch((error) => {
      cache.delete(sequenceNumber);
      throw error;
    });
    cache.set(sequenceNumber, checkpointRequest);
    const checkpoint = await checkpointRequest;
    cache.set(sequenceNumber, checkpoint);
    return checkpoint;
  };

  const first = await getCheckpoint(firstSequence);
  const last = await getCheckpoint(lastSequence);
  const firstMs = Date.parse(first.timestamp);
  const lastMs = Date.parse(last.timestamp);
  if (endMs < firstMs) {
    return {
      status: "empty",
      reason: "range_before_retained_data",
      queryCount,
    };
  }
  if (startMs > lastMs) {
    return {
      status: "empty",
      reason: "range_after_latest_checkpoint",
      queryCount,
    };
  }
  const lower = await firstCheckpointAtOrAfter(
    getCheckpoint,
    firstSequence,
    lastSequence,
    Math.max(startMs, firstMs),
  );
  const upper = await lastCheckpointAtOrBefore(
    getCheckpoint,
    firstSequence,
    lastSequence,
    Math.min(endMs, lastMs),
  );
  if (lower === undefined || upper === undefined || lower > upper) {
    return {
      status: "empty",
      reason: "no_checkpoint_in_time_range",
      queryCount,
    };
  }
  const [from, to] = await Promise.all([
    getCheckpoint(lower),
    getCheckpoint(upper),
  ]);
  return {
    status: "ok",
    fromCheckpoint: from.sequenceNumber,
    toCheckpoint: to.sequenceNumber,
    fromTimestamp: from.timestamp,
    toTimestamp: to.timestamp,
    queryCount,
  };
}

async function firstCheckpointAtOrAfter(
  getCheckpoint,
  firstSequence,
  lastSequence,
  timestampMs,
) {
  let low = firstSequence;
  let high = lastSequence;
  let answer;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const checkpoint = await getCheckpoint(mid);
    if (Date.parse(checkpoint.timestamp) >= timestampMs) {
      answer = mid;
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return answer;
}

async function lastCheckpointAtOrBefore(
  getCheckpoint,
  firstSequence,
  lastSequence,
  timestampMs,
) {
  let low = firstSequence;
  let high = lastSequence;
  let answer;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const checkpoint = await getCheckpoint(mid);
    if (Date.parse(checkpoint.timestamp) <= timestampMs) {
      answer = mid;
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  return answer;
}

function maxCheckpoint(left, right) {
  return BigInt(left) >= BigInt(right) ? String(left) : String(right);
}

function minCheckpoint(left, right) {
  return BigInt(left) <= BigInt(right) ? String(left) : String(right);
}

function numberFromCheckpoint(value) {
  const number = Number(value);
  assertCheckpointNumber(number);
  return number;
}

function assertCheckpointNumber(value) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Invalid checkpoint number: ${value}`);
  }
}

async function retryResourceExhausted(operation) {
  const configuredMaxAttempts = Number(process.env.SUI_GRPC_MAX_RETRIES ?? 4);
  const maxAttempts =
    Number.isSafeInteger(configuredMaxAttempts) && configuredMaxAttempts > 0
      ? configuredMaxAttempts
      : 4;
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (!isResourceExhausted(error) || attempt >= maxAttempts) {
        throw error;
      }
      await sleep(250 * 2 ** (attempt - 1));
    }
  }
}

function isResourceExhausted(error) {
  return (
    error?.code === "RESOURCE_EXHAUSTED" ||
    /Too Many Requests/i.test(error?.message ?? "")
  );
}

function isoFromGrpcTimestamp(value) {
  const seconds = value.seconds;
  const nanos = value.nanos ?? 0;
  if (
    typeof seconds !== "bigint" ||
    !Number.isInteger(nanos) ||
    nanos < 0 ||
    nanos >= 1_000_000_000
  ) {
    throw new Error("Invalid gRPC timestamp");
  }
  return new Date(
    Number(seconds) * 1000 + Math.floor(nanos / 1_000_000),
  ).toISOString();
}
