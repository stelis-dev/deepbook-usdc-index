import { readJson } from "./io.mjs";
import { BAR_INTERVAL_MINUTES } from "./paths.mjs";

export const DEFAULT_GRAPHQL_URL = "https://graphql.mainnet.sui.io/graphql";
export const DEFAULT_GRPC_URL = "https://fullnode.mainnet.sui.io:443";
export const DEFAULT_COLLECT_MAX_BUCKETS_PER_RUN = 12;
export const DEFAULT_COLLECT_INITIAL_LOOKBACK_MINUTES = 30;
export const DEFAULT_REPAIR_LIVE_LOOKBACK_HOURS = 24;
export const DEFAULT_BACKFILL_LOOKBACK_HOURS_PER_RUN = 168;
export const DEFAULT_BACKFILL_MAX_TRANSACTION_PAGES_PER_WINDOW = 80;
export const REPAIR_LIVE_LOOKBACK_HOUR_CHOICES = [6, 12, 18, 24];
export const LIVE_RUN_MODES = ["collect", "repair"];
export const USDC_DISCLAIMER =
  "USDC is a token-denominated reference asset in this index. It is not fiat USD and this repository does not guarantee a USDC/USD peg.";

export async function loadRegistry() {
  const registry = await readJson("registry/pairs.json");
  validateRegistry(registry);
  return registry;
}

export function enabledPairs(registry, pairId) {
  const pairs = registry.pairs.filter((pair) => pair.enabled);
  if (!pairId) {
    return pairs;
  }
  const pair = pairs.find((candidate) => candidate.id === pairId);
  if (!pair) {
    throw new Error(`Unknown or disabled pair: ${pairId}`);
  }
  return [pair];
}

export function parseArgs(argv = process.argv.slice(2)) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const item = argv[index];
    if (!item.startsWith("--")) {
      continue;
    }
    const key = item.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      args[key] = "true";
      continue;
    }
    args[key] = next;
    index += 1;
  }
  return args;
}

export function shouldWriteGeneratedData(args) {
  if (args["dry-run"] === "true") {
    return false;
  }
  return process.env.GITHUB_ACTIONS === "true";
}

export function writeModeLabel(writeGeneratedData) {
  return writeGeneratedData ? "github-actions-write" : "local-dry-run";
}

export function collectMaxBucketsFromEnv() {
  const value = Number(
    process.env.COLLECT_MAX_BUCKETS_PER_RUN ??
      DEFAULT_COLLECT_MAX_BUCKETS_PER_RUN,
  );
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error("COLLECT_MAX_BUCKETS_PER_RUN must be a positive integer");
  }
  return value;
}

export function collectInitialLookbackMinutesFromEnv() {
  const value = Number(
    process.env.COLLECT_INITIAL_LOOKBACK_MINUTES ??
      DEFAULT_COLLECT_INITIAL_LOOKBACK_MINUTES,
  );
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(
      "COLLECT_INITIAL_LOOKBACK_MINUTES must be a positive integer",
    );
  }
  return value;
}

export function repairLiveLookbackHoursFromInput(args) {
  const value = Number(
    args.hours ??
      process.env.REPAIR_LIVE_LOOKBACK_HOURS ??
      DEFAULT_REPAIR_LIVE_LOOKBACK_HOURS,
  );
  if (
    !Number.isSafeInteger(value) ||
    !REPAIR_LIVE_LOOKBACK_HOUR_CHOICES.includes(value)
  ) {
    throw new Error(
      `Repair lookback hours must be one of: ${REPAIR_LIVE_LOOKBACK_HOUR_CHOICES.join(", ")}`,
    );
  }
  return value;
}

export function backfillLookbackHoursFromEnv() {
  const value = Number(
    process.env.BACKFILL_LOOKBACK_HOURS_PER_RUN ??
      DEFAULT_BACKFILL_LOOKBACK_HOURS_PER_RUN,
  );
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(
      "BACKFILL_LOOKBACK_HOURS_PER_RUN must be a positive integer",
    );
  }
  return value;
}

export function backfillMaxTransactionPagesFromEnv() {
  const value = Number(
    process.env.BACKFILL_MAX_TRANSACTION_PAGES_PER_WINDOW ??
      DEFAULT_BACKFILL_MAX_TRANSACTION_PAGES_PER_WINDOW,
  );
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(
      "BACKFILL_MAX_TRANSACTION_PAGES_PER_WINDOW must be a positive integer",
    );
  }
  return value;
}

export function liveRunModeFromInput(args) {
  const mode = args.mode ?? process.env.LIVE_RUN_MODE ?? "collect";
  if (!LIVE_RUN_MODES.includes(mode)) {
    throw new Error(
      `Live run mode must be one of: ${LIVE_RUN_MODES.join(", ")}`,
    );
  }
  return mode;
}

function validateRegistry(registry) {
  if (
    !registry ||
    registry.schemaVersion !== 1 ||
    registry.network !== "sui:mainnet"
  ) {
    throw new Error(
      "registry/pairs.json must be schemaVersion 1 for sui:mainnet",
    );
  }
  if (
    registry.quoteAsset?.symbol !== "USDC" ||
    registry.quoteAsset?.decimals !== 6
  ) {
    throw new Error(
      "registry quoteAsset must be canonical USDC with 6 decimals",
    );
  }
  const seen = new Set();
  for (const pair of registry.pairs ?? []) {
    if (seen.has(pair.id)) {
      throw new Error(`Duplicate pair id: ${pair.id}`);
    }
    seen.add(pair.id);
    if (
      !pair.id.endsWith("_USDC") ||
      pair.quoteAsset !== "USDC" ||
      pair.priceConvention !== "USDC_PER_BASE"
    ) {
      throw new Error(`Pair ${pair.id} must use USDC_PER_BASE`);
    }
    if (!pair.poolId?.startsWith("0x")) {
      throw new Error(`Pair ${pair.id} has invalid poolId`);
    }
    if (pair.collection?.barIntervalMinutes !== BAR_INTERVAL_MINUTES) {
      throw new Error(`Pair ${pair.id} must use 10-minute UTC bars`);
    }
  }
}
