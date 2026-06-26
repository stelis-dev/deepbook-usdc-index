import { sortFillRecords } from "./events.mjs";
import { readJson, writeJson } from "./io.mjs";
import { buildBar, mergeWeeklyBars, missingBar } from "./ohlc.mjs";
import {
  BAR_INTERVAL_MINUTES,
  addMinutes,
  barsWeekPath,
  bucketStartsBetween,
  floorIsoToInterval,
} from "./paths.mjs";

export async function writeCoveredBucket(input) {
  const records = sortFillRecords(input.records);
  if (input.writeGeneratedData) {
    const barsPath = barsWeekPath(input.pair.id, input.startIso);
    const existing = await readJson(barsPath, undefined);
    const bar = buildBar(input.pair, records, {
      startIso: input.startIso,
      endIso: input.endIso,
    });
    await writeJson(
      barsPath,
      mergeWeeklyBars(existing, input.pair, input.startIso, bar),
    );
  }
  return {
    status: records.length > 0 ? "filled" : "empty",
    records,
  };
}

export async function writeCoveredBucketRange(input) {
  const groups = new Map();
  for (const record of input.records) {
    const startIso = floorIsoToInterval(
      new Date(record.timestamp),
      BAR_INTERVAL_MINUTES,
    );
    const group = groups.get(startIso) ?? [];
    group.push(record);
    groups.set(startIso, group);
  }
  const results = [];
  for (const startIso of bucketStartsBetween(
    input.startIso,
    input.endExclusiveIso,
    BAR_INTERVAL_MINUTES,
  )) {
    results.push(
      await writeCoveredBucket({
        pair: input.pair,
        startIso,
        endIso: addMinutes(startIso, BAR_INTERVAL_MINUTES),
        records: groups.get(startIso) ?? [],
        writeGeneratedData: input.writeGeneratedData,
      }),
    );
  }
  return results;
}

export async function writeMissingBucket(input) {
  if (input.writeGeneratedData) {
    const barsPath = barsWeekPath(input.pair.id, input.startIso);
    const existing = await readJson(barsPath, undefined);
    const bar = missingBar(input.startIso, input.endIso, input.reason);
    await writeJson(
      barsPath,
      mergeWeeklyBars(existing, input.pair, input.startIso, bar),
    );
  }
  return {
    status: "missing",
    records: [],
  };
}
