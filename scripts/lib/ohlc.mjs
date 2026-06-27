import { USDC_DISCLAIMER } from "./config.mjs";
import { DEFAULT_BAR_INTERVAL_MINUTES, utcIsoWeek } from "./paths.mjs";

export function buildBar(pair, fillRecords, input) {
  const records = sortForOhlc(fillRecords);
  if (input.status === "missing") {
    return missingBar(
      input.startIso,
      input.endIso,
      input.missingReason ?? "collection_failed",
    );
  }
  if (records.length === 0) {
    return emptyBar(input.startIso, input.endIso);
  }
  let open = null;
  let high = null;
  let low = null;
  let close = null;
  let baseVolume = 0n;
  let quoteVolume = 0n;
  for (const record of records) {
    const price = record.price.decimal;
    if (open === null) open = price;
    if (high === null || compareDecimal(price, high) > 0) high = price;
    if (low === null || compareDecimal(price, low) < 0) low = price;
    close = price;
    baseVolume += BigInt(record.baseQuantityAtomic);
    quoteVolume += BigInt(record.quoteQuantityAtomic);
  }
  return {
    start: input.startIso,
    end: input.endIso,
    status: "filled",
    eventCount: records.length,
    open,
    high,
    low,
    close,
    baseVolumeAtomic: baseVolume.toString(),
    quoteVolumeAtomic: quoteVolume.toString(),
  };
}

export function mergeWeeklyBars(existing, pair, startIso, bar) {
  const week = utcIsoWeek(startIso);
  const barIntervalMinutes =
    pair.collection?.barIntervalMinutes ?? DEFAULT_BAR_INTERVAL_MINUTES;
  if (
    existing?.barIntervalMinutes !== undefined &&
    existing.barIntervalMinutes !== barIntervalMinutes
  ) {
    throw new Error(
      `${pair.id} ${startIso} week file uses ${existing.barIntervalMinutes}-minute bars but pair registry uses ${barIntervalMinutes}-minute bars`,
    );
  }
  const base = existing ?? {
    schemaVersion: 1,
    pairId: pair.id,
    week: {
      weekYear: week.weekYear,
      week: week.week,
      startsAt: week.startsAt,
      endsAt: week.endsAt,
      timeZone: "UTC",
    },
    barIntervalMinutes,
    priceConvention: "USDC_PER_BASE",
    disclaimer: USDC_DISCLAIMER,
    bars: [],
  };
  const bars = base.bars.filter((item) => item.start !== bar.start);
  bars.push(bar);
  bars.sort((a, b) => Date.parse(a.start) - Date.parse(b.start));
  return { ...base, bars };
}

export function missingBar(startIso, endIso, reason) {
  return {
    start: startIso,
    end: endIso,
    status: "missing",
    eventCount: null,
    open: null,
    high: null,
    low: null,
    close: null,
    baseVolumeAtomic: null,
    quoteVolumeAtomic: null,
    missingReason: reason,
  };
}

export function emptyBar(startIso, endIso) {
  return {
    start: startIso,
    end: endIso,
    status: "empty",
    eventCount: 0,
    open: null,
    high: null,
    low: null,
    close: null,
    baseVolumeAtomic: "0",
    quoteVolumeAtomic: "0",
  };
}

function sortForOhlc(records) {
  return [...records].sort((a, b) => {
    const time = Date.parse(a.timestamp) - Date.parse(b.timestamp);
    if (time !== 0) return time;
    const sequence =
      BigInt(a.eventSequenceNumber) - BigInt(b.eventSequenceNumber);
    return sequence < 0n ? -1 : sequence > 0n ? 1 : 0;
  });
}

function compareDecimal(left, right) {
  const [leftWhole, leftFrac = ""] = left.split(".");
  const [rightWhole, rightFrac = ""] = right.split(".");
  const wholeDiff = BigInt(leftWhole) - BigInt(rightWhole);
  if (wholeDiff !== 0n) return wholeDiff < 0n ? -1 : 1;
  const width = Math.max(leftFrac.length, rightFrac.length);
  const fracDiff =
    BigInt((leftFrac || "0").padEnd(width, "0")) -
    BigInt((rightFrac || "0").padEnd(width, "0"));
  return fracDiff < 0n ? -1 : fracDiff > 0n ? 1 : 0;
}
