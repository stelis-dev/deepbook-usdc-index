# DeepBook USDC Index

Public DeepBook candle index for selected Sui mainnet pools quoted against canonical Circle USDC.

This repository is a data index, not a trading service. It records 10-minute UTC candles derived from observed DeepBook `OrderFilled` events for registered pools. It does not choose routes, rank venues, provide best-price advice, compute P&L, provide tax or cost-basis output, or treat USDC as fiat USD.

## What This Repository Stores

Public files:

- Pair registry: `registry/pairs.json`
- Weekly 10-minute candles: `data/<PAIR>/bars/<ISO_WEEK_YEAR>/W<ISO_WEEK>.json`

The public data schema is bars-only: one registry file plus weekly 10-minute candle files.

`USDC` in this repository means the Sui mainnet Circle USDC coin type pinned in `registry/pairs.json`. It is not fiat USD and is not a USDC/USD peg guarantee.

## Source Of Truth

- Pair configuration: `registry/pairs.json`
- Public candle data: `data/<PAIR>/bars/<ISO_WEEK_YEAR>/W<ISO_WEEK>.json`
- Collection workflow state: `_workflow/missing.json`

If workflow state is missing or stale, it can be reconciled from local candle files. Reconciliation does not recreate checkpoint metadata that is not stored in public candle files; later collection runs refresh checkpoint fields.

## Pair Registry Format

`registry/pairs.json` is the source of truth for which DeepBook pools this repository indexes. To add a pair, append one object to `pairs`:

```json
{
  "id": "SUI_USDC",
  "enabled": true,
  "poolId": "0xe05dafb5133bcffb8d59f4e12465dc0e9faeaa05e3e342a08fe135800e3e4407",
  "baseAsset": {
    "symbol": "SUI",
    "coinType": "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI",
    "decimals": 9
  },
  "quoteAsset": "USDC",
  "priceConvention": "USDC_PER_BASE",
  "collection": {
    "barIntervalMinutes": 10,
    "rollingRetentionYears": 2
  }
}
```

Field rules:

- `id`: stable uppercase pair id used in file paths, formatted as `<BASE>_USDC`.
- `enabled`: only `true` pairs are included in generated data.
- `poolId`: DeepBook pool object id. History is scoped to this pool object; an older pool object must be registered separately if it should be indexed.
- `baseAsset`: token symbol, full Sui coin type, and decimals for the asset priced against USDC.
- `quoteAsset`: must be `USDC`. This repository does not silently choose another quote asset.
- `priceConvention`: must be `USDC_PER_BASE`, meaning the candle prices are denominated in USDC units per one base token.
- `collection.barIntervalMinutes`: fixed at `10`; public files are UTC 10-minute candles.
- `collection.rollingRetentionYears`: generated weekly candles older than this rolling window are pruned after collection or backfill runs.

The top-level `quoteAsset` pins canonical Circle USDC on Sui mainnet. Do not add WUSDC, USDT, DBUSDC, or fiat USD as this repository's quote asset.

## Public Read Paths

Consumers do not need a directory index or GitHub folder listing for normal reads. If a consumer knows the repository, pair id, and date, it can compute the UTC ISO week file path and fetch that file directly:

```text
https://cdn.jsdelivr.net/gh/stelis-dev/deepbook-usdc-index@main/registry/pairs.json
https://cdn.jsdelivr.net/gh/stelis-dev/deepbook-usdc-index@main/data/SUI_USDC/bars/2026/W26.json
```

Weekly files use ISO-8601 UTC week-year and week number. Candle boundaries are always 10-minute UTC boundaries. A weekly file contains the 10-minute candles that have been written for that ISO week. Each candle has a `status`:

- `filled`: the bucket was scanned and has one or more DeepBook fills; OHLCV fields are populated.
- `empty`: the bucket was scanned and no fills were observed; OHLC fields are `null` and volumes are zero.
- `missing`: the bucket could not be completed; consumers should treat it as unavailable unless a later file version replaces it.

A consumer that wants a date range should compute every UTC ISO week touched by that range, fetch those weekly files, and filter the `bars` array by `start` and `end`. Direct file URLs cannot list directories. GitHub's REST Contents and Git Trees APIs can list repository paths, but this index is designed so normal readers do not need those APIs.

### TypeScript Read Example

This example reads weekly files directly from direct file URLs and filters the 10-minute UTC bars for a requested time range. It reads USDC-denominated DeepBook fill candles; it does not convert USDC to fiat USD.

```ts
type DeepBookBar = {
  start: string;
  end: string;
  status: "filled" | "empty" | "missing";
  eventCount: number | null;
  open: string | null;
  high: string | null;
  low: string | null;
  close: string | null;
  baseVolumeAtomic: string | null;
  quoteVolumeAtomic: string | null;
};

type WeeklyBarsFile = {
  pairId: string;
  barIntervalMinutes: 10;
  bars: DeepBookBar[];
};

const OWNER = "stelis-dev";
const REPO = "deepbook-usdc-index";
const BRANCH = "main";

export async function readDeepBookUsdcBars(input: {
  pair: string;
  start: string;
  end: string;
}): Promise<DeepBookBar[]> {
  const weeks = utcIsoWeeksBetween(input.start, input.end);
  const files = await Promise.all(
    weeks.map(async ({ weekYear, week }) => {
      const url = `https://cdn.jsdelivr.net/gh/${OWNER}/${REPO}@${BRANCH}/data/${input.pair}/bars/${weekYear}/W${String(week).padStart(2, "0")}.json`;
      const response = await fetch(url);
      if (response.status === 404) return null;
      if (!response.ok)
        throw new Error(`Failed to fetch ${url}: ${response.status}`);
      return (await response.json()) as WeeklyBarsFile;
    }),
  );
  const startMs = Date.parse(input.start);
  const endMs = Date.parse(input.end);
  return files
    .flatMap((file) => file?.bars ?? [])
    .filter(
      (bar) =>
        Date.parse(bar.start) >= startMs && Date.parse(bar.start) < endMs,
    )
    .sort((left, right) => Date.parse(left.start) - Date.parse(right.start));
}

function utcIsoWeeksBetween(startIso: string, endIso: string) {
  const result: Array<{ weekYear: number; week: number }> = [];
  const seen = new Set<string>();
  for (
    let day = utcDateFloor(startIso);
    day.getTime() < Date.parse(endIso);
    day.setUTCDate(day.getUTCDate() + 1)
  ) {
    const week = utcIsoWeek(day);
    const key = `${week.weekYear}-W${week.week}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(week);
    }
  }
  return result;
}

function utcIsoWeek(input: Date) {
  const day = new Date(
    Date.UTC(input.getUTCFullYear(), input.getUTCMonth(), input.getUTCDate()),
  );
  const weekday = day.getUTCDay() || 7;
  day.setUTCDate(day.getUTCDate() + 4 - weekday);
  const weekYear = day.getUTCFullYear();
  const firstThursday = new Date(Date.UTC(weekYear, 0, 4));
  const firstWeekday = firstThursday.getUTCDay() || 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() + 4 - firstWeekday);
  const week =
    1 + Math.round((day.getTime() - firstThursday.getTime()) / 604_800_000);
  return { weekYear, week };
}

function utcDateFloor(iso: string) {
  const date = new Date(iso);
  return new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
}
```

## Data Meaning

Weekly candle files contain observed DeepBook fills for the currently registered pool object. If a market used an older pool object before the current one, that older object must be registered separately before this repository can index that earlier period.

Candle calculation is chronological inside each 10-minute bucket: `open` is the earliest fill in the bucket, `close` is the latest fill in the bucket, `high` and `low` are extrema over the bucket, and volume fields are sums.

The candles are not:

- fiat USD prices;
- P&L, tax, or cost basis;
- a best route or best price claim;
- a promise that fills before the currently registered pool object first appears in GraphQL transaction history are included;
- evidence that a user can execute a trade at the listed candle price.

## Local Check

```bash
npm install
npm run check
```
