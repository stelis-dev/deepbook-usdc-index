export const BAR_INTERVAL_MINUTES = 10;

export function barsWeekPath(pairId, startIso) {
  const week = utcIsoWeek(startIso);
  return `data/${pairId}/bars/${week.weekYear}/W${two(week.week)}.json`;
}

export function workflowMissingPath() {
  return `_workflow/missing.json`;
}

export function utcIsoWeek(input) {
  const date =
    typeof input === "string" ? new Date(input) : new Date(input.getTime());
  const day = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
  const weekday = day.getUTCDay() || 7;
  day.setUTCDate(day.getUTCDate() + 4 - weekday);
  const weekYear = day.getUTCFullYear();
  const firstThursday = new Date(Date.UTC(weekYear, 0, 4));
  const firstWeekday = firstThursday.getUTCDay() || 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() + 4 - firstWeekday);
  const week =
    1 + Math.round((day.getTime() - firstThursday.getTime()) / 604_800_000);
  return {
    weekYear,
    week,
    startsAt: isoWeekStart(weekYear, week),
    endsAt: addMinutes(isoWeekStart(weekYear, week), 7 * 24 * 60),
  };
}

export function floorIsoToInterval(date, minutes = BAR_INTERVAL_MINUTES) {
  const ms = date.getTime();
  const intervalMs = minutes * 60_000;
  return new Date(Math.floor(ms / intervalMs) * intervalMs).toISOString();
}

export function latestClosedBucketStart(
  now = new Date(),
  minutes = BAR_INTERVAL_MINUTES,
) {
  return addMinutes(floorIsoToInterval(now, minutes), -minutes);
}

export function addMinutes(iso, minutes) {
  return new Date(Date.parse(iso) + minutes * 60_000).toISOString();
}

export function addHours(iso, hours) {
  return new Date(Date.parse(iso) + hours * 3_600_000).toISOString();
}

export function bucketStartsBetween(
  startIso,
  endExclusiveIso,
  minutes = BAR_INTERVAL_MINUTES,
) {
  const starts = [];
  for (
    let current = startIso;
    compareIso(current, endExclusiveIso) < 0;
    current = addMinutes(current, minutes)
  ) {
    starts.push(current);
  }
  return starts;
}

export function compareIso(left, right) {
  return Date.parse(left) - Date.parse(right);
}

function isoWeekStart(weekYear, week) {
  const jan4 = new Date(Date.UTC(weekYear, 0, 4));
  const weekday = jan4.getUTCDay() || 7;
  const monday = new Date(jan4.getTime());
  monday.setUTCDate(jan4.getUTCDate() - weekday + 1 + (week - 1) * 7);
  monday.setUTCHours(0, 0, 0, 0);
  return monday.toISOString();
}

function two(value) {
  return String(value).padStart(2, "0");
}
