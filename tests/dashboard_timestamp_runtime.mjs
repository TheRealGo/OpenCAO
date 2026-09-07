import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const source = fs.readFileSync(process.argv[2], "utf8");
const marker = "function localTimestamp(";
const start = source.indexOf(marker);
assert.notEqual(start, -1, "localTimestamp must exist");
const opening = source.indexOf("{", start);
let depth = 0;
let end = -1;
for (let index = opening; index < source.length; index += 1) {
  if (source[index] === "{") depth += 1;
  if (source[index] === "}") depth -= 1;
  if (depth === 0) {
    end = index + 1;
    break;
  }
}
assert.notEqual(end, -1, "localTimestamp must be complete");

const sandbox = { Date, Intl, Number, result: null };
vm.runInNewContext(`${source.slice(start, end)}; result = localTimestamp;`, sandbox);
const localTimestamp = sandbox.result;
assert.equal(typeof localTimestamp, "function");

const options = {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  timeZoneName: "short",
};
const formatter = new Intl.DateTimeFormat(undefined, options);
const part = (value, type) => formatter.formatToParts(new Date(value))
  .find((item) => item.type === type)?.value;

assert.equal(localTimestamp("/Users/owner/private/token"), null);
assert.equal(localTimestamp("2026-08-12T00:00:00Z"), formatter.format(
  new Date("2026-08-12T00:00:00Z"),
));

if (process.argv[3] === "tokyo") {
  assert.equal(formatter.resolvedOptions().timeZone, "Asia/Tokyo");
  assert.equal(part("2026-08-12T00:00:00Z", "year"), "2026");
  assert.equal(part("2026-08-12T00:00:00Z", "month"), "08");
  assert.equal(part("2026-08-12T00:00:00Z", "day"), "12");
  assert.equal(part("2026-08-12T00:00:00Z", "hour"), "09");
  assert.ok(part("2026-08-12T00:00:00Z", "timeZoneName"));
}

if (process.argv[3] === "new-york") {
  assert.equal(formatter.resolvedOptions().timeZone, "America/New_York");
  assert.equal(part("2026-01-15T12:00:00Z", "hour"), "07");
  assert.equal(part("2026-07-15T12:00:00Z", "hour"), "08");
  assert.notEqual(
    part("2026-01-15T12:00:00Z", "timeZoneName"),
    part("2026-07-15T12:00:00Z", "timeZoneName"),
  );
}
