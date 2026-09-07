import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

class Element {
  constructor(tag = "div") {
    this.tagName = tag;
    this.children = [];
    this.attributes = new Map();
    this.textContent = "";
    this.hidden = false;
    this.scrollTop = 0;
    this.scrollHeight = 100;
    this.value = "";
  }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  setAttribute(key, value) { this.attributes.set(key, value); }
  getAttribute(key) { return this.attributes.get(key); }
  set innerHTML(_) { throw new Error("Untrusted text must never become HTML"); }
}
const elements = new Map();
const document = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, new Element());
    return elements.get(id);
  },
  createElement(tag) { return new Element(tag); },
};
const requests = [];
const fetch = async (url, options) => new Promise((resolve, reject) => {
  requests.push({ url, resolve, signal: options.signal });
  options.signal.addEventListener("abort", () => reject(new Error("Request deadline")));
});
let now = 0;
let timerId = 0;
const timers = new Map();
const clock = {
  setTimeout(callback, delay) { timers.set(++timerId, { callback, due: now + delay }); return timerId; },
  clearTimeout(id) { timers.delete(id); },
};
function advance(milliseconds) {
  now += milliseconds;
  for (const [id, timer] of timers) {
    if (timer.due <= now) { timers.delete(id); timer.callback(); }
  }
}
const source = fs.readFileSync(process.argv[2], "utf8");
const sandbox = { document, fetch, AbortController, URLSearchParams, Intl,
  window: clock, testAPI: null };
vm.runInNewContext(source.slice(0, source.lastIndexOf("  (async () => {")) +
  "globalThis.testAPI = { state, appendWork, renderSnapshot, selectHistoryWork, loadWorkIndex, renderWorkIndex, fetchWithDeadline }; })();", sandbox);
const api = sandbox.testAPI;
const text = (node, visible = false) => node.hidden && visible ? "" : [node.textContent,
  ...(visible && node.tagName === "details" && !node.open ? node.children.slice(0, 1) : node.children)
    .map((child) => text(child, visible))].join("\n");
const find = (node, test) => test(node) ? node : node.children.map((child) => find(child, test)).find(Boolean);
const full = "Readable paragraph.\n".repeat(80) + "THE FINAL SENTENCE";
const work = {
  history_reference: "a".repeat(64), work_title: "A work", objective_text: full,
  latest_report_text: full, latest_report_summary: "truncated…", latest_report_kind: "progress",
  attention_owner: "worker", status_request_state: "responded", closure_summary: {},
  completed_at: "2026-09-01T00:00:00Z", state: "waiting_user", attempt_state: "completed",
  latest_cao_review_decision: "ok", requester_decision: "pending",
};
const container = new Element();
api.appendWork(container, work, { completed: true });
assert.match(text(container), /THE FINAL SENTENCE/);
assert.doesNotMatch(text(container, true), /停滞検出|Runtime生存信号|依頼者の受理記録|復旧待ち開始/);
assert.match(text(container, true), /CAOレビュー済み/);
assert.doesNotMatch(text(container, true), /あなたの回答・判断が必要/);
const disclosure = find(container, (node) => node.getAttribute("data-reading-key") === `${work.history_reference}:report`);
disclosure.open = true;
disclosure.ontoggle();
const replacement = new Element();
api.appendWork(replacement, work);
assert.equal(find(replacement, (node) => node.getAttribute("data-reading-key") === `${work.history_reference}:report`).open, true);

const respond = (request, body, ok = true) => request.resolve({ ok, async json() { return body; } });
const body = (item, entries = []) => ({ work: item, entries, has_more: false });
const first = api.selectHistoryWork(work.history_reference);
const secondWork = { ...work, history_reference: "b".repeat(64), work_title: "B work" };
const second = api.selectHistoryWork(secondWork.history_reference);
respond(requests[1], body(secondWork));
await second;
respond(requests[0], body(work));
await first;
assert.match(text(elements.get("reader-content")), /B work/);
assert.doesNotMatch(text(elements.get("reader-content")), /A work/);
assert.equal(api.state.readerSelection, secondWork.history_reference);

api.state.readerLoaded = true;
const readingNode = elements.get("reader-content").children[0];
elements.get("work-reader").scrollTop = 240;
api.renderSnapshot({ cursor: "new-cursor", snapshot_digest: "c".repeat(64), operator: {} });
assert.equal(elements.get("reader-content").children[0], readingNode);
assert.equal(elements.get("work-reader").scrollTop, 240);

const entry = { reference: "d".repeat(64), kind: "progress", text: "Earlier report", occurred_at: "2026-09-01T00:00:00Z" };
api.state.readerEntries = [{ ...entry, reference: "e".repeat(64), text: "Latest report" }];
api.state.readerBefore = "e".repeat(64);
const older = api.selectHistoryWork(secondWork.history_reference, true);
respond(requests[2], body(secondWork, [entry]));
await older;
assert.deepEqual(Array.from(api.state.readerEntries, (entry) => entry.text), ["Earlier report", "Latest report"]);

const failure = api.selectHistoryWork("f".repeat(64));
assert.equal(elements.get("reader-content").children.length, 0, "a different selection cannot retain the preceding work's content");
respond(requests[3], {}, false);
await failure;
assert.match(elements.get("reader-status").textContent, /読み込めません/);
const slowRead = api.selectHistoryWork(work.history_reference);
advance(12000);
assert.equal(requests[4].signal.aborted, false, "retained history can finish beyond the live-update deadline");
respond(requests[4], body(work, [entry]));
await slowRead;
assert.match(text(elements.get("reader-content")), /Earlier report/);
const stalledRead = api.selectHistoryWork(work.history_reference);
advance(30001);
await stalledRead;
assert.equal(requests[5].signal.aborted, true, "history still has a bounded failure path");
assert.match(elements.get("reader-status").textContent, /読み込めません/);
const liveRead = api.fetchWithDeadline("/dashboard/api/snapshot").catch(() => {});
advance(10001);
await liveRead;
assert.equal(requests[6].signal.aborted, true, "live update deadlines remain unchanged");
console.log("Full reading, contextual status, retained disclosure/scroll, pagination, selection races and failures passed.");
