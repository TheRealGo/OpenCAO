import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const source = fs.readFileSync(process.argv[2], "utf8");

let snapshotCursor = "cursor-1";
let snapshotFetches = 0;
let historyFetches = 0;
let activeSnapshotFetches = 0;
let maximumConcurrentSnapshotFetches = 0;
let snapshotRenders = 0;
let renderReports = 0;
let snapshotFailuresRemaining = 0;
let snapshotHangsRemaining = 0;
let snapshotAborts = 0;
const reportTimestamp = "2026-08-12T00:00:00Z";
const internalRetryTimestamp = "2099-12-31T23:59:59Z";

class Element {
  constructor(id = "") {
    this.id = id;
    this.hidden = false;
    this.textContent = "";
    this.className = "";
    this.children = [];
    this.attributes = new Map();
  }

  append(...children) {
    this.children.push(...children);
  }

  replaceChildren(...children) {
    this.children = children;
    if (this.id === "working") snapshotRenders += 1;
  }

  setAttribute(name, value) {
    this.attributes.set(name, value);
  }
}

const elements = new Map();
const document = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, new Element(id));
    return elements.get(id);
  },
  createElement() {
    return new Element();
  },
};

function snapshot() {
  return {
    format: "cao-dashboard-read-model/v1",
    authority: { mode: "canonical", generation: 1 },
    cursor: snapshotCursor,
    snapshot_digest: "d".repeat(64),
    operator: {
      needs_attention: [],
      cao_processing: [],
      user_confirmation: [],
      stopped_or_failed: [],
      working: [
        {
          worker_label: "Visible Worker",
          current_work_items: [
            {
              work_title: "Visible work",
              latest_reported_at: reportTimestamp,
              provider_condition: "rate_limited",
              provider_retry_after_at: internalRetryTimestamp,
              cooldown_until: internalRetryTimestamp,
              closure_summary: {},
            },
          ],
        },
      ],
      ready: [],
      inactive_workers: [],
      recently_completed: [],
      runtime_delivery: { effects_by_state: {} },
    },
  };
}

const response = (body, status = 200) => ({
  status,
  ok: status >= 200 && status < 300,
  async json() {
    return body;
  },
});

async function fetch(url, options = {}) {
  if (url === "/dashboard/api/snapshot") {
    snapshotFetches += 1;
    activeSnapshotFetches += 1;
    maximumConcurrentSnapshotFetches = Math.max(
      maximumConcurrentSnapshotFetches,
      activeSnapshotFetches,
    );
    if (snapshotHangsRemaining > 0) {
      snapshotHangsRemaining -= 1;
      try {
        await new Promise((resolve, reject) => {
          assert.ok(options.signal, "snapshot requests must have a deadline signal");
          const abort = () => {
            snapshotAborts += 1;
            reject(new Error("snapshot request aborted at its deadline"));
          };
          if (options.signal.aborted) abort();
          else options.signal.addEventListener("abort", abort, { once: true });
        });
      } finally {
        activeSnapshotFetches -= 1;
      }
    }
    await new Promise((resolve) => setImmediate(resolve));
    activeSnapshotFetches -= 1;
    if (snapshotFailuresRemaining > 0) {
      snapshotFailuresRemaining -= 1;
      return response({}, 502);
    }
    return response(snapshot());
  }
  if (url === "/dashboard/api/history?limit=20") {
    historyFetches += 1;
    return response({ items: [] });
  }
  if (url === "/dashboard/session/rendered") {
    renderReports += 1;
    assert.equal(options.method, "POST");
    assert.deepEqual(JSON.parse(options.body), {
      cursor: snapshotCursor,
      snapshot_digest: "d".repeat(64),
    });
    return response({}, 204);
  }
  throw new Error(`unexpected fetch: ${url}`);
}

class MockEventSource {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.closed = false;
    this.listeners = new Map();
    MockEventSource.instances.push(this);
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  emit(type, event = {}) {
    if (type === "open" && typeof this.onopen === "function") this.onopen(event);
    if (type === "error" && typeof this.onerror === "function") this.onerror(event);
    for (const listener of this.listeners.get(type) || []) listener(event);
  }

  close() {
    this.closed = true;
  }
}

const window = {
  location: { hash: "", pathname: "/dashboard/" },
  history: { replaceState() {} },
  setTimeout(callback) {
    return setImmediate(callback);
  },
  clearTimeout(handle) {
    clearImmediate(handle);
  },
};

vm.runInNewContext(source, {
  AbortController,
  Array,
  Date,
  EventSource: MockEventSource,
  Intl,
  JSON,
  Map,
  Math,
  Object,
  Promise,
  Set,
  String,
  URLSearchParams,
  document,
  encodeURIComponent,
  fetch,
  window,
});

async function settle() {
  for (let index = 0; index < 8; index += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

function renderedText(node) {
  return [node.textContent, ...node.children.flatMap((child) => renderedText(child))]
    .filter(Boolean)
    .join("\n");
}

await settle();
assert.equal(snapshotFetches, 1);
assert.equal(historyFetches, 1);
assert.equal(snapshotRenders, 1);
assert.equal(renderReports, 1);
assert.equal(MockEventSource.instances.length, 1);
const initialWorkerText = renderedText(elements.get("working"));
const expectedReportTime = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  timeZoneName: "short",
}).format(new Date(reportTimestamp));
assert.match(initialWorkerText, /Visible Worker/);
assert.match(initialWorkerText, /Visible work/);
assert.ok(initialWorkerText.includes(expectedReportTime));
for (const hiddenValue of [
  "Provider状態",
  "再試行可能時刻",
  "rate_limited",
  internalRetryTimestamp,
]) {
  assert.equal(initialWorkerText.includes(hiddenValue), false, `${hiddenValue} must stay internal`);
}

const originalSource = MockEventSource.instances[0];
originalSource.emit("open");
originalSource.emit("heartbeat", { data: ": heartbeat" });
originalSource.emit("dashboard-synced", { lastEventId: "cursor-1", data: "{}" });
await settle();
assert.equal(snapshotFetches, 1, "idle/open/heartbeat must not fetch a snapshot");
assert.equal(snapshotRenders, 1, "idle/open/heartbeat must not redraw");
assert.equal(renderReports, 2, "the durable sync boundary reasserts the rendered identity");
assert.equal(elements.get("connection").textContent, "接続済み");

snapshotCursor = "cursor-hidden";
originalSource.emit("open");
originalSource.emit("dashboard-synced", { lastEventId: "cursor-hidden", data: "{}" });
await settle();
assert.equal(snapshotFetches, 2, "hidden durable progress after reconnect must converge once");
assert.equal(snapshotRenders, 2, "hidden durable progress after reconnect must redraw once");
assert.equal(MockEventSource.instances.length, 1, "native reconnect must retain one EventSource");

snapshotCursor = "cursor-2";
const firstUpdate = {
  lastEventId: "cursor-2",
  data: JSON.stringify({ event: { type: "work.changed", occurred_at: "now" } }),
};
originalSource.emit("dashboard-update", firstUpdate);
originalSource.emit("dashboard-synced", { lastEventId: "cursor-2", data: "{}" });
await settle();
assert.equal(snapshotFetches, 3, "one durable event batch must fetch one snapshot");
assert.equal(snapshotRenders, 3, "one durable event batch must redraw once");
assert.equal(MockEventSource.instances.length, 1, "normal updates must keep one EventSource");
assert.equal(originalSource.closed, false);

snapshotFailuresRemaining = 1;
snapshotCursor = "cursor-retry";
const retryUpdate = {
  lastEventId: "cursor-retry",
  data: JSON.stringify({ event: { type: "work.changed", occurred_at: "retry" } }),
};
originalSource.emit("dashboard-update", retryUpdate);
originalSource.emit("dashboard-synced", { lastEventId: "cursor-retry", data: "{}" });
await settle();
assert.equal(snapshotFetches, 5, "one failed durable update must retry without a later event");
assert.equal(snapshotRenders, 4, "the failed fetch must render only after convergence");
assert.equal(MockEventSource.instances.length, 1, "retry must keep the existing stream");

originalSource.emit("dashboard-update", retryUpdate);
originalSource.emit("dashboard-synced", { lastEventId: "cursor-retry", data: "{}" });
await settle();
assert.equal(snapshotFetches, 5, "an already applied event id must not refetch");
assert.equal(snapshotRenders, 4, "an already applied event id must not redraw");

snapshotCursor = "cursor-4";
originalSource.emit("dashboard-update", {
  lastEventId: "cursor-3",
  data: JSON.stringify({ event: { type: "attempt.progress", occurred_at: "later" } }),
});
originalSource.emit("dashboard-synced", { lastEventId: "cursor-3", data: "{}" });
originalSource.emit("dashboard-update", {
  lastEventId: "cursor-4",
  data: JSON.stringify({ event: { type: "message.created", occurred_at: "latest" } }),
});
originalSource.emit("dashboard-synced", { lastEventId: "cursor-4", data: "{}" });
await settle();
assert.equal(snapshotFetches, 7);
assert.equal(snapshotRenders, 6);
assert.equal(maximumConcurrentSnapshotFetches, 1, "event snapshots must be serialized");
assert.equal(MockEventSource.instances.length, 1);
assert.equal(originalSource.closed, false);

snapshotCursor = "cursor-5";
originalSource.emit("resync-required", { lastEventId: "cursor-resync" });
await settle();
assert.equal(originalSource.closed, true, "resync must close the obsolete stream");
assert.equal(snapshotFetches, 8, "resync must fetch exactly one fresh snapshot");
assert.equal(snapshotRenders, 7);
assert.equal(MockEventSource.instances.length, 2, "resync must open one replacement stream");
assert.match(MockEventSource.instances[1].url, /after=cursor-5$/);
MockEventSource.instances[1].emit("open");
MockEventSource.instances[1].emit("dashboard-synced", { lastEventId: "cursor-5", data: "{}" });
await settle();
assert.equal(elements.get("connection").textContent, "接続済み");

snapshotCursor = "cursor-6";
snapshotHangsRemaining = 1;
const disconnectedSource = MockEventSource.instances[1];
disconnectedSource.emit("error");
disconnectedSource.emit("error");
await settle();
await settle();
assert.equal(disconnectedSource.closed, true, "transport loss must close the stale stream");
assert.equal(snapshotAborts, 1, "a hung Safari-style fetch must be aborted at its deadline");
assert.equal(snapshotFetches, 10, "transport loss must retry one deadline-aborted snapshot");
assert.equal(snapshotRenders, 8, "transport loss must render the fresh snapshot once");
assert.equal(
  MockEventSource.instances.length,
  3,
  "repeated errors from the stale stream must open one replacement stream",
);
assert.match(MockEventSource.instances[2].url, /after=cursor-6$/);
MockEventSource.instances[2].emit("open");
MockEventSource.instances[2].emit("dashboard-synced", {
  lastEventId: "cursor-6",
  data: "{}",
});
await settle();
assert.equal(elements.get("connection").textContent, "接続済み");
