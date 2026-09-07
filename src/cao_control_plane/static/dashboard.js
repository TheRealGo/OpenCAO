(() => {
  "use strict";

  const state = {
    cursor: "",
    source: null,
    history: [],
    updateQueue: Promise.resolve(),
    lastEventId: "",
    snapshotDigest: "",
    pendingRevision: 0,
    completedRevision: 0,
    retryTimer: null,
    retryDelayMs: 250,
    reconnectAfterRefresh: false,
    expanded: new Set(),
    readerItems: [],
    readerEntries: [],
    readerSelection: "",
    readerBefore: "",
    readerNext: "",
    readerLoaded: false,
    readerCursor: "",
    readerRequest: 0,
    readerLoading: false,
  };
  const REQUEST_DEADLINE_MS = 10000;
  const HISTORY_REQUEST_DEADLINE_MS = 30000;
  const byId = (id) => document.getElementById(id);
  const text = (value, fallback = "未取得") => (
    typeof value === "string" || typeof value === "number" ? String(value) : fallback
  );
  const setConnection = (message, warning = false) => {
    const node = byId("connection");
    node.textContent = message;
    node.className = warning ? "warning" : "";
  };
  const show = (id) => { byId(id).hidden = false; };
  const hide = (id) => { byId(id).hidden = true; };

  async function fetchWithDeadline(resource, options, deadlineMs = REQUEST_DEADLINE_MS) {
    const controller = new AbortController();
    const deadline = window.setTimeout(() => controller.abort(), deadlineMs);
    try {
      return await fetch(resource, { ...(options || {}), signal: controller.signal });
    } finally {
      window.clearTimeout(deadline);
    }
  }

  function localTimestamp(value) {
    if (typeof value !== "string" || !value) return null;
    const instant = new Date(value);
    if (Number.isNaN(instant.getTime())) return null;
    try {
      return new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        timeZoneName: "short",
      }).format(instant);
    } catch (_) {
      return null;
    }
  }

  function appendDefinition(container, label, value) {
    const term = document.createElement("dt");
    const description = document.createElement("dd");
    term.textContent = label;
    description.textContent = text(value);
    container.append(term, description);
  }

  const attentionReasonLabel = (value) => ({
    "user-action-required": "依頼者の対応が必要",
    "cao-action-required": "CAOの対応が必要",
    "cao-processing": "CAOが自動処理中",
    "external-action-required": "外部対応待ち",
    "progress-at-risk": "進捗に注意が必要",
    "awaiting-explicit-close": "完了確認・終了処理待ち",
    "system-reconciliation": "システム整合化待ち",
    "runner-failed": "Workerの実行が失敗",
    "runner-missing": "Workerの実行環境が見つからない",
    "runner-stopped": "Workerが停止中",
  }[value] || null);

  const reportKindLabel = (value) => ({
    progress: "進捗報告",
    question: "質問",
    blocker: "停止要因",
    artifact: "成果物報告",
    completion_claim: "完了申告",
    worker_output: "回答を自動回収（未検証）",
  }[value] || null);

  const statusRequestLabel = (value) => ({
    pending: "回答待ち",
    overdue: "回答期限超過",
    responded: "回答済み",
  }[value] || null);

  const completionContractLabel = (value) => ({
    completion_required: "成果物の引き渡しが必要",
    no_artifact_expected: "成果物なしで完了可能",
    legacy_unclassified: "旧形式（引き渡し条件未分類）",
  }[value] || null);

  const deliveryStateLabel = (value) => ({
    pending: "完了申告待ち",
    not_required: "成果物の引き渡し不要",
    legacy_unclassified: "旧形式（引き渡し状態未分類）",
    ready: "成果物を確認可能",
    delivery_missing: "成果物が未引き渡し",
  }[value] || null);

  const recoveryActionLabel = (value) => ({
    dispose_continue_or_correct: "同じWorkerで再開可能",
    reconcile_continue_same_thread: "同じWorkerで安全に照合・継続可能",
    system_reconciliation: "システム整合化が必要",
  }[value] || null);

  const recoveryNotificationLabel = (value) => ({
    queued: "未確認",
    leased: "確認処理中",
    dispatched: "確認結果が未確定",
    delivered: "受信済み",
    acknowledged: "確認済み",
    handled: "処理済み",
    dead: "通知配信終了",
  }[value] || null);

  const caoSupervisionLabel = (value) => ({
    scheduled: "CAO自動処理を予約済み",
    active: "CAO処理中",
    unscheduled: "自動処理が未起動（異常）",
  }[value] || null);

  function reportMeta(item) {
    const values = [reportKindLabel(item.latest_report_kind), localTimestamp(item.latest_reported_at)]
      .filter((value) => typeof value === "string" && value);
    return values.length ? values.join(" · ") : null;
  }

  function disclosure(key, label, className = "") {
    const details = document.createElement("details");
    details.className = className;
    details.setAttribute("data-reading-key", key);
    details.open = state.expanded.has(key);
    details.ontoggle = () => {
      if (details.open) state.expanded.add(key);
      else state.expanded.delete(key);
    };
    const summary = document.createElement("summary");
    summary.textContent = label;
    details.append(summary);
    return details;
  }

  function appendReadable(container, label, value, key, meta = null) {
    if (typeof value !== "string" || !value.trim()) return;
    const body = document.createElement("div");
    body.className = "reading-text";
    body.textContent = value;
    const block = document.createElement("section");
    block.className = "reading-block";
    if (value.length > 500 || value.split("\n").length > 8) {
      const details = disclosure(key, label, "reading-disclosure");
      const summary = details.children[0];
      const preview = document.createElement("span");
      preview.className = "closed-preview";
      preview.textContent = value.slice(0, 180) + "…";
      const expand = document.createElement("span");
      expand.className = "expand-label";
      expand.textContent = `全文を表示 · ${value.length.toLocaleString()}文字`;
      const collapse = document.createElement("span");
      collapse.className = "collapse-label";
      collapse.textContent = "折りたたむ";
      summary.append(preview, expand, collapse);
      details.append(body);
      block.append(details);
    } else {
      const heading = document.createElement("p");
      heading.className = "reading-label";
      heading.textContent = label;
      block.append(heading, body);
    }
    if (meta) {
      const information = document.createElement("p");
      information.className = "reading-meta";
      information.textContent = meta;
      block.append(information);
    }
    container.append(block);
  }

  function appendKnown(container, label, value) {
    if (value === null || value === undefined || value === "") return;
    appendDefinition(container, label, value);
  }

  function actionLabel(item) {
    if (item.state === "suspended") return "中断中";
    if (item.state === "canceled") return "終了済み · 記録を保持";
    if (item.state === "failed") return "失敗した作業の記録";
    if (item.cao_supervision_state === "active") return "CAOが確認・判断中";
    if (item.cao_supervision_state === "scheduled") return "CAOの自動処理待ち";
    return { user: "あなたの回答・判断が必要", cao: "CAOの確認・判断が必要",
      external: "外部の対応待ち", worker: "Workerが作業中" }[item.attention_owner] || null;
  }

  function appendWork(container, item, { completed = false, historyLink = true } = {}) {
    const work = document.createElement("section");
    const heading = document.createElement("h5");
    const key = item.history_reference || item.display_label || item.work_title;
    work.className = "work-summary";
    heading.textContent = text(item.work_title, text(item.display_label, "現在の作業"));
    work.append(heading);
    const status = document.createElement("p");
    status.className = completed ? "work-status complete" : "work-status";
    status.textContent = completed
      ? ["CAOレビュー済み", localTimestamp(item.completed_at)].filter(Boolean).join(" · ")
      : (actionLabel(item) || "作業の記録");
    work.append(status);
    appendReadable(work, "目的", item.objective_text || item.objective_summary, `${key}:objective`);
    appendReadable(work, "最新のWorker報告", item.latest_report_text || item.latest_report_summary,
      `${key}:report`, reportMeta(item));
    if (!item.latest_report_text && !item.latest_report_summary && item.latest_reported_at) {
      const meta = document.createElement("p");
      meta.className = "reading-meta";
      meta.textContent = reportMeta(item);
      work.append(meta);
    }
    const current = document.createElement("dl");
    current.className = "work-facts";
    if (!completed) {
      if (item.progress_stage && !["completed", "working", "active"].includes(item.progress_stage)) {
        appendKnown(current, "進捗", item.progress_stage);
      }
      appendKnown(current, "次の予定", item.next_boundary_summary);
      if (item.supervision_pause) {
        appendKnown(current, "中断の理由", item.supervision_pause.reason);
        appendKnown(current, "再開の条件", item.supervision_pause.resume_condition);
      }
    }
    if (["ready", "delivery_missing"].includes(item.delivery_state)) {
      appendKnown(current, "成果物", deliveryStateLabel(item.delivery_state));
    }
    if (current.children.length) work.append(current);
    // Recovery is an observed exceptional state, never a standing suggestion.
    if (!completed && item.recovery_action) {
      const recovery = document.createElement("p");
      recovery.className = "recovery-notice";
      recovery.textContent = recoveryActionLabel(item.recovery_action) || "";
      work.append(recovery);
    }
    const technical = disclosure(`${key}:technical`, "記録・診断の詳細", "worker-technical");
    const facts = document.createElement("dl");
    [
      ["作業状態", item.state], ["実行状態", item.attempt_state],
      ["進行評価", item.trajectory], ["CAOレビュー", item.latest_cao_review_decision],
      ["依頼者の受理記録", item.requester_decision], ["終了処理", item.closure_state],
      ["成果物の条件", completionContractLabel(item.completion_contract)],
      ["最終Worker活動", localTimestamp(item.last_worker_activity_at)],
      ["Runtime生存信号", localTimestamp(item.runtime_heartbeat_at)],
      ["成果物更新", localTimestamp(item.last_artifact_at)],
    ].forEach(([label, value]) => appendKnown(facts, label, value));
    if (!completed && item.status_request_state) {
      appendKnown(facts, "システムの状況照会", statusRequestLabel(item.status_request_state));
      appendKnown(facts, "停滞検出の基準時刻", localTimestamp(item.status_response_due_at));
      appendKnown(facts, "状況照会への回答", localTimestamp(item.status_responded_at));
    }
    if (!completed && item.recovery_action) {
      appendKnown(facts, "復旧待ち開始", localTimestamp(item.recovery_waiting_since));
      appendKnown(facts, "復旧通知の配送", recoveryNotificationLabel(item.recovery_notification_state));
    }
    if (!completed && item.cao_supervision_state) {
      appendKnown(facts, "CAO自動処理", caoSupervisionLabel(item.cao_supervision_state));
    }
    const closure = item.closure_summary || {};
    if (closure.unresolved_deliveries > 0) appendKnown(facts, "未解決の配送", closure.unresolved_deliveries);
    if (closure.unresolved_effects > 0) appendKnown(facts, "未解決の外部効果", closure.unresolved_effects);
    technical.append(facts);
    work.append(technical);
    if (historyLink && item.history_reference) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "text-button";
      button.textContent = "指示・報告・レビューの履歴";
      button.onclick = () => {
        show("recently-completed-section");
        selectHistoryWork(item.history_reference);
        byId("work-reader").scrollIntoView({ block: "start", behavior: "smooth" });
      };
      work.append(button);
    }
    container.append(work);
  }

  function workerCard(item) {
    const row = document.createElement("li");
    const heading = document.createElement("h4");
    row.className = "worker-card";
    heading.textContent = text(item.worker_label, "Worker");
    row.append(heading);
    const attentionText = attentionReasonLabel(item.attention_reason);
    if (attentionText) {
      const attention = document.createElement("p");
      attention.className = "attention-reason";
      attention.textContent = attentionText;
      row.append(attention);
    }
    const workItems = Array.isArray(item.current_work_items) ? item.current_work_items : [];
    workItems.forEach((work) => appendWork(row, work));
    const technical = disclosure(`worker:${item.worker_label}`, "接続・モデル詳細", "worker-technical");
    const facts = document.createElement("dl");
    [
      ["接続状態", item.runner_connection_state], ["Runner", item.runner_adapter],
      ["モデル", item.runner_effective_model || item.runner_model],
      ["推論Effort", item.runner_effective_reasoning_effort || item.runner_reasoning_effort],
    ].forEach(([label, value]) => appendKnown(facts, label, value));
    if (item.runner_requested_model !== item.runner_effective_model) {
      appendKnown(facts, "要求モデル", item.runner_requested_model);
    }
    if (item.runner_requested_reasoning_effort !== item.runner_effective_reasoning_effort) {
      appendKnown(facts, "要求Effort", item.runner_requested_reasoning_effort);
    }
    technical.append(facts);
    row.append(technical);
    return row;
  }

  function renderWorkerGroup(operator, key, sectionId, listId) {
    const workers = Array.isArray(operator[key]) ? operator[key] : [];
    const list = byId(listId);
    list.replaceChildren(...workers.map(workerCard));
    if (workers.length) show(sectionId);
    else hide(sectionId);
    return workers.length;
  }

  async function bootstrapFromFragment() {
    const fragment = new URLSearchParams(window.location.hash.slice(1));
    const secret = fragment.get("dashboard_bootstrap");
    if (!secret) return false;
    window.history.replaceState(null, "", window.location.pathname);
    const response = await fetchWithDeadline("/dashboard/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ secret }),
    });
    if (!response.ok) {
      setConnection("一回限りのアクセスリンクを確認できませんでした。", true);
      return false;
    }
    return true;
  }

  function renderSnapshot(body) {
    const focused = document.activeElement && document.activeElement.closest
      ? document.activeElement.closest("details[data-reading-key]") : null;
    const focusedKey = focused ? focused.getAttribute("data-reading-key") : null;
    if (document.querySelectorAll) {
      for (const details of document.querySelectorAll("details[data-reading-key]")) {
        const key = details.getAttribute("data-reading-key");
        if (details.open) state.expanded.add(key);
        else state.expanded.delete(key);
      }
    }
    const authority = body && body.authority || {};
    const operator = body && body.operator || {};
    const delivery = operator.runtime_delivery || {};
    state.cursor = text(body && body.cursor, "");
    state.snapshotDigest = text(body && body.snapshot_digest, "");

    const caoProcessing = renderWorkerGroup(
      operator, "cao_processing", "cao-processing-section", "cao-processing"
    );
    const userConfirmation = renderWorkerGroup(
      operator, "user_confirmation", "user-confirmation-section", "user-confirmation"
    );
    const stoppedOrFailed = renderWorkerGroup(
      operator, "stopped_or_failed", "stopped-or-failed-section", "stopped-or-failed"
    );
    const working = renderWorkerGroup(operator, "working", "working-section", "working");
    const ready = renderWorkerGroup(operator, "ready", "ready-section", "ready");
    show("current-workers");
    if (caoProcessing + userConfirmation + stoppedOrFailed + working + ready === 0) {
      show("empty-current");
    }
    else hide("empty-current");

    show("recently-completed-section");
    if (!state.readerLoaded) {
      state.readerItems = Array.isArray(operator.recently_completed) ? operator.recently_completed : [];
      renderWorkIndex();
    }
    if (state.readerLoaded && state.readerCursor !== state.cursor) show("history-refresh");

    const inactive = Array.isArray(operator.inactive_workers) ? operator.inactive_workers : [];
    byId("inactive-workers").replaceChildren(...inactive.map(workerCard));
    if (inactive.length) show("inactive-section");
    else hide("inactive-section");

    const runtime = byId("runtime-summary");
    runtime.replaceChildren();
    appendDefinition(runtime, "Authority", authority.mode);
    appendDefinition(runtime, "Generation", authority.generation);
    appendDefinition(runtime, "Runtime sessions", delivery.runtime_count || 0);
    appendDefinition(runtime, "Queued deliveries", delivery.queued_deliveries || 0);
    appendDefinition(runtime, "Unknown delivery outcomes", delivery.unknown_delivery_outcomes || 0);
    appendDefinition(runtime, "Dead deliveries", delivery.dead_deliveries || 0);
    appendDefinition(
      runtime,
      "Effects",
      Object.entries(delivery.effects_by_state || {})
        .map(([key, value]) => `${key}: ${value}`)
        .join(", ") || "none"
    );
    show("runtime-section");
    if (focusedKey && document.querySelectorAll) {
      for (const details of document.querySelectorAll("details[data-reading-key]")) {
        if (details.getAttribute("data-reading-key") === focusedKey) {
          details.children[0].focus({ preventScroll: true });
          break;
        }
      }
    }
  }

  const exchangeLabel = (kind) => ({
    goal: "CAO · 目的", instruction: "CAO · 指示", review: "CAO · レビュー",
    requester_decision: "依頼者 · 判断", decision: "CAO · 判断",
    progress: "Worker · 進捗報告", question: "Worker · 質問", blocker: "Worker · 停止要因",
    artifact: "Worker · 成果物報告", completion_claim: "Worker · 完了申告",
    worker_output: "Worker · 自動回収した回答（未検証）",
  }[kind] || "記録");
  const outcomeLabel = (value) => ({
    ok: "確認済み", "needs-work": "修正が必要", accepted: "受理", rejected: "差し戻し",
    accept: "完了を確認", continue: "継続", correct: "修正して継続", pause: "中断",
    wait_user: "依頼者に確認", escalate: "エスカレーション", close: "終了", cancel: "取り消し",
  }[value] || null);

  function renderWorkIndex() {
    const focusedReference = document.activeElement && document.activeElement.getAttribute
      ? document.activeElement.getAttribute("data-history-reference") : null;
    const query = (byId("history-search").value || "").trim().toLocaleLowerCase();
    const items = state.readerItems.filter((item) =>
      `${item.work_title || ""} ${item.worker_label || ""}`.toLocaleLowerCase().includes(query));
    byId("recently-completed").replaceChildren(...items.map((item) => {
      const row = document.createElement("li");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "history-choice";
      button.setAttribute("aria-pressed", String(item.history_reference === state.readerSelection));
      button.setAttribute("data-history-reference", item.history_reference || "");
      const title = document.createElement("span");
      title.className = "history-choice-title";
      title.textContent = text(item.work_title, "作業の記録");
      const meta = document.createElement("span");
      meta.className = "reading-meta";
      meta.textContent = [item.worker_label, item.completed_at ? "CAOレビュー済み" : "作業の記録",
        localTimestamp(item.completed_at)].filter(Boolean).join(" · ");
      button.append(title, meta);
      button.onclick = () => selectHistoryWork(item.history_reference);
      row.append(button);
      return row;
    }));
    byId("history-count").textContent = `${items.length}件を表示`;
    byId("history-empty").hidden = items.length > 0;
    byId("history-more").hidden = !state.readerNext;
    if (focusedReference && document.querySelectorAll) {
      for (const button of document.querySelectorAll("[data-history-reference]")) {
        if (button.getAttribute("data-history-reference") === focusedReference) button.focus({ preventScroll: true });
      }
    }
  }

  async function historyRequest(params) {
    const response = await fetchWithDeadline(`/dashboard/api/work-history?${new URLSearchParams(params)}`,
      { credentials: "same-origin" }, HISTORY_REQUEST_DEADLINE_MS);
    if (!response.ok) throw new Error("history unavailable");
    return response.json();
  }

  async function loadWorkIndex(more = false) {
    if (state.readerLoading) return;
    state.readerLoading = true;
    const cursor = state.cursor;
    byId("history-status").textContent = "履歴を読み込み中…";
    byId("history-more").disabled = true;
    byId("history-refresh").disabled = true;
    try {
      const body = await historyRequest({ limit: "20", ...(more ? { before: state.readerNext } : {}) });
      const items = Array.isArray(body.items) ? body.items : [];
      state.readerItems = more
        ? [...state.readerItems, ...items.filter((item) => !state.readerItems.some(
          (old) => old.history_reference === item.history_reference))] : items;
      state.readerNext = body.has_more ? body.next_before : "";
      state.readerLoaded = true;
      state.readerCursor = cursor;
      byId("history-status").textContent = "";
      hide("history-refresh");
      renderWorkIndex();
      if (!state.readerSelection && state.readerItems.length) {
        await selectHistoryWork(state.readerItems[0].history_reference);
      }
    } catch (_) {
      byId("history-status").textContent = "履歴を読み込めませんでした。もう一度お試しください。";
      show("history-refresh");
    } finally {
      state.readerLoading = false;
      byId("history-more").disabled = false;
      byId("history-refresh").disabled = false;
    }
  }

  async function selectHistoryWork(reference, older = false) {
    if (!reference) return;
    const request = ++state.readerRequest;
    const previousSelection = state.readerSelection;
    state.readerSelection = reference;
    renderWorkIndex();
    const reader = byId("work-reader");
    const previousTop = reader.scrollTop;
    const previousHeight = reader.scrollHeight;
    if (!older && previousSelection !== reference) {
      byId("reader-content").replaceChildren();
      state.readerEntries = [];
      state.readerBefore = "";
      hide("reader-older");
    }
    byId("reader-status").textContent = "読み込み中…";
    byId("reader-older").disabled = true;
    try {
      const body = await historyRequest({ work: reference, limit: "30",
        ...(older ? { before: state.readerBefore } : {}) });
      if (request !== state.readerRequest) return;
      state.readerEntries = older
        ? [...body.entries, ...state.readerEntries.filter((entry) => !body.entries.some(
          (old) => old.reference === entry.reference))] : body.entries;
      state.readerBefore = body.has_more ? body.next_before : "";
      const content = byId("reader-content");
      content.replaceChildren();
      appendWork(content, body.work, { completed: Boolean(body.work.completed_at), historyLink: false });
      const timeline = document.createElement("ol");
      timeline.className = "exchange-timeline";
      state.readerEntries.forEach((entry) => {
        const row = document.createElement("li");
        row.className = `exchange exchange-${entry.kind}`;
        appendReadable(row, exchangeLabel(entry.kind), entry.text, `entry:${entry.reference}`,
          [outcomeLabel(entry.outcome), localTimestamp(entry.occurred_at)].filter(Boolean).join(" · "));
        timeline.append(row);
      });
      if (state.readerEntries.length) {
        const heading = document.createElement("h3");
        heading.className = "timeline-title";
        heading.textContent = "やり取りの記録";
        content.append(heading, timeline);
      }
      byId("reader-status").textContent = `${state.readerEntries.length}件の記録 · 古い順`;
      byId("reader-older").hidden = !state.readerBefore;
      reader.scrollTop = older ? previousTop + reader.scrollHeight - previousHeight
        : (previousSelection === reference ? previousTop : 0);
    } catch (_) {
      if (request === state.readerRequest) {
        byId("reader-status").textContent = "記録を読み込めませんでした。作業を選び直して再試行できます。";
      }
    } finally {
      if (request === state.readerRequest) byId("reader-older").disabled = false;
    }
  }

  function setupReader() {
    byId("history-search").oninput = renderWorkIndex;
    byId("history-more").onclick = () => loadWorkIndex(true);
    byId("history-refresh").onclick = () => {
      loadWorkIndex();
      if (state.readerSelection) selectHistoryWork(state.readerSelection);
    };
    byId("reader-older").onclick = () => selectHistoryWork(state.readerSelection, true);
  }

  function renderHistory() {
    const list = byId("history");
    list.replaceChildren();
    state.history.slice(-20).reverse().forEach((event) => {
      const row = document.createElement("li");
      row.textContent = `${text(event.type)} · ${text(localTimestamp(event.occurred_at))}`;
      list.append(row);
    });
    if (state.history.length) show("history-section");
    else hide("history-section");
  }

  async function loadHistory() {
    const response = await fetchWithDeadline("/dashboard/api/history?limit=20", {
      credentials: "same-origin",
    });
    if (!response.ok) return;
    const body = await response.json();
    state.history = Array.isArray(body.items)
      ? body.items
        .map((item) => item.event || {})
        .filter((event) => event && typeof event === "object")
      : [];
    renderHistory();
  }

  async function reportRendered() {
    if (!state.cursor || !/^[0-9a-f]{64}$/.test(state.snapshotDigest)) return false;
    const response = await fetchWithDeadline("/dashboard/session/rendered", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ cursor: state.cursor, snapshot_digest: state.snapshotDigest }),
    });
    if (response.status === 401) return false;
    if (!response.ok) throw new Error("render observation unavailable");
    return true;
  }

  async function refresh(resync = false) {
    const response = await fetchWithDeadline(
      "/dashboard/api/snapshot",
      { credentials: "same-origin" },
    );
    if (response.status === 401) {
      show("bootstrap");
      setConnection("ダッシュボードへのアクセスが必要です。", true);
      byId("dashboard").setAttribute("aria-busy", "false");
      return false;
    }
    if (!response.ok) throw new Error("snapshot unavailable");
    renderSnapshot(await response.json());
    if (!state.history.length) await loadHistory();
    if (!state.readerLoaded && !state.readerLoading) {
      setupReader();
      void loadWorkIndex();
    }
    if (!await reportRendered()) return false;
    setConnection(resync ? "再同期しました。接続中…" : "接続済み");
    byId("dashboard").setAttribute("aria-busy", "false");
    return true;
  }

  function clearRetry() {
    if (state.retryTimer !== null) window.clearTimeout(state.retryTimer);
    state.retryTimer = null;
    state.retryDelayMs = 250;
  }

  function scheduleRetry() {
    if (state.retryTimer !== null || state.completedRevision >= state.pendingRevision) return;
    const delay = state.retryDelayMs;
    state.retryDelayMs = Math.min(state.retryDelayMs * 2, 5000);
    state.retryTimer = window.setTimeout(() => {
      state.retryTimer = null;
      enqueueRefreshAttempt(state.pendingRevision);
    }, delay);
  }

  function enqueueRefreshAttempt(revision) {
    state.updateQueue = state.updateQueue
      .then(async () => {
        if (revision <= state.completedRevision) return;
        const shouldReconnect = state.reconnectAfterRefresh;
        if (!await refresh(shouldReconnect)) {
          state.completedRevision = Math.max(state.completedRevision, revision);
          state.reconnectAfterRefresh = false;
          clearRetry();
          return;
        }
        state.completedRevision = Math.max(state.completedRevision, revision);
        if (state.completedRevision < state.pendingRevision) {
          return;
        }
        clearRetry();
        if (state.reconnectAfterRefresh) {
          state.reconnectAfterRefresh = false;
          connect();
        }
      })
      .catch(() => {
        setConnection("更新を表示できませんでした。同じ更新を再取得しています。", true);
        scheduleRetry();
      });
  }

  function requestRefresh({ reconnect = false } = {}) {
    state.pendingRevision += 1;
    state.reconnectAfterRefresh = state.reconnectAfterRefresh || reconnect;
    enqueueRefreshAttempt(state.pendingRevision);
  }

  function enqueueUpdate(event) {
    const eventId = event.lastEventId;
    if (!eventId || eventId === state.lastEventId) return;

    let body;
    try {
      body = JSON.parse(event.data);
    } catch (_) {
      state.lastEventId = eventId;
      setConnection("更新データを確認できませんでした。最新状態を再取得しています。", true);
      requestRefresh();
      return;
    }

    state.lastEventId = eventId;
    state.history.push(body.event || {});
    if (state.history.length > 20) state.history.splice(0, state.history.length - 20);
    renderHistory();
  }

  function enqueueSync(event) {
    const eventId = event.lastEventId;
    if (!eventId) {
      setConnection("同期位置を確認できませんでした。最新状態を再取得しています。", true);
      requestRefresh();
      return;
    }
    state.lastEventId = eventId;
    if (eventId !== state.cursor) {
      requestRefresh();
      return;
    }
    reportRendered()
      .then((rendered) => {
        if (rendered) setConnection("接続済み");
        else requestRefresh();
      })
      .catch(() => requestRefresh());
  }

  function enqueueResync(source) {
    source.close();
    if (state.source === source) state.source = null;
    requestRefresh({ reconnect: true });
  }

  function connect() {
    if (state.source) state.source.close();
    const query = state.cursor ? `?after=${encodeURIComponent(state.cursor)}` : "";
    const source = new EventSource(`/dashboard/api/stream${query}`);
    state.source = source;
    source.addEventListener("dashboard-update", enqueueUpdate);
    source.addEventListener("dashboard-synced", enqueueSync);
    source.addEventListener("resync-required", () => enqueueResync(source));
    source.onopen = () => {
      setConnection("同期を確認しています…");
    };
    source.onerror = () => {
      if (state.source !== source) return;
      setConnection("接続が中断しました。最新状態を再取得しています。", true);
      enqueueResync(source);
    };
  }

  (async () => {
    try {
      await bootstrapFromFragment();
      requestRefresh({ reconnect: true });
    } catch (_) {
      byId("dashboard").setAttribute("aria-busy", "false");
      setConnection("ダッシュボードを一時的に利用できません。", true);
    }
  })();
})();
