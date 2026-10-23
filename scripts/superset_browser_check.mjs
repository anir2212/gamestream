// Open a GameStream Superset dashboard in headless Chrome, as a logged-in user, and
// record what the real UI did: the date range each chart query carried, the rows
// Superset returned, how many charts drew something, and any error alerts. Saves a
// screenshot and a JSON summary. Needs Node 22+ (built-in WebSocket) and Chrome.
//
//   node scripts/superset_browser_check.mjs <dashboard-id> <out.png> <out.json> [time-range]
//
// With a time range, it is put in the URL as the dashboard's date-filter value
// (filter id NATIVE_FILTER-gamestream-date-<dashboard-id>, set by superset_dashboards.py),
// exactly as Superset encodes a changed native filter. Without one, the filter's
// saved default applies.
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const [dashId, outPng, outJson, range] = process.argv.slice(2);
if (!outJson) {
  console.error("usage: superset_browser_check.mjs <dashboard-id> <out.png> <out.json> [range]");
  process.exit(2);
}
const BASE = process.env.SUPERSET_BASE || "http://127.0.0.1:18088";
const CHROME = process.env.CHROME ||
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const PORT = 9333;
const filterId = `NATIVE_FILTER-gamestream-date-${dashId}`;
const profile = mkdtempSync(join(tmpdir(), "gamestream-chrome-"));
const chrome = spawn(CHROME, ["--headless=new", `--remote-debugging-port=${PORT}`,
  `--user-data-dir=${profile}`, "--window-size=1600,1300", "--no-first-run",
  "--hide-scrollbars", "about:blank"], { stdio: "ignore" });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function devtools(path, method = "GET") {
  for (let attempt = 0; attempt < 50; attempt++) {
    try { return await (await fetch(`http://127.0.0.1:${PORT}${path}`, { method })).json(); }
    catch { await sleep(200); }
  }
  throw new Error("Chrome did not open its debugging port");
}

let socket, nextId = 1;
const pending = new Map(), listeners = [];
function send(method, params = {}) {
  const id = nextId++;
  socket.send(JSON.stringify({ id, method, params }));
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}

try {
  await devtools("/json/version");
  const target = await devtools("/json/new?about:blank", "PUT");
  socket = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve) => socket.addEventListener("open", resolve));
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id);
      pending.delete(message.id);
      message.error ? reject(new Error(JSON.stringify(message.error))) : resolve(message.result);
    } else listeners.forEach((listener) => listener(message));
  });
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Network.enable");
  await send("Emulation.setDeviceMetricsOverride",
    { width: 1600, height: 1300, deviceScaleFactor: 1, mobile: false });
  const evaluate = async (expression) => (await send("Runtime.evaluate",
    { expression, awaitPromise: true, returnByValue: true })).result.value;
  const open = async (url) => { await send("Page.navigate", { url }); await sleep(2500); };

  await open(`${BASE}/login/`);
  await evaluate(`document.querySelector('#username').value = 'admin';
    document.querySelector('#password').value = 'local-demo-only';
    document.querySelector('form').submit(); true`);
  await sleep(3000);

  // Pair each chart-data request body with its response.
  const requests = new Map(), charts = [];
  let lastActivity = Date.now();
  listeners.push(async (message) => {
    const { method, params } = message;
    if (method === "Network.requestWillBeSent" &&
        params.request.url.includes("/api/v1/chart/data")) {
      requests.set(params.requestId, params.request.postData || "{}");
      lastActivity = Date.now();
    }
    if (method === "Network.loadingFinished" && requests.has(params.requestId)) {
      lastActivity = Date.now();
      try {
        const body = await send("Network.getResponseBody", { requestId: params.requestId });
        const request = JSON.parse(requests.get(params.requestId));
        const query = (request.queries || [{}])[0];
        const result = (JSON.parse(body.body).result || [{}])[0];
        charts.push({
          slice_id: request.form_data?.slice_id,
          time_range: query.time_range ?? null,
          post_processing: (query.post_processing || []).map((step) => step?.operation)
            .filter(Boolean),
          rowcount: result.rowcount, status: result.status, error: result.error ?? null,
        });
      } catch (error) { charts.push({ error: String(error) }); }
    }
  });

  let url = `${BASE}/superset/dashboard/${dashId}/`;
  if (range) {
    const rison = `(${filterId}:(extraFormData:(time_range:'${range}'),` +
      `filterState:(value:'${range}'),id:${filterId},ownState:()))`;
    url += `?native_filters=${encodeURIComponent(rison)}`;
  }
  await open(url);
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    await sleep(1000);
    const holders = await evaluate(
      "document.querySelectorAll('.dashboard-component-chart-holder').length");
    if (holders && charts.length >= holders && Date.now() - lastActivity > 4000) break;
  }
  await sleep(3500);
  const page = await evaluate(`(() => {
    const holders = [...document.querySelectorAll('.dashboard-component-chart-holder')];
    return {
      title: document.title,
      charts: holders.length,
      rendered: holders.filter((h) => h.querySelector('canvas, svg, table')).length,
      error_alerts: document.querySelectorAll('[data-test="error-alert"], .alert-danger').length,
    };
  })()`);
  const shot = await send("Page.captureScreenshot", { format: "png" });
  writeFileSync(outPng, Buffer.from(shot.data, "base64"));
  const summary = { dashboard_id: Number(dashId), url_time_range: range ?? null, ...page,
    chart_data: charts.sort((a, b) => (a.slice_id ?? 0) - (b.slice_id ?? 0)) };
  writeFileSync(outJson, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary));
} finally {
  try { socket?.close(); } catch { /* already closed */ }
  chrome.kill();
  await sleep(1000);
  rmSync(profile, { recursive: true, force: true });
}
