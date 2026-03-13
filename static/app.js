const $ = (id) => document.getElementById(id);

const state = {
  status: {},
  sample: {},
  ports: [],
  errorText: "",
  plot: {
    t: [],
    starterRpm: [],
    starterDuty: [],
    starterCur: [],
    startedAt: null,
    maxSeconds: 30,
    maxPoints: 1200,
  },
};

let ws = null;
let autoPortsTimer = null;

function fmt(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  if (typeof v === "number") return v.toFixed(digits);
  return String(v);
}

async function api(path, method = "POST", body = null) {
  const res = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : null,
  });

  const text = await res.text();
  if (!res.ok) throw new Error(text || `${res.status}`);

  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}

function setLamp(id, on) {
  const el = $(id);
  if (!el) return;
  el.classList.toggle("on", !!on);
  el.classList.toggle("off", !on);
}

function setActive(buttons, activeId) {
  buttons.forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.classList.toggle("active", id === activeId);
  });
}

function setError(text) {
  state.errorText = text || "";
  $("lbl-error").textContent = state.errorText;
}

function parsePorts(payload) {
  return (payload?.ports || []).map((p) => {
    if (typeof p === "string") return p;
    if (p && typeof p === "object") return p.device || p.name || p.port || String(p);
    return String(p);
  });
}

function fillPorts(ports) {
  state.ports = ports || [];
  ["cb-pump", "cb-starter", "cb-psu"].forEach((id) => {
    const cb = $(id);
    const current = cb.value;
    cb.innerHTML = "";

    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = "";
    cb.appendChild(empty);

    for (const p of state.ports) {
      const opt = document.createElement("option");
      opt.value = p;
      opt.textContent = p;
      cb.appendChild(opt);
    }

    if (state.ports.includes(current)) {
      cb.value = current;
    }
  });
}

async function refreshPorts() {
  const data = await fetch("/api/ports").then((r) => r.json());
  fillPorts(parsePorts(data));
}

function toggleAutoPorts() {
  if (autoPortsTimer) {
    clearInterval(autoPortsTimer);
    autoPortsTimer = null;
  }

  if ($("chk-auto-ports").checked) {
    autoPortsTimer = setInterval(() => {
      const connected = state.status.connected || state.sample.connected || {};
      const anyConnected = !!(connected.pump || connected.starter || connected.psu);
      if (!anyConnected) {
        refreshPorts().catch((e) => setError(e.message || String(e)));
      }
    }, 1500);
  }
}

function resetPlot() {
  state.plot.t = [];
  state.plot.starterRpm = [];
  state.plot.starterDuty = [];
  state.plot.starterCur = [];
  state.plot.startedAt = null;
  drawStarterChart();
}

function numOrNull(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function pushPlot(sample) {
  const starter = sample?.starter || {};
  const now = performance.now() / 1000;

  if (state.plot.startedAt === null) {
    state.plot.startedAt = now;
  }

  const t = now - state.plot.startedAt;

  state.plot.t.push(t);
  state.plot.starterRpm.push(numOrNull(starter.rpm_mech));
  state.plot.starterDuty.push(numOrNull(starter.duty));
  state.plot.starterCur.push(numOrNull(starter.current_motor));

  while (state.plot.t.length > state.plot.maxPoints) {
    state.plot.t.shift();
    state.plot.starterRpm.shift();
    state.plot.starterDuty.shift();
    state.plot.starterCur.shift();
  }

  const maxSeconds = state.plot.maxSeconds;
  while (state.plot.t.length && (state.plot.t[state.plot.t.length - 1] - state.plot.t[0]) > maxSeconds) {
    state.plot.t.shift();
    state.plot.starterRpm.shift();
    state.plot.starterDuty.shift();
    state.plot.starterCur.shift();
  }

  drawStarterChart();
}

function seriesMax(values, fallback, base) {
  const clean = values.filter((v) => v !== null && Number.isFinite(v));
  const raw = clean.length ? Math.max(...clean) : fallback;
  if (!Number.isFinite(raw) || raw <= 0) return fallback;
  return Math.ceil(raw / base) * base;
}

function seriesMax(values, fallback, base) {
  const clean = values.filter((v) => v !== null && Number.isFinite(v));
  const raw = clean.length ? Math.max(...clean) : fallback;
  if (!Number.isFinite(raw) || raw <= 0) return fallback;
  return Math.ceil(raw / base) * base;
}

function seriesRange(values, fallbackMin, fallbackMax, base) {
  const clean = values.filter((v) => v !== null && Number.isFinite(v));

  if (!clean.length) {
    return { min: fallbackMin, max: fallbackMax };
  }

  let min = Math.min(...clean);
  let max = Math.max(...clean);

  if (min === max) {
    const pad = Math.max(Math.abs(min) * 0.2, base);
    min -= pad;
    max += pad;
  } else {
    const pad = Math.max((max - min) * 0.1, base * 0.25);
    min -= pad;
    max += pad;
  }

  min = Math.floor(min / base) * base;
  max = Math.ceil(max / base) * base;

  if (min === max) {
    min -= base;
    max += base;
  }

  return { min, max };
}

function drawAxisTicks(ctx, x, yTop, yBottom, maxValue, color, align = "left") {
  ctx.fillStyle = color;
  ctx.font = "12px Segoe UI, Arial";
  ctx.textAlign = align;

  for (let i = 0; i <= 5; i += 1) {
    const yVal = (maxValue * (5 - i)) / 5;
    const y = yTop + ((yBottom - yTop) * i) / 5;
    const text = maxValue <= 1 ? yVal.toFixed(2) : yVal.toFixed(0);
    ctx.fillText(text, x, y + 4);
  }
}

function drawAxisTicksRange(ctx, x, yTop, yBottom, minValue, maxValue, color, decimals = 1) {
  ctx.fillStyle = color;
  ctx.font = "12px Segoe UI, Arial";
  ctx.textAlign = "left";

  for (let i = 0; i <= 5; i += 1) {
    const ratio = i / 5;
    const y = yTop + (yBottom - yTop) * ratio;
    const value = maxValue - (maxValue - minValue) * ratio;
    ctx.fillText(value.toFixed(decimals), x, y + 4);
  }
}

function drawStarterChart() {
  const canvas = $("starter-chart");
  if (!canvas) return;

  const rect = canvas.getBoundingClientRect();
  const width = Math.max(500, Math.floor(rect.width));
  const height = Math.max(220, Math.floor(rect.height));
  const dpr = window.devicePixelRatio || 1;

  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);

  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const pad = { left: 60, right: 130, top: 18, bottom: 36 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, width, height);

  ctx.strokeStyle = "#d9d9d9";
  ctx.lineWidth = 1;

  for (let i = 0; i <= 5; i += 1) {
    const y = pad.top + (plotH * i) / 5;
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(pad.left + plotW, y);
    ctx.stroke();
  }

  for (let i = 0; i <= 6; i += 1) {
    const x = pad.left + (plotW * i) / 6;
    ctx.beginPath();
    ctx.moveTo(x, pad.top);
    ctx.lineTo(x, pad.top + plotH);
    ctx.stroke();
  }

  const t = state.plot.t;
  const rpm = state.plot.starterRpm;
  const duty = state.plot.starterDuty;
  const cur = state.plot.starterCur;

  let tMin = 0;
  let tMax = state.plot.maxSeconds;

  if (t.length) {
    tMax = Math.max(state.plot.maxSeconds, t[t.length - 1]);
    tMin = Math.max(0, tMax - state.plot.maxSeconds);
  }

  const rpmVals = rpm.filter((v) => v !== null);
  const dutyVals = duty.filter((v) => v !== null);
  const curVals = cur.filter((v) => v !== null);

  const rpmMax = seriesMax(rpmVals, 1000, 1000);
  const dutyMax = seriesMax(dutyVals, 0.1, 0.1);
  const curRange = seriesRange(curVals, -10, 10, 5);

  const mapX = (x) =>
    pad.left + ((x - tMin) / Math.max(1e-9, tMax - tMin)) * plotW;

  const mapLeftY = (y) =>
    pad.top + (1 - y / Math.max(1e-9, rpmMax)) * plotH;

  const mapDutyY = (y) =>
    pad.top + (1 - y / Math.max(1e-9, dutyMax)) * plotH;

  const mapCurY = (y) =>
    pad.top + (1 - (y - curRange.min) / Math.max(1e-9, curRange.max - curRange.min)) * plotH;

  ctx.strokeStyle = "#000000";
  ctx.lineWidth = 2;
  drawSeries(ctx, t, rpm, mapX, mapLeftY, tMin);

  ctx.strokeStyle = "#008000";
  ctx.lineWidth = 2;
  ctx.setLineDash([8, 6]);
  drawSeries(ctx, t, duty, mapX, mapDutyY, tMin);
  ctx.setLineDash([]);

  ctx.strokeStyle = "#c00000";
  ctx.lineWidth = 2;
  ctx.setLineDash([3, 6]);
  drawSeries(ctx, t, cur, mapX, mapCurY, tMin);
  ctx.setLineDash([]);

  ctx.strokeStyle = "#333333";
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.moveTo(pad.left, pad.top);
  ctx.lineTo(pad.left, pad.top + plotH);
  ctx.lineTo(pad.left + plotW, pad.top + plotH);
  ctx.stroke();

  const dutyAxisX = pad.left + plotW + 8;
  const curAxisX = pad.left + plotW + 62;

  ctx.beginPath();
  ctx.moveTo(dutyAxisX, pad.top);
  ctx.lineTo(dutyAxisX, pad.top + plotH);
  ctx.strokeStyle = "#008000";
  ctx.stroke();

  ctx.beginPath();
  ctx.moveTo(curAxisX, pad.top);
  ctx.lineTo(curAxisX, pad.top + plotH);
  ctx.strokeStyle = "#c00000";
  ctx.stroke();

  if (curRange.min < 0 && curRange.max > 0) {
    const yZero = mapCurY(0);
    ctx.save();
    ctx.strokeStyle = "rgba(192, 0, 0, 0.35)";
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(pad.left, yZero);
    ctx.lineTo(pad.left + plotW, yZero);
    ctx.stroke();
    ctx.restore();
  }

  ctx.fillStyle = "#222";
  ctx.font = "12px Segoe UI, Arial";
  ctx.textAlign = "left";

  for (let i = 0; i <= 5; i += 1) {
    const yVal = (rpmMax * (5 - i)) / 5;
    const y = pad.top + (plotH * i) / 5;
    ctx.fillText(String(Math.round(yVal)), 8, y + 4);
  }

  for (let i = 0; i <= 5; i += 1) {
    const xVal = tMin + ((tMax - tMin) * i) / 5;
    const x = pad.left + (plotW * i) / 5;
    ctx.fillText(xVal.toFixed(0), x - 6, height - 12);
  }

  drawAxisTicks(ctx, dutyAxisX + 6, pad.top, pad.top + plotH, dutyMax, "#008000", "left");
  drawAxisTicksRange(ctx, curAxisX + 6, pad.top, pad.top + plotH, curRange.min, curRange.max, "#c00000", 1);

  ctx.fillStyle = "#222";
  ctx.textAlign = "left";
  ctx.fillText("RPM", 14, 14);
  ctx.fillText("t (s)", pad.left + plotW / 2 - 10, height - 8);

  ctx.fillStyle = "#008000";
  ctx.fillText("Duty", dutyAxisX - 2, 14);

  ctx.fillStyle = "#c00000";
  ctx.fillText("Current", curAxisX - 8, 14);

  drawLegend(ctx, pad.left + 10, 16);
}

function drawSeries(ctx, t, values, mapX, mapY, tMin) {
  let started = false;
  ctx.beginPath();
  for (let i = 0; i < t.length; i += 1) {
    const xVal = t[i];
    const yVal = values[i];
    if (xVal < tMin || yVal === null) continue;
    const x = mapX(xVal);
    const y = mapY(yVal);
    if (!started) {
      ctx.moveTo(x, y);
      started = true;
    } else {
      ctx.lineTo(x, y);
    }
  }
  ctx.stroke();
}

function drawLegend(ctx, x, y) {
  const items = [
    ["#000000", "Starter RPM", []],
    ["#008000", "Starter Duty", [8, 6]],
    ["#c00000", "Starter Current", [3, 6]],
  ];

  let offset = 0;
  for (const [color, text, dash] of items) {
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.setLineDash(dash);
    ctx.beginPath();
    ctx.moveTo(x + offset, y);
    ctx.lineTo(x + offset + 24, y);
    ctx.stroke();
    ctx.restore();

    ctx.fillStyle = "#222";
    ctx.font = "12px Segoe UI, Arial";
    ctx.textAlign = "left";
    ctx.fillText(text, x + offset + 30, y + 4);
    offset += 112;
  }
}

function normalizeStage(stage) {
  const s = String(stage ?? "").trim();

  const map = {
    "idle": "idle",
    "Idle": "idle",

    "ready": "ready",
    "Ready": "ready",

    "starter": "starter",
    "Starter": "starter",

    "fuelramp": "fuelramp",
    "FuelRamp": "fuelramp",

    "running": "running",
    "Running": "running",

    "fault": "fault",
    "Fault": "fault",

    "manual": "manual",
    "Manual": "manual",

    "cooling": "cooling",
    "Cooling": "cooling",

    "stop": "stop",
    "Stop": "stop",

    "PumpProfile": "pumpprofile",
    "pump_profile": "pumpprofile",
    "pumpprofile": "pumpprofile",
  };

  return map[s] || (s ? s.toLowerCase() : "-");
}

function updateStatus(status) {
  if (!status) return;

  state.status = { ...state.status, ...status };

  if (status.ready || status.reset_plot) {
    resetPlot();
  }

   if (status.stage !== undefined) {
     $("lbl-stage").textContent = `stage: ${normalizeStage(status.stage)}`;
   }

  if (status.log_path) {
    $("lbl-log").textContent = `log: ${status.log_path}`;
  }

  if (status.connected) {
    const c = status.connected || {};
    setLamp("lamp-pump", !!c.pump);
    setLamp("lamp-starter", !!c.starter);
    setLamp("lamp-psu", !!c.psu);
  }

  if (status.pump_profile) {
    const active = !!status.pump_profile.active;
    $("btn-pump-profile-start").disabled = active;
    if (!active) setActive(["btn-pump-profile-start"], null);
  }

  if (status.valve_macro) {
    const active = !!status.valve_macro.active;
    if (!active) {
      setActive(["btn-valve-on", "btn-valve-off"], "btn-valve-off");
    }
  }
}

function updateSample(sample) {
  if (!sample) return;
  state.sample = sample;

  if (sample.stage !== undefined) {
  $("lbl-stage").textContent = `stage: ${normalizeStage(sample.stage)}`;
    }

  const pump = sample.pump || {};
  const starter = sample.starter || {};
  const psu = sample.psu || {};
  const connected = sample.connected || state.status.connected || {};

  setLamp("lamp-pump", !!connected.pump);
  setLamp("lamp-starter", !!connected.starter);
  setLamp("lamp-psu", !!connected.psu);

  $("lbl-pump-rpm-live").textContent = `${Math.round(Number(pump.rpm_mech || 0))} rpm`;
  $("lbl-starter-rpm-live").textContent = `${Math.round(Number(starter.rpm_mech || 0))} rpm`;
  $("lbl-psu-live").textContent = `${fmt(psu.v_out, 1)}V / ${fmt(psu.i_out, 1)}A`;

  pushPlot(sample);
}

function connectWs() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    setLamp("ws-lamp", true);
    $("ws-text").textContent = "WS: online";
    setError("");
  };

  ws.onclose = () => {
    setLamp("ws-lamp", false);
    $("ws-text").textContent = "WS: offline";
    setTimeout(connectWs, 1000);
  };

  ws.onerror = () => {
    setError("WebSocket error");
  };

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);

    if (msg.event === "snapshot") {
      const snap = msg.payload || {};
      fillPorts(parsePorts({ ports: snap.ports || [] }));
      updateStatus(snap.status || {});
      updateSample(snap.sample || {});
      return;
    }

    if (msg.event === "status") {
      updateStatus(msg.payload || {});
      return;
    }

    if (msg.event === "sample") {
      updateSample(msg.payload || {});
      return;
    }

    if (msg.event === "error") {
      setError(typeof msg.payload === "string" ? msg.payload : JSON.stringify(msg.payload));
    }
  };
}

function bindEnter(inputId, buttonId) {
  const input = $(inputId);
  if (!input) return;
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      $(buttonId).click();
    }
  });
}

async function choosePumpProfile() {
  if (!(window.pywebview && window.pywebview.api && window.pywebview.api.choose_xlsx)) {
    setError("File dialog is available only in desktop_webview.py");
    return;
  }

  try {
    const path = await window.pywebview.api.choose_xlsx();
    if (path) {
      $("in-pump-profile-path").value = path;
      setError("");
    }
  } catch (e) {
    setError(e.message || String(e));
  }
}

function savePolePairs() {
  return api("/api/pole-pairs", "POST", {
    pump: Number($("pp-pump").value),
    starter: Number($("pp-starter").value),
  });
}

function bind() {
  $("btn-ready").onclick = async () => {
    try {
      await api("/api/ready", "POST", { prefix: "manual" });
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

$("btn-update").onclick = async () => {
  try {
    await api("/api/update-reset");

    const snap = await fetch("/api/state").then((r) => r.json());

    resetPlot();
    fillPorts(parsePorts({ ports: snap.ports || [] }));
    updateStatus({ ...(snap.status || {}), reset_plot: true });
    updateSample(snap.sample || {});

    if (snap.last_error) {
      setError(String(snap.last_error));
    } else {
      setError("");
    }
  } catch (e) {
    setError(e.message || String(e));
  }
};

  $("btn-run").onclick = async () => {
    try {
      await api("/api/run-cycle");
      setActive(["btn-run"], "btn-run");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-stop-all").onclick = async () => {
    try {
      await api("/api/stop-all");
      setActive(["btn-run"], null);
      setActive(["btn-pump-set-duty", "btn-pump-set-rpm", "btn-pump-stop"], "btn-pump-stop");
      setActive(["btn-starter-set-duty", "btn-starter-set-rpm", "btn-starter-stop"], "btn-starter-stop");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-refresh-ports").onclick = () => refreshPorts().catch((e) => setError(e.message || String(e)));
  $("chk-auto-ports").onchange = toggleAutoPorts;

  $("btn-pump-connect").onclick = async () => {
    try {
      await api("/api/pump/connect", "POST", { port: $("cb-pump").value });
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };
  $("btn-pump-disconnect").onclick = async () => {
    try {
      await api("/api/pump/disconnect");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };
  $("btn-starter-connect").onclick = async () => {
    try {
      await api("/api/starter/connect", "POST", { port: $("cb-starter").value });
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };
  $("btn-starter-disconnect").onclick = async () => {
    try {
      await api("/api/starter/disconnect");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };
  $("btn-psu-connect").onclick = async () => {
    try {
      await api("/api/psu/connect", "POST", { port: $("cb-psu").value });
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };
  $("btn-psu-disconnect").onclick = async () => {
    try {
      await api("/api/psu/disconnect");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-pump-set-duty").onclick = async () => {
    try {
      await savePolePairs();
      await api("/api/pump/duty", "POST", { value: Number($("in-pump-duty").value) });
      setActive(["btn-pump-set-duty", "btn-pump-set-rpm", "btn-pump-stop"], "btn-pump-set-duty");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-pump-set-rpm").onclick = async () => {
    try {
      await savePolePairs();
      await api("/api/pump/rpm", "POST", { value: Number($("in-pump-rpm").value) });
      setActive(["btn-pump-set-duty", "btn-pump-set-rpm", "btn-pump-stop"], "btn-pump-set-rpm");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-pump-stop").onclick = async () => {
    try {
      await api("/api/pump/duty", "POST", { value: 0.0 });
      setActive(["btn-pump-set-duty", "btn-pump-set-rpm", "btn-pump-stop"], "btn-pump-stop");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-starter-set-duty").onclick = async () => {
    try {
      await savePolePairs();
      await api("/api/starter/duty", "POST", { value: Number($("in-starter-duty").value) });
      setActive(["btn-starter-set-duty", "btn-starter-set-rpm", "btn-starter-stop"], "btn-starter-set-duty");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-starter-set-rpm").onclick = async () => {
    try {
      await savePolePairs();
      await api("/api/starter/rpm", "POST", { value: Number($("in-starter-rpm").value) });
      setActive(["btn-starter-set-duty", "btn-starter-set-rpm", "btn-starter-stop"], "btn-starter-set-rpm");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-starter-stop").onclick = async () => {
    try {
      await api("/api/starter/duty", "POST", { value: 0.0 });
      setActive(["btn-starter-set-duty", "btn-starter-set-rpm", "btn-starter-stop"], "btn-starter-stop");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-pump-profile-browse").onclick = choosePumpProfile;

  $("btn-pump-profile-start").onclick = async () => {
    try {
      await api("/api/pump-profile/start", "POST", { path: $("in-pump-profile-path").value.trim() });
      setActive(["btn-pump-profile-start"], "btn-pump-profile-start");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-psu-set").onclick = async () => {
    try {
      await api("/api/psu/vi", "POST", {
        v: Number($("in-psu-v").value),
        i: Number($("in-psu-i").value),
      });
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-psu-on").onclick = async () => {
    try {
      await api("/api/psu/output", "POST", { value: true });
      setActive(["btn-psu-on", "btn-psu-off"], "btn-psu-on");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-psu-off").onclick = async () => {
    try {
      await api("/api/psu/output", "POST", { value: false });
      setActive(["btn-psu-on", "btn-psu-off"], "btn-psu-off");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-valve-on").onclick = async () => {
    try {
      await api("/api/valve/on");
      setActive(["btn-valve-on", "btn-valve-off"], "btn-valve-on");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  $("btn-valve-off").onclick = async () => {
    try {
      await api("/api/valve/off");
      setActive(["btn-valve-on", "btn-valve-off"], "btn-valve-off");
      setError("");
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  bindEnter("in-pump-duty", "btn-pump-set-duty");
  bindEnter("in-pump-rpm", "btn-pump-set-rpm");
  bindEnter("in-starter-duty", "btn-starter-set-duty");
  bindEnter("in-starter-rpm", "btn-starter-set-rpm");
  bindEnter("in-psu-v", "btn-psu-set");
  bindEnter("in-psu-i", "btn-psu-set");

  window.addEventListener("resize", drawStarterChart);

  setActive(["btn-pump-set-duty", "btn-pump-set-rpm", "btn-pump-stop"], "btn-pump-stop");
  setActive(["btn-starter-set-duty", "btn-starter-set-rpm", "btn-starter-stop"], "btn-starter-stop");
  setActive(["btn-psu-on", "btn-psu-off"], "btn-psu-off");
  setActive(["btn-valve-on", "btn-valve-off"], "btn-valve-off");
}

window.addEventListener("DOMContentLoaded", async () => {
  bind();
  drawStarterChart();
  connectWs();

  try {
    await refreshPorts();
  } catch (e) {
    setError(e.message || String(e));
  }

  try {
    const snap = await fetch("/api/state").then((r) => r.json());
    fillPorts(parsePorts({ ports: snap.ports || [] }));
    updateStatus(snap.status || {});
    updateSample(snap.sample || {});
  } catch (e) {
    setError(e.message || String(e));
  }
});
