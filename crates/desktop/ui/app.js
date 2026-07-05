// Crawl Crew frontend. All data arrives over Tauri IPC (`invoke` +
// `listen`) — the webview itself never touches the network. Everything is
// built to idle at ~0% CPU: the canvas only animates while orbs are alive
// and the tab is visible, and the leaderboard polls at a lazy 45 s.

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;

const $ = (s) => document.querySelector(s);
const fmt = (n) => Number(n).toLocaleString("en-US");

// ---------- state ----------
let ui = null; // UiState from Rust
let stats = null; // StatsResponse
let tab = "m60";
let session = { jobs: 0 };
let lifetime = { requests: 0, matches: 0 };

// ---------- host colors ----------
const HOST_HUES = { europe: 265, americas: 174, asia: 38, sea: 330 };
function hueOf(host) {
  if (host in HOST_HUES) return HOST_HUES[host];
  let h = 0;
  for (const c of host) h = (h * 31 + c.charCodeAt(0)) % 360;
  return h;
}
const colorOf = (host, l = 66) => `hsl(${hueOf(host)} 85% ${l}%)`;

const seenHosts = new Set();
function legendAdd(host) {
  if (seenHosts.has(host)) return;
  seenHosts.add(host);
  const k = document.createElement("span");
  k.className = "key";
  const sw = document.createElement("i");
  sw.className = "swatch";
  sw.style.background = colorOf(host);
  k.append(sw, document.createTextNode(host));
  $("#legend").append(k);
}

// ---------- warm words ----------
const MESSAGES = [
  (n) => `every match you fetch makes the model a little smarter 🧠`,
  (n) => `you're literally donating science right now`,
  (n) => `${n}, you absolute legend 💜`,
  (n) => `somewhere, a gradient is descending thanks to you`,
  (n) => `your API key is living its best life`,
  (n) => `the crawl must flow ⏳`,
  (n) => `friendship: measured in requests per second`,
  (n) => `Riot's servers say hi (politely, within rate limits)`,
  (n) => `this window is 100% optional — the crew works from the tray too`,
  (n) => `fun fact: closing this window frees RAM, the crawling keeps going`,
];
let msgIdx = 0;
function rotateMsg() {
  const el = $("#rotator");
  el.classList.add("fade");
  setTimeout(() => {
    msgIdx = (msgIdx + 1) % MESSAGES.length;
    el.textContent = MESSAGES[msgIdx](ui?.name || "friend");
    el.classList.remove("fade");
  }, 600);
}

// ---------- status pill ----------
function setStatus(kind, text) {
  const pill = $("#status");
  pill.className = "pill " + kind;
  $("#status-text").textContent = text;
}

// ---------- counters ----------
function bump(sel) {
  const el = $(sel);
  el.classList.remove("bump");
  void el.offsetWidth; // restart the transition
  el.classList.add("bump");
}
function renderCounters() {
  $("#c-matches").textContent = fmt(lifetime.matches);
  $("#c-requests").textContent = fmt(lifetime.requests);
  $("#c-session").textContent = fmt(session.jobs);
  $("#vault-n").textContent = fmt(session.jobs);
}

// ---------- milestones ----------
const MILESTONES = [100, 500, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000];
function checkMilestone(matches) {
  const hit = MILESTONES.filter((m) => matches >= m).pop() || 0;
  const seen = Number(localStorage.getItem("milestone") || 0);
  if (hit > seen) {
    localStorage.setItem("milestone", String(hit));
    if (seen > 0) {
      // don't celebrate historical milestones on first ever load
      viz.confetti();
      toast(`🎉 ${fmt(hit)} matches delivered — thank you!!`);
    }
  }
}

// ---------- toast ----------
let toastTimer = null;
function toast(text) {
  const t = $("#toast");
  t.textContent = text;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 4200);
}

// ---------- canvas visualization ----------
// One orb per job, following its real lifecycle:
//   wait   — pulled from the server, queued on the local rate limiter:
//            falls in, then hovers with a *very slow* jitter, 50% dim.
//   active — the Riot request is on the wire (and, after job_done, the
//            result is uploading): bright, fast jitter.
//   fly    — uploaded OK: jitter stops, smooth glide into the vault.
//   fade   — uploaded but not a stored body (404 grey / failed red /
//            key_rejected amber), or a safety timeout: still fade-out.
// wait<->active blend through an "energy" scalar so speed, amplitude and
// brightness ramp smoothly instead of popping.
const viz = (() => {
  const canvas = $("#viz");
  const ctx = canvas.getContext("2d");
  let W = 0, H = 0, dpr = 1;
  const orbs = new Map(); // job id -> orb
  let parts = []; // particles
  let raf = null, lastT = 0;

  const FADE_COLORS = { not_found: "#6b7194", failed: "#f87171", key_rejected: "#ffb454" };

  function resize() {
    const r = canvas.parentElement.getBoundingClientRect();
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = Math.max(1, r.width);
    H = Math.max(1, r.height);
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  new ResizeObserver(resize).observe(canvas.parentElement);

  function vaultXY() {
    const v = $("#vault");
    const c = canvas.getBoundingClientRect();
    const r = v.getBoundingClientRect();
    return { x: r.left - c.left + r.width / 2, y: r.top - c.top + r.height / 2 };
  }

  function spawn(id, host) {
    if (orbs.size > 90) return; // sanity cap; counters still tick
    legendAdd(host);
    const x = 20 + Math.random() * (W - 40);
    orbs.set(id, {
      host,
      x,
      y: -8,
      wx: x, wy: -8, // last drawn position (incl. wobble)
      hoverY: H * (0.30 + Math.random() * 0.38),
      phase: Math.random() * Math.PI * 2,
      r: 4.5 + Math.random() * 2.5,
      mode: "wait",
      e: 0, // energy: 0 = queued, 1 = on the wire
      t: 0, // seconds in current mode
      uploadT: null, // seconds since job_done (upload in flight)
      outcome: null,
      fadeColor: FADE_COLORS.not_found,
    });
    wake();
  }

  // Limiter permits acquired: the request is going out.
  function activate(id) {
    const o = orbs.get(id);
    if (o && o.mode === "wait") {
      o.mode = "active";
      o.t = 0;
      wake();
    }
  }

  // Riot answered; the result is now uploading to the server.
  function setOutcome(id, outcome) {
    const o = orbs.get(id);
    if (!o) return;
    o.outcome = outcome;
    o.uploadT = 0;
    if (o.mode === "wait") {
      // job_active got dropped (event lag): catch up.
      o.mode = "active";
      o.t = 0;
    }
    wake();
  }

  // Result reached the server: stop jittering and leave.
  function resolve(id) {
    const o = orbs.get(id);
    if (o) resolveOrb(id, o);
  }

  function resolveOrb(id, o) {
    if (o.mode === "fly" || o.mode === "fade") return;
    if (o.outcome === "ok") {
      o.mode = "fly";
    } else {
      o.mode = "fade";
      o.fadeColor = FADE_COLORS[o.outcome] || FADE_COLORS.not_found;
    }
    o.t = 0;
    wake();
  }

  function fadeOut(o, color) {
    o.mode = "fade";
    o.fadeColor = color;
    o.t = 0;
  }

  function drawOrb(x, y, r, color, alpha) {
    ctx.beginPath();
    ctx.arc(x, y, r, 0, 7);
    ctx.fillStyle = color;
    ctx.globalAlpha = Math.max(0, Math.min(1, alpha));
    ctx.fill();
    ctx.globalAlpha = 1;
  }

  function bumpVault() {
    const v = $("#vault");
    v.classList.remove("bump");
    void v.offsetWidth;
    v.classList.add("bump");
  }

  function pop(x, y, color, n = 10) {
    for (let i = 0; i < n; i++) {
      const a = Math.random() * Math.PI * 2;
      const v = 30 + Math.random() * 90;
      parts.push({ x, y, vx: Math.cos(a) * v, vy: Math.sin(a) * v, life: 0.55, t: 0, color, r: 1.5 + Math.random() * 1.8 });
    }
    wake();
  }

  function confetti() {
    const colors = ["#8b7cff", "#ff7a9e", "#34d3bd", "#ffb454", "#4ade80"];
    for (let i = 0; i < 130; i++) {
      parts.push({
        x: Math.random() * W, y: -10,
        vx: (Math.random() - 0.5) * 60, vy: 60 + Math.random() * 120,
        life: 2.2 + Math.random() * 1.2, t: 0,
        color: colors[i % colors.length], r: 2 + Math.random() * 2.5,
      });
    }
    wake();
  }

  function step(now) {
    raf = null;
    const dt = Math.min(0.05, (now - lastT) / 1000 || 0.016);
    lastT = now;
    ctx.clearRect(0, 0, W, H);

    const v = vaultXY();
    for (const [id, o] of orbs) {
      o.t += dt;
      if (o.mode === "wait" || o.mode === "active") {
        // Energy ramps 0->1 on activation so jitter speed, amplitude and
        // brightness blend smoothly instead of popping.
        const target = o.mode === "active" ? 1 : 0;
        o.e += (target - o.e) * Math.min(1, dt * 6);
        // Very slow jitter queued (0.35), the familiar fast one active (2.5).
        o.phase += dt * (0.35 + 2.15 * o.e);
        if (o.uploadT !== null) o.uploadT += dt;

        // Fall gently to the hover band (slow-jitter descent), then bob.
        if (o.y < o.hoverY) o.y = Math.min(o.hoverY, o.y + dt * 110);
        const hovering = o.y >= o.hoverY;
        const wob = Math.sin(o.phase * 0.6) * (3.5 + 2.5 * o.e);
        const bob = hovering ? Math.sin(o.phase) * (2 + o.e) : 0;
        o.wx = o.x + wob;
        o.wy = o.y + bob;

        // Queued: ~50% of the active brightness. Active: bright pulse.
        const aWait = 0.4 + 0.05 * Math.sin(o.phase);
        const aActive = 0.75 + 0.25 * Math.sin(o.phase * 1.7);
        drawOrb(o.wx, o.wy, o.r, colorOf(o.host), aWait * (1 - o.e) + aActive * o.e);

        // Safety nets for lost events; server-side leases make these moot.
        if (o.mode === "wait" && o.t > 160) {
          fadeOut(o, FADE_COLORS.not_found); // lease long expired
        } else if (o.uploadT !== null && o.uploadT > 20) {
          resolveOrb(id, o); // job_uploaded got lost; resolve by outcome
        } else if (o.mode === "active" && o.t > 180) {
          fadeOut(o, FADE_COLORS.not_found); // stuck in retries/cooldowns
        }
      } else if (o.mode === "fly") {
        // Jitter is over: glide from the exact last drawn position.
        const k = 1 - Math.pow(1 - Math.min(1, o.t / 0.6), 3);
        const x = o.wx + (v.x - o.wx) * k;
        const y = o.wy + (v.y - o.wy) * k;
        drawOrb(x, y, o.r * (1 - 0.5 * k), colorOf(o.host, 74), 1);
        if (k >= 1) {
          orbs.delete(id);
          pop(v.x, v.y, colorOf(o.host, 74), 8);
          bumpVault();
        }
      } else {
        // fade: frozen in place, no jitter
        const k = Math.min(1, o.t / 0.5);
        drawOrb(o.wx, o.wy, o.r, o.fadeColor, (1 - k) * 0.8);
        if (k >= 1) orbs.delete(id);
      }
    }

    parts = parts.filter((p) => {
      p.t += dt;
      if (p.t >= p.life) return false;
      p.x += p.vx * dt;
      p.y += p.vy * dt;
      p.vy += 140 * dt;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, 7);
      ctx.fillStyle = p.color;
      ctx.globalAlpha = 1 - p.t / p.life;
      ctx.fill();
      ctx.globalAlpha = 1;
      return true;
    });

    if ((orbs.size || parts.length) && !document.hidden) {
      raf = requestAnimationFrame(step);
    } else {
      ctx.clearRect(0, 0, W, H);
    }
  }

  function wake() {
    if (!raf && !document.hidden) {
      lastT = performance.now();
      raf = requestAnimationFrame(step);
    }
  }

  document.addEventListener("visibilitychange", () => { if (!document.hidden) wake(); });
  resize();
  return { spawn, activate, setOutcome, resolve, confetti };
})();

// ---------- leaderboard ----------
function renderBoard() {
  const board = $("#board");
  if (!stats || !stats.nodes.length) {
    board.innerHTML = '<div class="empty">waiting for the first numbers…</div>';
    return;
  }
  const rows = [...stats.nodes].sort((a, b) => b.requests[tab] - a.requests[tab] || a.name.localeCompare(b.name));
  const max = Math.max(1, ...rows.map((r) => r.requests[tab]));
  board.textContent = "";
  const medals = ["🥇", "🥈", "🥉"];
  rows.forEach((r, i) => {
    const row = document.createElement("div");
    row.className = "row" + (r.name === stats.you ? " you" : "");

    const rank = document.createElement("div");
    rank.className = "rank";
    rank.textContent = r.requests[tab] > 0 && i < 3 ? medals[i] : String(i + 1);

    const who = document.createElement("div");
    who.className = "who";
    const nm = document.createElement("div");
    nm.className = "nm";
    const dot = document.createElement("span");
    dot.className = "on" + (r.online ? " live" : "");
    dot.title = r.online ? "online" : "offline";
    const name = document.createElement("span");
    name.textContent = r.name;
    nm.append(dot, name);
    if (r.name === stats.you) {
      const tag = document.createElement("span");
      tag.className = "tag";
      tag.textContent = "YOU";
      nm.append(tag);
    }
    const bar = document.createElement("div");
    bar.className = "bar";
    const fill = document.createElement("i");
    fill.style.width = "0%";
    bar.append(fill);
    who.append(nm, bar);
    requestAnimationFrame(() => { fill.style.width = (100 * r.requests[tab] / max).toFixed(1) + "%"; });

    const val = document.createElement("div");
    val.className = "val";
    const req = document.createElement("div");
    req.className = "req";
    req.textContent = fmt(r.requests[tab]);
    const mat = document.createElement("div");
    mat.className = "mat";
    mat.textContent = fmt(r.matches[tab]) + " matches";
    val.append(req, mat);

    row.append(rank, who, val);
    board.append(row);
  });

  const treq = rows.reduce((s, r) => s + r.requests[tab], 0);
  const tmat = rows.reduce((s, r) => s + r.matches[tab], 0);
  $("#crewline").innerHTML = "";
  const line = $("#crewline");
  line.append("together: " + fmt(treq) + " requests · ");
  const b = document.createElement("b");
  b.textContent = fmt(tmat) + " matches";
  line.append(b, " 💜");
}

async function pollStats() {
  if (document.hidden) return;
  try {
    stats = await invoke("fetch_stats");
    const me = stats.nodes.find((n) => n.name === stats.you);
    if (me) {
      lifetime = { requests: me.requests.all, matches: me.matches.all };
      renderCounters();
      checkMilestone(lifetime.matches);
    }
    $("#board-when").textContent = "live";
    renderBoard();
  } catch {
    $("#board-when").textContent = "server unreachable";
  }
}

$("#tabs").addEventListener("click", (e) => {
  const btn = e.target.closest("button");
  if (!btn) return;
  tab = btn.dataset.w;
  document.querySelectorAll("#tabs button").forEach((b) => b.classList.toggle("on", b === btn));
  renderBoard();
});

// ---------- node events ----------
function onNodeEvent(ev) {
  switch (ev.type) {
    case "job_started":
      viz.spawn(ev.id, ev.host);
      break;
    case "job_active":
      viz.activate(ev.id);
      break;
    case "job_done":
      viz.setOutcome(ev.id, ev.outcome);
      session.jobs += 1;
      if (ev.method === "match-v5.match" && ev.outcome === "ok") {
        lifetime.matches += 1; // optimistic; stats poll trues it up
        bump("#c-matches");
      }
      lifetime.requests += 1;
      renderCounters();
      break;
    case "job_uploaded":
      viz.resolve(ev.id);
      break;
    case "connected":
      setStatus("online", "connected");
      pollStats();
      break;
    case "disconnected":
      setStatus("offline", "reconnecting…");
      break;
    case "key_bad":
      setStatus("paused", "key expired");
      $("#keybar-msg").textContent =
        "💔 Riot expired your API key (dev keys last 24 h). Paste a fresh one and we're back in business:";
      $("#keybar").classList.add("show");
      break;
    case "key_ok":
      setStatus("online", "connected");
      $("#keybar").classList.remove("show");
      $("#keybar-input").value = "";
      toast("back to work 💪 thanks!");
      break;
    case "protocol_mismatch":
      $("#overlay-msg").textContent = ev.message;
      $("#overlay").classList.add("show");
      break;
    case "stopped":
      setStatus("offline", "stopped");
      break;
  }
}

// ---------- key banner ----------
$("#keybar-save").addEventListener("click", async () => {
  const key = $("#keybar-input").value.trim();
  if (!key) return;
  try {
    await invoke("set_key", { key });
    $("#keybar-msg").textContent = "🤞 key saved — resuming as soon as Riot accepts it…";
  } catch (e) {
    toast("could not save key: " + e);
  }
});

// ---------- enrollment ----------
$("#e-go").addEventListener("click", async () => {
  const btn = $("#e-go");
  const err = $("#e-err");
  err.textContent = "";
  const server = $("#e-server").value.trim();
  const name = $("#e-name").value.trim().toLowerCase();
  const invite = $("#e-invite").value.trim();
  const riotKey = $("#e-key").value.trim();
  if (!server || !name || !invite || !riotKey) {
    err.textContent = "all four fields are needed";
    return;
  }
  btn.disabled = true;
  btn.textContent = "Joining…";
  try {
    ui = await invoke("enroll", { server, name, invite, riotKey });
    $("#enroll").classList.remove("show");
    startMain();
    toast(`welcome to the crew, ${ui.name} 💜`);
  } catch (e) {
    err.textContent = String(e);
  } finally {
    btn.disabled = false;
    btn.textContent = "Start helping 🚀";
  }
});

// ---------- boot ----------
function startMain() {
  $("#who").textContent = ui.name;
  session.jobs = Number(ui.snapshot.completed);
  renderCounters();
  if (ui.snapshot.key_bad) {
    setStatus("paused", "key expired");
    $("#keybar").classList.add("show");
  } else if (ui.snapshot.connected) {
    setStatus("online", "connected");
  } else {
    setStatus("offline", "connecting…");
  }
  rotateMsg();
  setInterval(rotateMsg, 14000);
  pollStats();
  setInterval(pollStats, 45000);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) pollStats(); });
}

(async function init() {
  await listen("node", (e) => onNodeEvent(e.payload));
  ui = await invoke("get_state");
  if (!ui.enrolled) {
    $("#enroll").classList.add("show");
  } else {
    startMain();
  }
})();
