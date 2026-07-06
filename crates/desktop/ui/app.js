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
// Job events arrive in bursts over IPC; a DOM write per event forces layout
// between canvas frames. Handlers only bump the numbers and set a dirty
// flag — the viz loop applies flushDom() once per frame.
const dom = { counters: false, matchPulse: false };
function markCounters(matchPulse) {
  dom.counters = true;
  if (matchPulse) dom.matchPulse = true;
  viz.wake();
}
function flushDom() {
  if (!dom.counters) return;
  dom.counters = false;
  renderCounters();
  if (dom.matchPulse) {
    dom.matchPulse = false;
    pulse($("#c-matches"));
  }
}
// Scale pulse via the Web Animations API: compositor-driven, restart-safe.
function pulse(el, scale = 1.06) {
  el.animate(
    [{ transform: "scale(1)" }, { transform: `scale(${scale})` }, { transform: "scale(1)" }],
    { duration: 240, easing: "ease-out" }
  );
}
let vaultDigits = 0;
function renderCounters() {
  $("#c-matches").textContent = fmt(lifetime.matches);
  $("#c-requests").textContent = fmt(lifetime.requests);
  $("#c-session").textContent = fmt(session.jobs);
  const vn = fmt(session.jobs);
  $("#vault-n").textContent = vn;
  if (vn.length !== vaultDigits) {
    vaultDigits = vn.length; // vault pill grew: re-measure the fly target
    viz.layoutChanged();
  }
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

// ---------- easter egg ----------
// Ten rapid clicks anywhere (each within 500 ms of the last) pop the
// milestone confetti on demand. Resets on trigger, so the restless can
// keep going.
let eggClicks = 0;
let eggLast = 0;
window.addEventListener("pointerdown", () => {
  const now = performance.now();
  eggClicks = now - eggLast < 500 ? eggClicks + 1 : 1;
  eggLast = now;
  if (eggClicks >= 10) {
    eggClicks = 0;
    viz.confetti();
    toast("🎉 you found the confetti stash — carry on");
  }
});

// ---------- canvas visualization ----------
// One orb per job, following its real lifecycle:
//   wait   — pulled from the server, queued on the local rate limiter:
//            falls in, settles into a hover band, drifts lazily, 50% dim.
//   active — the Riot request is on the wire (and, after job_done, the
//            result is uploading): bright, lively drift.
//   fly    — uploaded OK: smooth glide into the vault.
//   fade   — uploaded but not a stored body (404 grey / failed red /
//            key_rejected amber), or a safety timeout: still fade-out.
//
// Motion: every orb is a spring-damper chasing a wander target near its
// home point. wait<->active blend through an "energy" scalar that scales
// wander range, re-pick cadence and brightness — never the rate of any
// oscillator — so activations don't read as the animation changing speed.
//
// Frame pipeline, ordered so nothing forces layout mid-frame:
//   1. re-measure the vault target if stale (DOM reads while layout clean)
//   2. drain queued node events (IPC handlers never touch the sim or DOM)
//   3. flushDom() — the batched counter/pulse writes
//   4. advance the sim in fixed 1/60 s steps, render with interpolation
// Fixed timestepping means a slow frame drops time instead of warping the
// animation speed. The loop still sleeps whenever nothing is alive.
const viz = (() => {
  const canvas = $("#viz");
  const ctx = canvas.getContext("2d");
  let W = 0, H = 0, dpr = 1;
  const orbs = new Map(); // job id -> orb
  let parts = []; // particles
  const queue = []; // node events awaiting the next frame
  let raf = null, lastT = 0, acc = 0, delivered = 0;
  const STEP = 1 / 60; // sim timestep, seconds

  const FADE_COLORS = { not_found: "#6b7194", failed: "#f87171", key_rejected: "#ffb454" };

  // Fly-to-vault target, cached: measured on resize / vault growth / wake,
  // never per frame (getBoundingClientRect in the loop forces layout).
  let vaultPos = { x: 0, y: 0 };
  let vaultStale = true;

  function resize() {
    const r = canvas.parentElement.getBoundingClientRect();
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    W = Math.max(1, r.width);
    H = Math.max(1, r.height);
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    vaultStale = true;
  }
  new ResizeObserver(resize).observe(canvas.parentElement);

  function refreshVault() {
    vaultStale = false;
    const c = canvas.getBoundingClientRect();
    const r = $("#vault").getBoundingClientRect();
    // The delivery pulse scales the pill about its center, so the center
    // stays a stable target even mid-bump.
    vaultPos = { x: r.left - c.left + r.width / 2, y: r.top - c.top + r.height / 2 };
  }

  // Handlers enqueue; the frame loop applies everything in one batch so a
  // burst of IPC events can't interleave DOM work with rendering.
  function onEvent(ev) {
    queue.push(ev);
    if (!document.hidden) wake();
    else if (queue.length >= 512) drain(); // hidden: keep state, don't hoard
  }

  function drain() {
    for (const ev of queue) {
      const o = orbs.get(ev.id);
      switch (ev.type) {
        case "job_started":
          spawn(ev.id, ev.host);
          break;
        case "job_active": // limiter permit acquired: request on the wire
          if (o && o.mode === "wait") setMode(o, "active");
          break;
        case "job_done": // Riot answered; the result is now uploading
          if (o) {
            o.outcome = ev.outcome;
            o.uploadT = 0;
            if (o.mode === "wait") setMode(o, "active"); // job_active lost
          }
          break;
        case "job_uploaded": // result reached the server
          if (o) resolveOrb(ev.id, o);
          break;
      }
    }
    queue.length = 0;
  }

  function spawn(id, host) {
    if (orbs.size > 90) return; // sanity cap; counters still tick
    legendAdd(host);
    const x = 20 + Math.random() * (W - 40);
    const hy = H * (0.30 + Math.random() * 0.38);
    const k = 16 + Math.random() * 10; // per-orb spring stiffness
    orbs.set(id, {
      x, y: -8, px: x, py: -8, // current + previous sim position
      vx: 0, vy: 0,
      hx: x, hy, // home point in the hover band
      tx: x, ty: hy, // wander target; starts at home so the orb falls in
      repick: 1 + Math.random(), // settle first, wander after
      k, c: 2 * Math.sqrt(k) * 0.75, // underdamped: lands with a soft bounce
      phase: Math.random() * Math.PI * 2, // brightness pulse, constant rate
      r: 4.5 + Math.random() * 2.5,
      color: colorOf(host),
      flyColor: colorOf(host, 74),
      mode: "wait",
      e: 0, // energy: 0 = queued, 1 = on the wire
      t: 0, // seconds in current mode
      uploadT: null, // seconds since job_done (upload in flight)
      outcome: null,
      fadeColor: FADE_COLORS.not_found,
      fx: 0, fy: 0, // fly start position
    });
  }

  function setMode(o, mode) {
    o.mode = mode;
    o.t = 0;
  }

  function resolveOrb(id, o) {
    if (o.mode === "fly" || o.mode === "fade") return;
    if (o.outcome === "ok") {
      o.fx = o.x;
      o.fy = o.y;
      setMode(o, "fly");
    } else {
      fadeOut(o, FADE_COLORS[o.outcome] || FADE_COLORS.not_found);
    }
  }

  function fadeOut(o, color) {
    o.fadeColor = color;
    setMode(o, "fade");
  }

  function drawOrb(x, y, r, color, alpha) {
    ctx.beginPath();
    ctx.arc(x, y, r, 0, 7);
    ctx.fillStyle = color;
    ctx.globalAlpha = Math.max(0, Math.min(1, alpha));
    ctx.fill();
    ctx.globalAlpha = 1;
  }

  function pop(x, y, color, n = 10) {
    for (let i = 0; i < n; i++) {
      const a = Math.random() * Math.PI * 2;
      const v = 30 + Math.random() * 90;
      parts.push({
        x, y, px: x, py: y,
        vx: Math.cos(a) * v, vy: Math.sin(a) * v,
        life: 0.55, t: 0, color, r: 1.5 + Math.random() * 1.8,
      });
    }
  }

  function confetti() {
    const colors = ["#8b7cff", "#ff7a9e", "#34d3bd", "#ffb454", "#4ade80"];
    for (let i = 0; i < 130; i++) {
      const x = Math.random() * W;
      parts.push({
        x, y: -10, px: x, py: -10,
        vx: (Math.random() - 0.5) * 60, vy: 60 + Math.random() * 120,
        life: 2.2 + Math.random() * 1.2, t: 0,
        color: colors[i % colors.length], r: 2 + Math.random() * 2.5,
      });
    }
    wake();
  }

  function simStep() {
    for (const [id, o] of orbs) {
      o.px = o.x;
      o.py = o.y;
      o.t += STEP;
      if (o.mode === "wait" || o.mode === "active") {
        o.e += ((o.mode === "active" ? 1 : 0) - o.e) * (STEP * 6);
        o.phase += STEP * 3;
        if (o.uploadT !== null) o.uploadT += STEP;

        // Wander: re-pick a target near home, sooner and wider when active.
        o.repick -= STEP;
        if (o.repick <= 0) {
          o.repick = (1.5 - 1.0 * o.e) * (0.7 + 0.6 * Math.random());
          const range = 3 + 7 * o.e;
          o.tx = o.hx + (Math.random() * 2 - 1) * range * 1.7;
          o.ty = o.hy + (Math.random() * 2 - 1) * range;
        }
        // Spring-damper chase (semi-implicit Euler, stable at this k/STEP).
        o.vx += ((o.tx - o.x) * o.k - o.vx * o.c) * STEP;
        o.vy += ((o.ty - o.y) * o.k - o.vy * o.c) * STEP;
        o.x += o.vx * STEP;
        o.y += o.vy * STEP;

        // Safety nets for lost events; server-side leases make these moot.
        if (o.mode === "wait" && o.t > 160) {
          fadeOut(o, FADE_COLORS.not_found); // lease long expired
        } else if (o.uploadT !== null && o.uploadT > 20) {
          resolveOrb(id, o); // job_uploaded got lost; resolve by outcome
        } else if (o.mode === "active" && o.t > 180) {
          fadeOut(o, FADE_COLORS.not_found); // stuck in retries/cooldowns
        }
      } else if (o.mode === "fly") {
        const s = 1 - Math.pow(1 - Math.min(1, o.t / 0.6), 3);
        o.x = o.fx + (vaultPos.x - o.fx) * s;
        o.y = o.fy + (vaultPos.y - o.fy) * s;
        if (o.t >= 0.6) {
          orbs.delete(id);
          pop(vaultPos.x, vaultPos.y, o.flyColor, 8);
          delivered += 1;
        }
      } else if (o.t >= 0.5) {
        orbs.delete(id); // fade finished
      }
    }

    parts = parts.filter((p) => {
      p.t += STEP;
      if (p.t >= p.life) return false;
      p.px = p.x;
      p.py = p.y;
      p.x += p.vx * STEP;
      p.y += p.vy * STEP;
      p.vy += 140 * STEP;
      return true;
    });
  }

  // `a` in [0,1): how far past the last sim step this frame lands.
  function render(a) {
    ctx.clearRect(0, 0, W, H);
    for (const o of orbs.values()) {
      const x = o.px + (o.x - o.px) * a;
      const y = o.py + (o.y - o.py) * a;
      if (o.mode === "fly") {
        const s = Math.min(1, o.t / 0.6);
        drawOrb(x, y, o.r * (1 - 0.5 * s), o.flyColor, 1);
      } else if (o.mode === "fade") {
        drawOrb(x, y, o.r, o.fadeColor, (1 - Math.min(1, o.t / 0.5)) * 0.8);
      } else {
        // Queued: ~50% of the active brightness. Active: bright pulse.
        const aWait = 0.4 + 0.05 * Math.sin(o.phase);
        const aActive = 0.75 + 0.25 * Math.sin(o.phase * 1.7);
        drawOrb(x, y, o.r, o.color, aWait * (1 - o.e) + aActive * o.e);
      }
    }
    for (const p of parts) {
      ctx.beginPath();
      ctx.arc(p.px + (p.x - p.px) * a, p.py + (p.y - p.py) * a, p.r, 0, 7);
      ctx.fillStyle = p.color;
      ctx.globalAlpha = 1 - p.t / p.life;
      ctx.fill();
      ctx.globalAlpha = 1;
    }
  }

  function frame(now) {
    raf = null;
    acc += Math.min(0.1, Math.max(0, (now - lastT) / 1000));
    lastT = now;
    if (vaultStale) refreshVault();
    drain();
    flushDom();
    let n = 0;
    while (acc >= STEP && n < 8) {
      simStep();
      acc -= STEP;
      n += 1;
    }
    if (n === 8) acc = 0; // hopelessly behind: drop the debt
    if (delivered) {
      pulse($("#vault"), 1.12);
      delivered = 0;
    }
    render(acc / STEP);
    if ((orbs.size || parts.length || queue.length) && !document.hidden) {
      raf = requestAnimationFrame(frame);
    } else {
      ctx.clearRect(0, 0, W, H);
    }
  }

  function wake() {
    if (!raf && !document.hidden) {
      lastT = performance.now();
      acc = 0;
      vaultStale = true; // layout may have shifted while asleep
      raf = requestAnimationFrame(frame);
    }
  }

  document.addEventListener("visibilitychange", () => { if (!document.hidden) wake(); });
  resize();
  return { onEvent, confetti, wake, layoutChanged: () => { vaultStale = true; } };
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
    case "job_active":
    case "job_uploaded":
      viz.onEvent(ev);
      break;
    case "job_done": {
      viz.onEvent(ev);
      session.jobs += 1;
      lifetime.requests += 1;
      const isMatch = ev.method === "match-v5.match" && ev.outcome === "ok";
      if (isMatch) lifetime.matches += 1; // optimistic; stats poll trues it up
      markCounters(isMatch);
      break;
    }
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

// The window is created hidden (tauri.conf.json) so the user never sees
// the webview's white pre-paint frame; we show it once real content has
// painted. The timeout is a hard floor: whatever goes wrong above, the
// window must never stay invisible.
let revealed = false;
function reveal() {
  if (revealed) return;
  revealed = true;
  const w = window.__TAURI__.window.getCurrentWindow();
  w.show().then(() => w.setFocus()).catch(() => {});
}
setTimeout(reveal, 2000);

(async function init() {
  await listen("node", (e) => onNodeEvent(e.payload));
  ui = await invoke("get_state");
  if (!ui.enrolled) {
    $("#enroll").classList.add("show");
  } else {
    startMain();
  }
  // Two rAFs: the first fires before this frame paints, the second after.
  requestAnimationFrame(() => requestAnimationFrame(reveal));
})();
