let DATA       = null
let CONFIG     = null
let CONFIGURED = false
let ACTIVE_TAB = "dashboard"
let _pollTimer = null

const statusEl   = () => document.getElementById("status")
const topFilters = () => document.getElementById("topFilters")

/* ========================================================
   HELPERS
======================================================== */

function yearBucket(y){
  const year = parseInt(y || "0", 10)
  if (!year)        return ""
  if (year >= 2020) return "2020s"
  if (year >= 2010) return "2010s"
  if (year >= 2000) return "2000s"
  if (year >= 1990) return "1990s"
  return "older"
}

function pill(text){
  return `<span class="text-xs px-2 py-1 rounded bg-zinc-800 border border-zinc-700 text-zinc-200">${text}</span>`
}

// Tabs that use a group-name dropdown instead of year/sort
const GROUP_TABS = new Set(["franchises", "directors", "actors"])

/* ========================================================
   API
======================================================== */

async function api(path, method = "GET", body = null){
  const opts = { method, headers: {} }
  if (body){
    opts.headers["Content-Type"] = "application/json"
    opts.body = JSON.stringify(body)
  }
  const r = await fetch(path, opts)
  return r.json()
}

/* ========================================================
   DATA LOADING
======================================================== */

async function loadConfig(){
  CONFIG = await api("/api/config")
}

async function loadStatus(){
  const s = await api("/api/config/status")
  CONFIGURED = !!s.configured
}

async function loadResults(){
  statusEl().textContent = "Loading…"
  const data = await api("/api/results")

  if (data.scanning){
    statusEl().textContent = "Scan in progress…"
    startPolling()
    return
  }

  DATA = data
  statusEl().textContent = `Loaded • ${DATA.generated_at || ""}`
  render()
}

/* ========================================================
   SCAN + PROGRESS POLLING
======================================================== */

async function rescan(){
  if (!CONFIGURED){ alert("Complete setup first."); return }
  const res = await api("/api/scan", "POST")
  if (!res.ok){ alert(res.error || "Could not start scan"); return }
  startPolling()
}

function startPolling(){
  stopPolling()
  renderScanProgress(null)
  _pollTimer = setInterval(pollScanStatus, 1500)
}

function stopPolling(){
  if (_pollTimer){ clearInterval(_pollTimer); _pollTimer = null }
}

async function pollScanStatus(){
  let status
  try { status = await api("/api/scan/status") } catch(e){ return }

  renderScanProgress(status)

  if (!status.running){
    stopPolling()
    if (status.error){
      statusEl().textContent = `Scan failed: ${status.error}`
      document.getElementById("scanProgress")?.remove()
      return
    }
    const data = await api("/api/results")
    DATA = data
    statusEl().textContent = `Rescanned • ${DATA.generated_at || ""}`
    document.getElementById("scanProgress")?.remove()
    render()
  }
}

function renderScanProgress(status){
  let el = document.getElementById("scanProgress")
  if (!el){
    el = document.createElement("div")
    el.id = "scanProgress"
    el.style.cssText = "position:fixed;bottom:1.5rem;right:1.5rem;width:320px;background:#18181b;border:1px solid #27272a;border-radius:10px;padding:1rem;z-index:999;box-shadow:0 4px 24px #000a"
    document.body.appendChild(el)
  }

  if (!status || !status.running){
    el.innerHTML = `<div style="display:flex;align-items:center;gap:.6rem;color:#a1a1aa;font-size:.85rem">
      <div class="spin" style="width:14px;height:14px;border:2px solid #3f3f46;border-top-color:#22c55e;border-radius:50%"></div>
      Starting scan…
    </div>`
    return
  }

  const pct = status.step_total ? Math.round((status.step_index / status.step_total) * 100) : 0

  el.innerHTML = `
  <style>.spin{animation:spin .8s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}</style>
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.5rem">
    <span style="font-size:.78rem;font-weight:600;color:#d4d4d8">Scanning…</span>
    <span style="font-size:.75rem;color:#71717a">${status.step_index}/${status.step_total}</span>
  </div>
  <div style="height:4px;background:#27272a;border-radius:4px;margin-bottom:.6rem;overflow:hidden">
    <div style="height:4px;width:${pct}%;background:#22c55e;border-radius:4px;transition:width .4s ease"></div>
  </div>
  <div style="font-size:.78rem;color:#a1a1aa;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
    ${status.step || ""}${status.detail ? ` — ${status.detail}` : ""}
  </div>`

  statusEl().textContent = `${status.step || "Scanning…"} (${pct}%)`
}

/* ========================================================
   TOP FILTER BAR  (FIX #3 — tab-aware)
======================================================== */

/**
 * Rebuild the filter bar depending on which tab is active.
 * - GROUP_TABS (franchises/directors/actors): show a name-picker dropdown + sort
 * - Flat tabs (classics/suggestions/wishlist/...): show year filter + sort
 * - dashboard / config / metadata tabs: hide entirely
 */
function updateFilterBar(){
  const bar = topFilters()

  const hiddenTabs = new Set(["dashboard","config","notmdb","nomatch"])
  if (hiddenTabs.has(ACTIVE_TAB)){
    bar.style.display = "none"
    return
  }

  bar.style.display = "flex"

  if (GROUP_TABS.has(ACTIVE_TAB)){
    // Preserve whatever the user has already selected before rebuilding
    const prevGroup = document.getElementById("groupFilter")?.value || ""
    const prevSort  = document.getElementById("sort")?.value || "popularity"

    const groups = getGroupsForTab(ACTIVE_TAB).filter(g => (g.missing || []).length > 0)
    const options = groups.map(g => {
      const name = g.name || ""
      const sel  = name === prevGroup ? " selected" : ""
      return `<option value="${name}"${sel}>${name}</option>`
    }).join("")

    bar.innerHTML = `
      <select id="groupFilter"
        class="bg-zinc-900 border border-zinc-800 rounded px-3 py-2 min-w-[220px] max-w-xs">
        <option value="">All ${ACTIVE_TAB}</option>
        ${options}
      </select>
      <select id="sort" class="bg-zinc-900 border border-zinc-800 rounded px-3 py-2">
        <option value="popularity"${prevSort==="popularity"?" selected":""}>Sort: popularity</option>
        <option value="rating"${prevSort==="rating"?" selected":""}>Sort: rating</option>
        <option value="votes"${prevSort==="votes"?" selected":""}>Sort: votes</option>
        <option value="year"${prevSort==="year"?" selected":""}>Sort: year</option>
        <option value="title"${prevSort==="title"?" selected":""}>Sort: title</option>
      </select>`
  } else {
    const prevSearch = document.getElementById("search")?.value || ""
    const prevYear   = document.getElementById("yearFilter")?.value || ""
    const prevSort   = document.getElementById("sort")?.value || "popularity"

    bar.innerHTML = `
      <input id="search" class="bg-zinc-900 border border-zinc-800 rounded px-3 py-2 w-80"
             placeholder="Search title..." value="${prevSearch}"/>
      <select id="yearFilter" class="bg-zinc-900 border border-zinc-800 rounded px-3 py-2">
        <option value=""${prevYear===""?" selected":""}>All years</option>
        <option value="2020s"${prevYear==="2020s"?" selected":""}>2020s</option>
        <option value="2010s"${prevYear==="2010s"?" selected":""}>2010s</option>
        <option value="2000s"${prevYear==="2000s"?" selected":""}>2000s</option>
        <option value="1990s"${prevYear==="1990s"?" selected":""}>1990s</option>
        <option value="older"${prevYear==="older"?" selected":""}>Older</option>
      </select>
      <select id="sort" class="bg-zinc-900 border border-zinc-800 rounded px-3 py-2">
        <option value="popularity"${prevSort==="popularity"?" selected":""}>Sort: popularity</option>
        <option value="rating"${prevSort==="rating"?" selected":""}>Sort: rating</option>
        <option value="votes"${prevSort==="votes"?" selected":""}>Sort: votes</option>
        <option value="year"${prevSort==="year"?" selected":""}>Sort: year</option>
        <option value="title"${prevSort==="title"?" selected":""}>Sort: title</option>
      </select>`
  }
}

function getGroupsForTab(tab){
  if (tab === "franchises") return DATA.franchises || []
  if (tab === "directors")  return DATA.directors  || []
  if (tab === "actors")     return DATA.actors      || []
  return []
}

function getGroupFilter(){
  return document.getElementById("groupFilter")?.value || ""
}

/* ========================================================
   FILTERS / SORT
======================================================== */

function getFilters(){
  return {
    search: (document.getElementById("search")?.value || "").toLowerCase().trim(),
    year:   document.getElementById("yearFilter")?.value || "",
    sort:   document.getElementById("sort")?.value || "popularity",
  }
}

function applyFilters(list, groupName = ""){
  const { search, year, sort } = getFilters()

  let out = list.filter(m => {
    if (search){
      const inTitle = (m.title || "").toLowerCase().includes(search)
      const inGroup = groupName.toLowerCase().includes(search)
      if (!inTitle && !inGroup) return false
    }
    if (year && yearBucket(m.year) !== year) return false
    return true
  })

  out.sort((a, b) => {
    if (sort === "title")  return (a.title  || "").localeCompare(b.title  || "")
    if (sort === "year")   return parseInt(b.year   || 0) - parseInt(a.year   || 0)
    if (sort === "rating") return (b.rating || 0) - (a.rating || 0)
    if (sort === "votes")  return (b.votes  || 0) - (a.votes  || 0)
    return (b.popularity || 0) - (a.popularity || 0)
  })

  return out
}

/* ========================================================
   MOVIE CARD
======================================================== */

function movieCard(m){
  const poster = m.poster
    ? `<img src="${m.poster}" class="w-24 h-36 object-cover rounded bg-zinc-800" loading="lazy"/>`
    : `<div class="w-24 h-36 rounded bg-zinc-800 flex items-center justify-center text-xs text-zinc-400">No poster</div>`

  const radarrBtn = CONFIG?.RADARR?.RADARR_ENABLED
    ? `<button onclick="addToRadarr(${m.tmdb},'${(m.title||'').replace(/'/g,"\\'")}',this)"
         class="text-xs px-2 py-1 rounded bg-emerald-700 hover:bg-emerald-600 text-white">+ Radarr</button>`
    : ""

  const wishlistBtn = m.wishlist
    ? `<button onclick="removeWishlist(${m.tmdb},this)"
         class="text-xs px-2 py-1 rounded bg-yellow-700 hover:bg-yellow-600 text-white">★ Wishlisted</button>`
    : `<button onclick="addWishlist(${m.tmdb},this)"
         class="text-xs px-2 py-1 rounded bg-zinc-700 hover:bg-zinc-600 text-white">☆ Wishlist</button>`

  return `
  <div class="flex gap-3 bg-zinc-900 border border-zinc-800 rounded p-3">
    ${poster}
    <div class="flex-1 min-w-0">
      <div class="font-semibold truncate">
        ${m.title || "Untitled"}
        ${m.year ? `<span class="text-zinc-400 font-normal">(${m.year})</span>` : ""}
      </div>
      <div class="text-xs text-zinc-400 mt-1 flex flex-wrap gap-2">
        ${m.tmdb  ? pill(`tmdb:${m.tmdb}`) : ""}
        ${pill(`pop ${Math.round(m.popularity || 0)}`)}
        ${pill(`⭐ ${m.rating || 0}`)}
        ${pill(`votes ${m.votes || 0}`)}
      </div>
      <div class="flex gap-2 mt-2">${wishlistBtn}${radarrBtn}</div>
    </div>
  </div>`
}

/* ========================================================
   WISHLIST / RADARR ACTIONS
======================================================== */

async function addWishlist(tmdb, btn){
  await api("/api/wishlist/add", "POST", { tmdb })
  btn.textContent = "★ Wishlisted"
  btn.className   = "text-xs px-2 py-1 rounded bg-yellow-700 hover:bg-yellow-600 text-white"
  btn.onclick     = () => removeWishlist(tmdb, btn)
}

async function removeWishlist(tmdb, btn){
  await api("/api/wishlist/remove", "POST", { tmdb })
  btn.textContent = "☆ Wishlist"
  btn.className   = "text-xs px-2 py-1 rounded bg-zinc-700 hover:bg-zinc-600 text-white"
  btn.onclick     = () => addWishlist(tmdb, btn)
}

async function addToRadarr(tmdb, title, btn){
  btn.disabled    = true
  btn.textContent = "Adding…"
  const res = await api("/api/radarr/add", "POST", { tmdb, title })
  if (res.ok){
    btn.textContent = "✓ Added"
    btn.className   = "text-xs px-2 py-1 rounded bg-zinc-600 text-white cursor-default"
  } else {
    btn.textContent = "✗ Error"
    btn.disabled    = false
  }
}

/* ========================================================
   CHART REGISTRY
======================================================== */

const _charts = {}
function destroyChart(id){ if (_charts[id]){ _charts[id].destroy(); delete _charts[id] } }
function registerChart(id, inst){ _charts[id] = inst }

/* ========================================================
   DASHBOARD
======================================================== */

function scoreCard(label, value, color){
  const v = parseFloat(value) || 0
  return `
  <div style="background:#18181b;border:1px solid #27272a;border-radius:10px;padding:1.25rem">
    <div style="font-size:.7rem;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:#71717a;margin-bottom:.4rem">${label}</div>
    <div style="font-size:2.1rem;font-weight:700;line-height:1;letter-spacing:-1px;color:${color}">${v}%</div>
    <div style="height:4px;background:#27272a;border-radius:4px;margin-top:.7rem;overflow:hidden">
      <div style="height:4px;width:${v}%;background:${color};border-radius:4px;transition:width .8s ease"></div>
    </div>
  </div>`
}

function renderDashboard(){
  const c = document.getElementById("content")
  const s = DATA.scores || {}
  const p = DATA.plex   || {}

  // FIX #1: always read from DATA._ignored_franchises which is kept in sync
  const ignoredFranchises = new Set(DATA._ignored_franchises || [])

  let fComplete = 0, fMissingOne = 0, fMissingMore = 0
  ;(DATA.franchises || []).filter(f => !ignoredFranchises.has(f.name)).forEach(f => {
    const n = (f.missing || []).length
    if      (n === 0) fComplete++
    else if (n === 1) fMissingOne++
    else              fMissingMore++
  })

  const classicsMiss  = (DATA.classics || []).length
  const classicsTotal = Math.round(classicsMiss / (1 - (s.classics_proxy_pct || 0) / 100)) || classicsMiss
  const classicsHave  = Math.max(0, classicsTotal - classicsMiss)

  const noGuid   = p.no_tmdb_guid || 0
  const noMatch  = (DATA.tmdb_not_found || []).length
  const okMovies = Math.max(0, (p.indexed_tmdb || 0) - noMatch)

  const dBuckets = { "0": 0, "1–2": 0, "3–5": 0, "6–10": 0, "10+": 0 }
  ;(DATA.directors || []).forEach(d => {
    const n = (d.missing || []).length
    if      (n === 0) dBuckets["0"]++
    else if (n <= 2)  dBuckets["1–2"]++
    else if (n <= 5)  dBuckets["3–5"]++
    else if (n <= 10) dBuckets["6–10"]++
    else              dBuckets["10+"]++
  })

  const topActors = (DATA.charts?.top_actors || []).slice(0, 10)

  const card = inner => `<div style="background:#18181b;border:1px solid #27272a;border-radius:10px;padding:1.25rem">${inner}</div>`
  const sec  = t     => `<div style="font-size:.65rem;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:#71717a;margin-bottom:.9rem">${t}</div>`
  const dot  = col   => `<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:${col};margin-right:7px;flex-shrink:0"></span>`
  const leg  = (col, label, val) => `<div style="display:flex;align-items:center;margin-bottom:5px;font-size:.78rem">${dot(col)}<span style="color:#a1a1aa;flex:1">${label}</span><b>${val}</b></div>`
  const srow = (label, val, col = "") => `<div style="display:flex;justify-content:space-between;padding:.38rem 0;border-bottom:1px solid #27272a;font-size:.84rem"><span style="color:#a1a1aa">${label}</span><b ${col ? `style="color:${col}"` : ""}>${val}</b></div>`

  c.innerHTML = `
  <style>
    .db-grid-4  { display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin-bottom:1.1rem }
    .db-grid-3  { display:grid;grid-template-columns:1fr 1fr 1fr;gap:1rem;margin-bottom:1.1rem }
    .db-grid-31 { display:grid;grid-template-columns:2fr 1fr 1fr;gap:1rem }
    @media(max-width:900px){ .db-grid-4,.db-grid-3,.db-grid-31{ grid-template-columns:1fr 1fr } }
    @media(max-width:600px){ .db-grid-4,.db-grid-3,.db-grid-31{ grid-template-columns:1fr } }
  </style>

  <div class="db-grid-4">
    ${scoreCard("Franchise Completion", s.franchise_completion_pct ?? 0, "#22c55e")}
    ${scoreCard("Directors Score",      s.directors_proxy_pct      ?? 0, "#3b82f6")}
    ${scoreCard("Classics Coverage",    s.classics_proxy_pct       ?? 0, "#f59e0b")}
    ${scoreCard("Global Cinema Score",  s.global_cinema_score      ?? 0, "#a855f7")}
  </div>

  <div class="db-grid-3">
    ${card(`${sec("Franchise Status")}
      <canvas id="chartFranchise" height="190"></canvas>
      <div style="margin-top:.9rem">
        ${leg("#22c55e","Complete", fComplete)}
        ${leg("#f59e0b","Missing 1", fMissingOne)}
        ${leg("#ef4444","Missing 2+", fMissingMore)}
      </div>`)}
    ${card(`${sec("Classics Coverage")}
      <canvas id="chartClassics" height="190"></canvas>
      <div style="margin-top:.9rem">
        ${leg("#a855f7","In library", classicsHave)}
        ${leg("#3f3f46","Missing", classicsMiss)}
      </div>`)}
    ${card(`${sec("Metadata Health")}
      <canvas id="chartMeta" height="190"></canvas>
      <div style="margin-top:.9rem">
        ${leg("#22c55e","Valid TMDB", okMovies)}
        ${leg("#f59e0b","No GUID", noGuid)}
        ${leg("#ef4444","No Match", noMatch)}
      </div>`)}
  </div>

  <div class="db-grid-31">
    ${card(`${sec("Top 10 Actors in Library")}<canvas id="chartActors" height="210"></canvas>`)}
    ${card(`${sec("Directors — Missing Films")}<canvas id="chartDirs" height="210"></canvas>`)}
    ${card(`${sec("Library Stats")}
      ${srow("Scanned items",      p.scanned_items ?? 0)}
      ${srow("Indexed TMDB",       p.indexed_tmdb  ?? 0)}
      ${srow("Shorts skipped",     p.skipped_short ?? 0)}
      ${srow("No TMDB GUID",       noGuid,  noGuid  ? "#f59e0b" : "")}
      ${srow("TMDB no match",      noMatch, noMatch ? "#ef4444" : "")}
      ${srow("Franchises tracked", (DATA.franchises || []).filter(f => !ignoredFranchises.has(f.name)).length)}
      ${srow("Directors tracked",  (DATA.directors  || []).length)}
      ${srow("Wishlist",           (DATA.wishlist   || []).length)}
    `)}
  </div>`

  requestAnimationFrame(() => {
    Chart.defaults.color       = "#71717a"
    Chart.defaults.font.family = "ui-monospace,monospace"

    const doughnut = (labels, data, colors) => ({
      type: "doughnut",
      data: { labels, datasets: [{ data, backgroundColor: colors, borderColor: "#18181b", borderWidth: 3, hoverOffset: 8 }] },
      options: {
        cutout: "62%",
        animation: { duration: 700 },
        plugins: {
          legend:  { display: false },
          tooltip: { callbacks: { label: ctx => `${ctx.label}: ${ctx.parsed}` } },
        },
      },
    })

    destroyChart("franchise")
    registerChart("franchise", new Chart(document.getElementById("chartFranchise"),
      doughnut(["Complete","Missing 1","Missing 2+"], [fComplete,fMissingOne,fMissingMore], ["#22c55e","#f59e0b","#ef4444"])
    ))

    destroyChart("classics")
    registerChart("classics", new Chart(document.getElementById("chartClassics"),
      doughnut(["In library","Missing"], [classicsHave,classicsMiss], ["#a855f7","#3f3f46"])
    ))

    destroyChart("meta")
    registerChart("meta", new Chart(document.getElementById("chartMeta"),
      doughnut(["Valid TMDB","No GUID","No Match"], [okMovies,noGuid,noMatch], ["#22c55e","#f59e0b","#ef4444"])
    ))

    destroyChart("actors")
    registerChart("actors", new Chart(document.getElementById("chartActors"), {
      type: "bar",
      data: {
        labels:   topActors.map(a => a.name),
        datasets: [{ data: topActors.map(a => a.count), backgroundColor: topActors.map((_,i) => `hsl(${210+i*8},70%,${55-i*2}%)`), borderRadius: 4, borderSkipped: false }],
      },
      options: {
        indexAxis: "y", animation: { duration: 700 },
        scales: {
          x: { grid: { color: "#27272a" }, ticks: { color: "#71717a", font: { size: 11 } } },
          y: { grid: { display: false },   ticks: { color: "#d4d4d8", font: { size: 11 } } },
        },
        plugins: { legend: { display: false } },
      },
    }))

    destroyChart("dirs")
    registerChart("dirs", new Chart(document.getElementById("chartDirs"), {
      type: "bar",
      data: {
        labels:   Object.keys(dBuckets),
        datasets: [{ data: Object.values(dBuckets), backgroundColor: ["#3f3f46","#3b82f6","#f59e0b","#ef4444","#7f1d1d"], borderRadius: 4, borderSkipped: false }],
      },
      options: {
        animation: { duration: 700 },
        scales: {
          x: { grid: { display: false }, ticks: { color: "#d4d4d8", font: { size: 11 } } },
          y: { grid: { color: "#27272a" }, ticks: { color: "#71717a", font: { size: 11 } } },
        },
        plugins: { legend: { display: false }, tooltip: { callbacks: { title: ctx => `Missing: ${ctx[0].label} films` } } },
      },
    }))
  })
}

/* ========================================================
   GROUPED LIST RENDERER  (franchises / directors / actors)
   FIX #3 — uses groupFilter dropdown instead of year/search
======================================================== */

function renderGroupedList({ groups, nameKey, nameIcon, ignoreHandler, emptyMsg }){
  const c          = document.getElementById("content")
  const groupFilter = getGroupFilter()   // selected name from dropdown, or "" for all
  const sort        = document.getElementById("sort")?.value || "popularity"

  let html = ""

  groups.forEach(g => {
    const name = g[nameKey] || ""

    // If a specific group is selected, skip all others
    if (groupFilter && name !== groupFilter) return

    // Sort the missing list by selected sort
    const sorted = [...(g.missing || [])].sort((a, b) => {
      if (sort === "title")  return (a.title  || "").localeCompare(b.title  || "")
      if (sort === "year")   return parseInt(b.year   || 0) - parseInt(a.year   || 0)
      if (sort === "rating") return (b.rating || 0) - (a.rating || 0)
      if (sort === "votes")  return (b.votes  || 0) - (a.votes  || 0)
      return (b.popularity || 0) - (a.popularity || 0)
    })

    if (!sorted.length) return

    html += `
    <div class="mb-6">
      <div class="flex justify-between items-center mb-2">
        <div class="font-semibold">${nameIcon} ${name}
          ${g.have !== undefined ? `<span class="text-zinc-400 font-normal text-sm">(${g.have}/${g.total})</span>` : ""}
        </div>
        <button onclick="${ignoreHandler}('${name.replace(/'/g,"\\'")}',this)"
          class="text-xs px-2 py-1 rounded bg-zinc-700 hover:bg-red-700 text-zinc-300">Ignore</button>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
        ${sorted.map(movieCard).join("")}
      </div>
    </div>`
  })

  c.innerHTML = html || `<div class="text-zinc-400">${emptyMsg}</div>`
}

/* ========================================================
   FRANCHISES
======================================================== */

function renderFranchises(){
  renderGroupedList({
    groups:        DATA.franchises || [],
    nameKey:       "name",
    nameIcon:      "🎬",
    ignoreHandler: "ignoreFranchise",
    emptyMsg:      "No missing franchise movies 🎉",
  })
}

async function ignoreFranchise(name, btn){
  await api("/api/ignore", "POST", { kind: "franchise", value: name })

  // FIX #1: update DATA in memory immediately so dashboard chart reflects change
  if (!DATA._ignored_franchises) DATA._ignored_franchises = []
  if (!DATA._ignored_franchises.includes(name)) DATA._ignored_franchises.push(name)

  btn.closest(".mb-6").remove()

  // Rebuild the group dropdown to remove the ignored franchise
  updateFilterBar()
}

/* ========================================================
   DIRECTORS
======================================================== */

function renderDirectors(){
  renderGroupedList({
    groups:        DATA.directors || [],
    nameKey:       "name",
    nameIcon:      "🎬",
    ignoreHandler: "ignoreDirector",
    emptyMsg:      "No missing director films found.",
  })
}

async function ignoreDirector(name, btn){
  await api("/api/ignore", "POST", { kind: "director", value: name })
  // Remove from DATA so the dropdown and list update immediately
  DATA.directors = (DATA.directors || []).filter(d => d.name !== name)
  btn.closest(".mb-6").remove()
  updateFilterBar()
}

/* ========================================================
   ACTORS
======================================================== */

function renderActors(){
  renderGroupedList({
    groups:        DATA.actors || [],
    nameKey:       "name",
    nameIcon:      "🎭",
    ignoreHandler: "ignoreActor",
    emptyMsg:      "No actor suggestions found.",
  })
}

async function ignoreActor(name, btn){
  await api("/api/ignore", "POST", { kind: "actor", value: name })
  DATA.actors = (DATA.actors || []).filter(a => a.name !== name)
  btn.closest(".mb-6").remove()
  updateFilterBar()
}

/* ========================================================
   CLASSICS
======================================================== */

function renderClassics(){
  const c    = document.getElementById("content")
  const list = applyFilters(DATA.classics || [])

  if (!list.length){ c.innerHTML = `<div class="text-zinc-400">No missing classics found 🎉</div>`; return }

  c.innerHTML = `
  <div class="mb-3 text-zinc-400 text-sm">${list.length} classic films missing from your library</div>
  <div class="grid grid-cols-1 md:grid-cols-2 gap-3">${list.map(movieCard).join("")}</div>`
}

/* ========================================================
   SUGGESTIONS
======================================================== */

function renderSuggestions(){
  const c    = document.getElementById("content")
  const list = applyFilters(DATA.suggestions || [])

  if (!list.length){ c.innerHTML = `<div class="text-zinc-400">No TMDB suggestions available.</div>`; return }

  c.innerHTML = `
  <div class="mb-3 text-zinc-400 text-sm">${list.length} suggestions from TMDB Top Rated</div>
  <div class="grid grid-cols-1 md:grid-cols-2 gap-3">${list.map(movieCard).join("")}</div>`
}

/* ========================================================
   NO TMDB GUID
======================================================== */

function renderNoTmdb(){
  const c = document.getElementById("content")
  const { search } = getFilters()
  let list = DATA.no_tmdb_guid || []
  if (search) list = list.filter(m => (m.title || "").toLowerCase().includes(search))

  if (!list.length){ c.innerHTML = `<div class="text-zinc-400">All movies have a TMDB GUID 🎉</div>`; return }

  c.innerHTML = `
  <div class="mb-3 text-zinc-400 text-sm">${list.length} movies without a TMDB GUID — fix via Plex → Fix Match → TheMovieDB</div>
  <div class="space-y-2">
    ${list.map(m => `
    <div class="bg-zinc-900 border border-zinc-800 rounded p-3">
      <span class="font-medium">${m.title || "Unknown"}</span>
      ${m.year ? `<span class="text-zinc-400 ml-2">(${m.year})</span>` : ""}
    </div>`).join("")}
  </div>`
}

/* ========================================================
   TMDB NO MATCH
======================================================== */

function renderNoMatch(){
  const c    = document.getElementById("content")
  const list = DATA.tmdb_not_found || []

  if (!list.length){ c.innerHTML = `<div class="text-zinc-400">All TMDB matches resolved 🎉</div>`; return }

  c.innerHTML = `
  <div class="mb-3 text-zinc-400 text-sm">${list.length} movies with invalid TMDB metadata — refresh metadata or fix match in Plex</div>
  <div class="space-y-2">
    ${list.map(m => `<div class="bg-zinc-900 border border-zinc-800 rounded p-3">${pill(`tmdb:${m.tmdb}`)}</div>`).join("")}
  </div>`
}

/* ========================================================
   WISHLIST
======================================================== */

function renderWishlist(){
  const c    = document.getElementById("content")
  const list = applyFilters(DATA.wishlist || [])

  c.innerHTML = list.length
    ? `<div class="grid grid-cols-1 md:grid-cols-2 gap-3">${list.map(movieCard).join("")}</div>`
    : `<div class="text-zinc-400">Wishlist empty</div>`
}

/* ========================================================
   CONFIG
======================================================== */

function renderConfig(){
  const c     = document.getElementById("content")
  const cfg   = CONFIG || {}
  const plex  = cfg.PLEX       || {}
  const tmdb  = cfg.TMDB       || {}
  const radarr = cfg.RADARR    || {}
  const cls   = cfg.CLASSICS   || {}
  const act   = cfg.ACTOR_HITS || {}

  const field = (id, label, value, type = "text") => `
  <label class="block text-sm text-zinc-400">${label}
    <input id="${id}" type="${type}"
      class="mt-1 w-full bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-zinc-100"
      value="${value ?? ""}"/>
  </label>`

  c.innerHTML = `
  <div class="max-w-xl space-y-6">
    <div class="bg-zinc-900 border border-zinc-800 rounded p-4 space-y-3">
      <div class="font-semibold text-zinc-200">Plex</div>
      ${field("cfg_plex_url",   "Plex URL",     plex.PLEX_URL     || "")}
      ${field("cfg_plex_token", "Plex Token",   plex.PLEX_TOKEN   || "")}
      ${field("cfg_library",    "Library Name", plex.LIBRARY_NAME || "")}
    </div>
    <div class="bg-zinc-900 border border-zinc-800 rounded p-4 space-y-3">
      <div class="font-semibold text-zinc-200">TMDB</div>
      ${field("cfg_tmdb_key", "TMDB API Key", tmdb.TMDB_API_KEY || "")}
    </div>
    <details class="bg-zinc-900 border border-zinc-800 rounded p-4">
      <summary class="font-semibold text-zinc-200 cursor-pointer select-none">Advanced settings</summary>
      <div class="space-y-3 mt-3">
        <div class="text-xs uppercase tracking-wide text-zinc-500 pt-1">Classics</div>
        ${field("cfg_classics_pages",  "Pages to fetch (TMDB Top Rated)", cls.CLASSICS_PAGES       ?? 4,    "number")}
        ${field("cfg_classics_votes",  "Minimum votes",                   cls.CLASSICS_MIN_VOTES   ?? 5000, "number")}
        ${field("cfg_classics_rating", "Minimum rating",                  cls.CLASSICS_MIN_RATING  ?? 8.0,  "number")}
        ${field("cfg_classics_max",    "Max results",                     cls.CLASSICS_MAX_RESULTS ?? 120,  "number")}
        <div class="text-xs uppercase tracking-wide text-zinc-500 pt-2">Actors</div>
        ${field("cfg_actor_votes", "Minimum votes per film",    act.ACTOR_MIN_VOTES             ?? 500, "number")}
        ${field("cfg_actor_max",   "Max results per actor",     act.ACTOR_MAX_RESULTS_PER_ACTOR ?? 10,  "number")}
        <div class="text-xs uppercase tracking-wide text-zinc-500 pt-2">Plex scanner</div>
        ${field("cfg_plex_page_size", "Page size",                    plex.PLEX_PAGE_SIZE    ?? 500, "number")}
        ${field("cfg_short_limit",    "Short movie limit (minutes)",  plex.SHORT_MOVIE_LIMIT ?? 60,  "number")}
      </div>
    </details>
    <div class="bg-zinc-900 border border-zinc-800 rounded p-4 space-y-3">
      <div class="font-semibold text-zinc-200">Radarr <span class="text-xs font-normal text-zinc-400">(optional)</span></div>
      <label class="flex items-center gap-2 text-sm text-zinc-400">
        <input type="checkbox" id="cfg_radarr_enabled" ${radarr.RADARR_ENABLED ? "checked" : ""}/> Enabled
      </label>
      ${field("cfg_radarr_url",     "Radarr URL",         radarr.RADARR_URL              || "")}
      ${field("cfg_radarr_key",     "Radarr API Key",     radarr.RADARR_API_KEY          || "")}
      ${field("cfg_radarr_root",    "Root Folder Path",   radarr.RADARR_ROOT_FOLDER_PATH || "")}
      ${field("cfg_radarr_quality", "Quality Profile ID", radarr.RADARR_QUALITY_PROFILE_ID ?? 6, "number")}
    </div>
    <button onclick="saveConfig()"
      class="w-full px-4 py-2 rounded bg-emerald-600 hover:bg-emerald-500 text-black font-semibold">
      Save Configuration
    </button>
    <div id="cfgStatus" class="text-sm text-zinc-400"></div>
  </div>`
}

async function saveConfig(){
  const v  = id => document.getElementById(id)?.value?.trim() ?? ""
  const vi = id => parseInt(v(id))   || 0
  const vf = id => parseFloat(v(id)) || 0

  const payload = {
    PLEX: {
      PLEX_URL:          v("cfg_plex_url"),
      PLEX_TOKEN:        v("cfg_plex_token"),
      LIBRARY_NAME:      v("cfg_library"),
      PLEX_PAGE_SIZE:    vi("cfg_plex_page_size"),
      SHORT_MOVIE_LIMIT: vi("cfg_short_limit"),
    },
    TMDB:  { TMDB_API_KEY: v("cfg_tmdb_key") },
    CLASSICS: {
      CLASSICS_PAGES:       vi("cfg_classics_pages"),
      CLASSICS_MIN_VOTES:   vi("cfg_classics_votes"),
      CLASSICS_MIN_RATING:  vf("cfg_classics_rating"),
      CLASSICS_MAX_RESULTS: vi("cfg_classics_max"),
    },
    ACTOR_HITS: {
      ACTOR_MIN_VOTES:             vi("cfg_actor_votes"),
      ACTOR_MAX_RESULTS_PER_ACTOR: vi("cfg_actor_max"),
    },
    RADARR: {
      RADARR_ENABLED:            document.getElementById("cfg_radarr_enabled")?.checked ?? false,
      RADARR_URL:                v("cfg_radarr_url"),
      RADARR_API_KEY:            v("cfg_radarr_key"),
      RADARR_ROOT_FOLDER_PATH:   v("cfg_radarr_root"),
      RADARR_QUALITY_PROFILE_ID: vi("cfg_radarr_quality"),
    },
  }

  const res = await api("/api/config", "POST", payload)
  document.getElementById("cfgStatus").textContent = res.ok ? "✓ Saved" : "✗ Error saving"

  if (res.configured){
    CONFIGURED = true
    CONFIG     = await api("/api/config")
    await loadResults()
  }
}

/* ========================================================
   RENDER ROUTER
======================================================== */

function render(){
  if (!CONFIGURED){
    topFilters().style.display = "none"
    ACTIVE_TAB = "config"
    return renderConfig()
  }

  updateFilterBar()   // rebuild filter bar for current tab

  if (ACTIVE_TAB === "dashboard")   return renderDashboard()
  if (ACTIVE_TAB === "franchises")  return renderFranchises()
  if (ACTIVE_TAB === "directors")   return renderDirectors()
  if (ACTIVE_TAB === "actors")      return renderActors()
  if (ACTIVE_TAB === "classics")    return renderClassics()
  if (ACTIVE_TAB === "suggestions") return renderSuggestions()
  if (ACTIVE_TAB === "notmdb")      return renderNoTmdb()
  if (ACTIVE_TAB === "nomatch")     return renderNoMatch()
  if (ACTIVE_TAB === "wishlist")    return renderWishlist()
  if (ACTIVE_TAB === "config")      return renderConfig()
}

/* ========================================================
   NAVIGATION
======================================================== */

function setActiveTab(tab){
  ACTIVE_TAB = tab
  document.querySelectorAll(".nav").forEach(b => b.classList.remove("bg-zinc-800"))
  document.querySelector(`.nav[data-tab="${tab}"]`)?.classList.add("bg-zinc-800")
  render()
}

document.addEventListener("click", e => {
  const btn = e.target.closest(".nav")
  if (!btn) return
  setActiveTab(btn.dataset.tab)
})

// Live re-render when any filter control changes
document.addEventListener("input",  e => {
  if (["search","groupFilter","sort"].includes(e.target.id)) render()
})
document.addEventListener("change", e => {
  if (["yearFilter","groupFilter","sort"].includes(e.target.id)) render()
})

/* ========================================================
   BOOT
======================================================== */

async function boot(){
  await loadConfig()
  await loadStatus()
  if (CONFIGURED) await loadResults()
  else { statusEl().textContent = "Setup required"; render() }
}

document.getElementById("scanBtn")?.addEventListener("click", rescan)
boot()