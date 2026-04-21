// ── Pure helpers (no DOM dependency, safe on any page) ───────────────────────
function esc(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function hrSize(n) {
  const u = ['B', 'KB', 'MB', 'GB'];
  let s = n;
  for (const unit of u) { if (s < 1024) return s.toFixed(1) + ' ' + unit; s /= 1024; }
  return s.toFixed(1) + ' TB';
}
async function api(url, opts = {}) {
  const { timeout = 15000, ...fetchOpts } = opts;
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), timeout);
  try {
    const r = await fetch(url, { ...fetchOpts, signal: controller.signal });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || JSON.stringify(d));
    return d;
  } finally {
    clearTimeout(tid);
  }
}
function chk() {
  return '<svg style="width:100%;height:100%" viewBox="0 0 10 10" fill="none"><polyline points="1,5 4,8 9,2" stroke="white" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
}
function toast(msg, type = '') {
  const el = document.getElementById('toasts');
  if (!el) return;
  const t = document.createElement('div');
  t.className = 'toast' + (type ? ' ' + type : '');
  t.textContent = msg;
  el.prepend(t);
  
  // Auto-reveal activity panel on technical errors
  if (type === 'err') {
    document.querySelector('.ws')?.classList.add('drawer-open');
  }
  
  setTimeout(() => t.remove(), 4000);
}
function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

window.wipeCache = async () => {
  if (!confirm("Are you sure? This will clear all 300k+ cached media records. You'll need to re-scan to see items again.")) return;
  try {
    await fetch('/api/cache/wipe', { method: 'POST' });
    location.reload();
  } catch (e) { alert("Wipe failed: " + e.message); }
};
