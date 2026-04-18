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
  const r = await fetch(url, opts);
  const d = await r.json();
  if (!r.ok) throw new Error(d.detail || JSON.stringify(d));
  return d;
}
function chk() {
  return '<svg width="10" height="10" viewBox="0 0 10 10" fill="none"><polyline points="1,5 4,8 9,2" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
}
function toast(msg, type = '') {
  const el = document.getElementById('toasts');
  if (!el) return;
  const t = document.createElement('div');
  t.className = 'toast' + (type ? ' ' + type : '');
  t.textContent = msg;
  el.prepend(t);
  setTimeout(() => t.remove(), 4000);
}
function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

// ════════════════════════════════════════════════════════════════════════════
// INDEX PAGE
// ════════════════════════════════════════════════════════════════════════════
if (document.body.classList.contains('idx-page')) {
  (function () {
    // ── State ──────────────────────────────────────────────────────────────
    let allChs = [];
    let selChs = new Set();
    let items = new Map();   // key="cid_mid" → item object
    let selItems = new Set();
    let filter = 'all';
    let stream = null;
    let paused = false;
    let renderBuf = [];
    let cardIdx = 0;
    let viewMode = 'gallery';

    const COLORS = ['#4f8eff', '#ff6b8a', '#35d47b', '#ffb84f', '#a78bff', '#ff9b3d', '#0ecfcf'];

    const thumbObs = new IntersectionObserver(entries => {
      entries.forEach(e => {
        if (e.isIntersecting) {
          loadThumb(e.target, items.get(e.target.dataset.key));
          thumbObs.unobserve(e.target);
        }
      });
    }, { rootMargin: '1000px' });

    // ── View switcher ───────────────────────────────────────────────────────
    window.sv = v => {
      ['va', 'vo', 'vapp'].forEach(id => document.getElementById(id)?.classList.remove('active'));
      document.getElementById(v)?.classList.add('active');
    };

    // ── Auth ────────────────────────────────────────────────────────────────
    window.doAuth = async () => {
      const aid = +document.getElementById('aid').value;
      const ahs = document.getElementById('ahs').value.trim();
      const aph = document.getElementById('aph').value.trim();
      if (!aid || !ahs || !aph) return showErr('ae', 'All fields required');
      const btn = document.getElementById('abt');
      btn.disabled = true; btn.textContent = 'Sending…';
      try {
        const r = await api('/api/auth/init', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ api_id: aid, api_hash: ahs, phone: aph })
        });
        if (r.already_authorized) { sv('vapp'); initApp(); }
        else sv('vo');
      } catch (e) { showErr('ae', e.message); }
      finally { btn.disabled = false; btn.textContent = 'Send OTP →'; }
    };

    window.doOtp = async () => {
      const code = document.getElementById('ocode').value.trim();
      const pwd = document.getElementById('tpwd').value;
      if (!code) return showErr('oe', 'Enter the OTP code');
      const btn = document.getElementById('obt');
      btn.disabled = true; btn.textContent = 'Verifying…';
      try {
        const r = await api('/api/auth/verify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code, password: pwd || undefined })
        });
        if (r.needs_2fa) {
          document.getElementById('twof').style.display = 'block';
          document.getElementById('osub').textContent = '2FA active — enter your cloud password.';
        } else { sv('vapp'); initApp(); }
      } catch (e) { showErr('oe', e.message); }
      finally { btn.disabled = false; btn.textContent = 'Verify →'; }
    };

    window.doLogout = async () => {
      await api('/api/auth/logout', { method: 'POST' });
      selChs.clear(); items.clear(); selItems.clear();
      sv('va');
    };

    // ── App init ────────────────────────────────────────────────────────────
    function initApp() {
      allChs = []; selChs.clear(); items.clear(); selItems.clear(); updSelCount();
      document.getElementById('chlist').innerHTML =
        '<div style="padding:8px;display:grid;gap:6px">' +
        '<div class="sk" style="height:56px"></div>'.repeat(5) + '</div>';
      const es = new EventSource('/api/channels');
      es.onmessage = e => {
        const ch = JSON.parse(e.data);
        if (ch.done) { es.close(); return; }
        allChs.push(ch);
        appendCh(ch);
      };
      es.onerror = () => es.close();
      updViewport();
    }

    // ── Channel list ────────────────────────────────────────────────────────
    function chColor(t) {
      return COLORS[Math.abs([...t].reduce((a, c) => a + c.charCodeAt(0), 0)) % COLORS.length];
    }

    function appendCh(ch) {
      const list = document.getElementById('chlist');
      if (list.querySelector('.sk')) list.innerHTML = '';
      const color = chColor(ch.title);
      const sel = selChs.has(ch.id);
      const div = document.createElement('div');
      div.className = 'ch' + (sel ? ' sel' : '');
      div.dataset.id = ch.id;
      div.onclick = () => toggleCh(ch.id, div);
      const mem = ch.members
        ? `${ch.members > 999 ? (ch.members / 1000).toFixed(1) + 'k' : ch.members} members`
        : ch.type;
      div.innerHTML = `
        <div class="av" style="background:${color}20;color:${color}">${ch.title.charAt(0).toUpperCase()}</div>
        <div class="ci"><div class="cn">${esc(ch.title)}</div><div class="cm">${mem}</div></div>
        <div class="ck">${sel ? chk() : ''}</div>`;
      list.appendChild(div);
    }

    function toggleCh(id, el) {
      if (selChs.has(id)) { selChs.delete(id); el.classList.remove('sel'); el.querySelector('.ck').innerHTML = ''; }
      else { selChs.add(id); el.classList.add('sel'); el.querySelector('.ck').innerHTML = chk(); }
      const btn = document.getElementById('vmbtn');
      btn.textContent = `${selChs.size} selected`;
      // Auto-load immediately on selection change
      if (selChs.size > 0) loadMedia();
      else {
        // Clear grid when nothing selected
        items.clear(); selItems.clear(); allMediaKeys = [];
        const grid = document.getElementById('mgrid');
        grid.innerHTML = ''; grid.style.display = 'none';
        document.getElementById('empty').style.display = 'flex';
        updPillCounts();
      }
    }

    function updPillCounts() {
      const counts = { all: 0, photo: 0, video: 0, document: 0 };
      for (const item of items.values()) {
        counts.all++;
        if (counts[item.type] !== undefined) counts[item.type]++;
      }
      const labels = { all: 'All', photo: '📷 Photos', video: '🎬 Videos', document: '📄 Docs' };
      document.querySelectorAll('.pill').forEach(pill => {
        const f = pill.getAttribute('onclick')?.match(/'(\w+)'/)?.[1];
        if (f && counts[f] !== undefined) {
          pill.textContent = counts[f] > 0 ? `${labels[f]} ${counts[f].toLocaleString()}` : labels[f];
        }
      });
    }

    window.filterChs = q => {
      const lo = q.toLowerCase();
      document.getElementById('chlist').innerHTML = '';
      (q ? allChs.filter(c => c.title.toLowerCase().includes(lo)) : allChs).forEach(appendCh);
    };

    window.switchSTab = (name, el) => {
      document.querySelectorAll('.stab').forEach(t => t.classList.remove('active'));
      el.classList.add('active');
      document.querySelectorAll('.sidebar-pane').forEach(p => p.classList.remove('active'));
      document.getElementById('sp-' + name)?.classList.add('active');
      if (name === 'channels') updViewport();
    };

    // ── Virtual Scroller State ──────────────────────────────────────────────
    let allMediaKeys = [];
    let filteredKeys = [];
    let currentSearch = '';
    let currentSort = '';
    let viewportHeight = 0;
    let columns = 1;

    function updViewport() {
      const grid = document.getElementById('mgrid');
      if (!grid) return;
      viewportHeight = grid.clientHeight;
      const cardW = (viewMode === 'gallery') ? 250 : 152;
      columns = Math.floor((grid.clientWidth - 12) / (cardW + 8)) || 1;
      renderVirtual();
    }

    function renderVirtual() {
      const grid = document.getElementById('mgrid');
      if (!grid || !filteredKeys.length) {
        if (grid) { grid.innerHTML = ''; grid.style.paddingTop = '0'; grid.style.paddingBottom = '0'; }
        return;
      }

      const st = grid.scrollTop;
      const rowH = (viewMode === 'gallery') ? 258 : 160;
      const startRow = Math.max(0, Math.floor(st / rowH) - 1);
      const endRow = Math.min(Math.ceil((st + viewportHeight) / rowH) + 1, Math.ceil(filteredKeys.length / columns));

      const startIdx = startRow * columns;
      const endIdx = endRow * columns;
      const visibleKeys = filteredKeys.slice(startIdx, endIdx);
      const visibleSet = new Set(visibleKeys);

      // Update padding
      grid.style.paddingTop = (startRow * rowH) + 'px';
      grid.style.paddingBottom = (Math.max(0, Math.ceil(filteredKeys.length / columns) - endRow) * rowH) + 'px';

      // 1. Remove cards that are no longer visible
      const children = Array.from(grid.children);
      children.forEach(child => {
        const key = child.dataset.key;
        if (!visibleSet.has(key)) {
          grid.removeChild(child);
        }
      });

      // 2. Add or Move cards to maintain order
      const existing = new Map();
      Array.from(grid.children).forEach(c => existing.set(c.dataset.key, c));

      visibleKeys.forEach((key, idx) => {
        const item = items.get(key);
        if (!item) return;

        let el = existing.get(key);
        if (!el) {
          el = makeCard(key, item);
          thumbObs.observe(el);
          el.dataset.obs = '1';
        }
        grid.appendChild(el);
      });
    }

    window.addEventListener('scroll', e => {
      if (e.target.id === 'mgrid') renderVirtual();
    }, true);

    window.addEventListener('resize', updViewport);

    // ── Media loading ───────────────────────────────────────────────────────
    window.setFilter = (f, el) => {
      filter = f;
      document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
      el.classList.add('active');
      applyFilters();
      if (!allMediaKeys.length && selChs.size) loadMedia();
    };

    function applyFilters() {
      if (viewportHeight === 0) updViewport();
      let keys = allMediaKeys;

      // Type Filter
      if (filter !== 'all') {
        keys = keys.filter(k => {
          const it = items.get(k);
          return it && it.type === filter;
        });
      }

      // Search Filter
      if (currentSearch) {
        keys = keys.filter(k => items.get(k).filename.toLowerCase().includes(currentSearch));
      }

      // Sort
      if (currentSort) {
        keys.sort((a, b) => {
          const ia = items.get(a), ib = items.get(b);
          if (currentSort === 'date-desc') return new Date(ib.date) - new Date(ia.date);
          if (currentSort === 'date-asc') return new Date(ia.date) - new Date(ib.date);
          if (currentSort === 'size-desc') return ib.size - ia.size;
          if (currentSort === 'size-asc') return ia.size - ib.size;
          return 0;
        });
      }

      filteredKeys = keys;
      renderVirtual();
    }



    window.loadMedia = () => {
      if (!selChs.size) return;
      cardIdx = 0;
      if (stream) { stream.close(); stream = null; }
      items.clear(); selItems.clear(); allMediaKeys = []; updSelCount();
      renderBuf = []; paused = false;
      updViewport();

      const grid = document.getElementById('mgrid');
      grid.style.display = 'grid';
      grid.innerHTML = '';
      grid.style.paddingTop = '0';
      grid.style.paddingBottom = '0';
      grid.onscroll = renderVirtual;

      document.getElementById('empty').style.display = 'none';
      document.getElementById('sbar').classList.add('active');
      let cnt = 0;

      const ids = [...selChs].join(',');
      let url = `/api/media?channels=${ids}&type=all`;

      stream = new EventSource(url);

      stream.onmessage = e => {
        const d = JSON.parse(e.data);

        if (d.batch) {
          d.batch.forEach(item => {
            const key = `${item.channel_id}_${item.msg_id}`;
            items.set(key, item);
            allMediaKeys.push(key);
            cnt++;
          });
          document.getElementById('sbar-txt').textContent = `${cnt} from cache`;
          updPillCounts();
          applyFilters();
          return;
        }

        if (d.done) {
          stream.close(); stream = null;
          document.getElementById('sbar-txt').textContent = `${cnt} items loaded`;
          document.getElementById('pause-btn').style.display = 'none';
          updPillCounts();
          applyFilters();
          return;
        }

        if (d.error) { toast(d.error, 'err'); return; }

        const key = `${d.channel_id}_${d.msg_id}`;
        items.set(key, d);
        allMediaKeys.push(key);
        cnt++;
        if (cnt % 50 === 0) {
          document.getElementById('sbar-txt').textContent = `Loading… ${cnt}`;
          updPillCounts();
          if (!paused) applyFilters();
        }
      };

      stream.onerror = () => {
        if (stream) { stream.close(); stream = null; }
        document.getElementById('sbar-txt').textContent = `${cnt} items`;
      };
    };

    window.togglePause = () => {
      paused = !paused;
      document.getElementById('pause-btn').textContent = paused ? '▶ Resume' : '⏸ Pause';
      if (!paused) renderVirtual();
    };

    // ── Media cards ─────────────────────────────────────────────────────────
    function makeCard(key, item) {
      const sel = selItems.has(key);
      const el = document.createElement('div');
      el.className = 'mc' + (sel ? ' sel' : '');
      el.dataset.key = key;
      // Click card to select
      el.onclick = (e) => {
        if (e.target.closest('.mpreview')) {
          e.stopPropagation();
          openPreview(key);
        } else {
          toggleSel(key, el);
        }
      };
      el.ondblclick = ev => { ev.stopPropagation(); openPreview(key); };
      const ti = { photo: '🖼️', video: '🎬', document: '📄' }[item.type] || '❓';
      const cap = item.caption || '';
      el.innerHTML = `
        <div class="nth"><div class="ti">${ti}</div>
          <div style="font-size:9px;padding:0 6px;text-align:center">${esc(item.filename)}</div></div>
        <div class="mchk">${chk()}</div>
        <div class="tbadge ${item.type.charAt(0)}">${item.type.toUpperCase()}</div>
        <div class="minfo"><div class="mfn">${esc(item.filename)}</div>
          <div class="msz">${item.size_readable || hrSize(item.size)}</div></div>
        <div class="mpreview">🔍</div>
        ${cap ? `<div class="mcap">${esc(cap.slice(0, 120))}</div>` : ''}`;
      return el;
    }

    function appendCard(key, item) {
      // Not used in virtual mode, kept for legacy if needed
    }

    async function loadThumb(cardEl, item) {
      if (!item || item.type === 'document') return;
      try {
        const r = await fetch(`/api/thumb/${item.channel_id}/${item.msg_id}`);
        if (!r.ok) return;
        const blob = await r.blob();
        const url = URL.createObjectURL(blob);
        cardEl.style.setProperty('--bg-img', `url(${url})`);
        const nth = cardEl.querySelector('.nth');
        if (!nth) return;
        if (item.type === 'photo') {
          const img = document.createElement('img'); img.src = url; nth.replaceWith(img);
        } else {
          const vid = document.createElement('video');
          vid.poster = url; vid.muted = true; vid.loop = true; vid.setAttribute('playsinline', '');
          vid.className = 'card-vid';
          nth.replaceWith(vid);
          cardEl.onmouseenter = () => {
            if (!vid.src) { vid.src = `/api/preview/${item.channel_id}/${item.msg_id}`; vid.load(); }
            vid.play().catch(() => { });
          };
          cardEl.onmouseleave = () => vid.pause();
        }
      } catch { }
    }

    function toggleSel(key, el) {
      if (selItems.has(key)) { selItems.delete(key); el.classList.remove('sel'); }
      else { selItems.add(key); el.classList.add('sel'); }
      updSelCount();
    }

    window.selAll = () => {
      const allKeys = [...items.keys()];
      const allSel = allKeys.every(k => selItems.has(k));
      if (allSel) allKeys.forEach(k => selItems.delete(k));
      else allKeys.forEach(k => selItems.add(k));
      renderVirtual(); // Redraw all visible to show checkmarks
      updSelCount();
    };

    function updSelCount() {
      const n = selItems.size;
      document.getElementById('selc').textContent = n;
      const btn = document.getElementById('dlbtn');
      if (btn) btn.disabled = n === 0;
    }

    window.searchMedia = q => {
      currentSearch = q.toLowerCase();
      applyFilters();
    };

    window.sortMedia = by => {
      currentSort = by;
      applyFilters();
    };
    
    
    
    // ── Native Lightbox — improved strip model ─────────────────────
    let lbIdx = 0;
    let lbStripping = false;

    const lb = document.getElementById('lb');
    const lbTrack = document.getElementById('lb-content');
    const lbInfo = document.getElementById('lb-info');

    // Setup track
    lbTrack.innerHTML = '';
    lbTrack.style.cssText = `
      display: flex;
      width: 300%;
      height: 100%;
      will-change: transform;
      transition: transform 0.5s cubic-bezier(0.22, 1, 0.36, 1);
      transform: translateX(-33.333%);`;

    const panels = [0, 1, 2].map(() => {
      const p = document.createElement('div');
      p.className = 'lb-panel';
      lbTrack.appendChild(p);
      return p;
    });

    // 🔥 PRELOAD (CRITICAL)
    function preload(idx) {
      const key = filteredKeys[idx];
      if (!key) return;
      const item = items.get(key);
      if (!item || item.type === 'video') return;

      const img = new Image();
      img.src = `/api/preview/${item.channel_id}/${item.msg_id}`;
    }

    // Fill panel
    async function _lbFillPanel(panel, idx) {
      panel.innerHTML = '';
      const key = filteredKeys[idx];
      if (!key) return;

      const item = items.get(key);
      if (!item) return;

      const url = `/api/preview/${item.channel_id}/${item.msg_id}`;

      if (item.type === 'video') {
        const v = document.createElement('video');
        v.src = url;
        v.controls = true;
        v.playsInline = true;
        v.className = 'lb-media';
        panel.appendChild(v);
        return v;
      }

      return new Promise(res => {
        const img = new Image();
        img.className = 'lb-media';
        img.onload = () => {
          panel.appendChild(img);
          res(img);
        };
        img.onerror = () => {
          panel.innerHTML = '<div style="color:rgba(255,255,255,.4)">Preview not available</div>';
          res();
        };
        img.src = url;
      });
    }

    function _lbUpdateInfo() {
      const item = items.get(filteredKeys[lbIdx]);
      if (!item) return;
      lbInfo.textContent = `${item.filename} · ${item.size_readable || hrSize(item.size)} · ${lbIdx + 1} / ${filteredKeys.length}`;
    }

    // Init
    async function _lbInitStrip(idx) {
      lbTrack.style.transition = 'none';
      lbTrack.style.transform = 'translateX(-33.333%)';

      panels.forEach(p => p.innerHTML = '<div class="lb-spinner"></div>');

      await Promise.all([
        _lbFillPanel(panels[0], idx - 1),
        _lbFillPanel(panels[1], idx),
        _lbFillPanel(panels[2], idx + 1),
      ]);

      panels[1].classList.add('active');

      // 🔥 preload neighbors
      preload(idx + 2);
      preload(idx - 2);

      panels[1].querySelector('video')?.play().catch(() => { });
    }

    function lbOpen(key) {
      lbIdx = filteredKeys.indexOf(key);
      if (lbIdx === -1) return;

      lb.classList.add('open');
      document.body.style.overflow = 'hidden';

      _lbInitStrip(lbIdx);
      _lbUpdateInfo();
    }

    function lbClose() {
      lb.classList.remove('open');
      document.body.style.overflow = '';

      lbTrack.querySelectorAll('video').forEach(v => {
        v.pause();
        v.src = '';
      });
    }

    // 🚀 SMOOTH NAVIGATION
    function lbNav(dir) {
      if (lbStripping) return;

      const next = lbIdx + dir;
      if (next < 0 || next >= filteredKeys.length) return;

      lbStripping = true;

      const current = panels[1];
      const target = dir > 0 ? panels[2] : panels[0];

      // 🔥 CROSSFADE (important)
      current.classList.remove('active');
      current.classList.add('prev');
      target.classList.add('active');

      current.querySelector('video')?.pause();

      // 🔥 preload ahead BEFORE move
      preload(next + 1);
      preload(next - 1);

      lbTrack.style.transition = 'transform 0.5s cubic-bezier(0.22, 1, 0.36, 1)';
      lbTrack.style.transform =
        dir > 0 ? 'translateX(-66.666%)' : 'translateX(0%)';

      lbTrack.addEventListener('transitionend', async function onDone() {
        lbTrack.removeEventListener('transitionend', onDone);

        lbIdx = next;
        _lbUpdateInfo();

        // 🔥 Rotate panels WITHOUT visual jump
        lbTrack.style.transition = 'none';

        if (dir > 0) {
          lbTrack.appendChild(lbTrack.firstElementChild);
        } else {
          lbTrack.prepend(lbTrack.lastElementChild);
        }

        // Refresh panel refs
        for (let i = 0; i < 3; i++) panels[i] = lbTrack.children[i];

        // Reset states
        panels.forEach(p => p.classList.remove('active', 'prev'));
        panels[1].classList.add('active');

        // Recenter
        lbTrack.style.transform = 'translateX(-33.333%)';

        // Fill edge
        const edgeIdx = dir > 0 ? lbIdx + 1 : lbIdx - 1;
        const edgePanel = dir > 0 ? panels[2] : panels[0];

        edgePanel.innerHTML = '<div class="lb-spinner"></div>';
        await _lbFillPanel(edgePanel, edgeIdx);

        panels[1].querySelector('video')?.play().catch(() => { });

        lbStripping = false;
      }, { once: true });
    }
    // ── Native Lightbox ─────────────────────────────────────────────────────
    let lbIdx = 0;
    let lbActiveMedia = null;
    let lbNavigating = false;

    const lb     = document.getElementById('lb');
    const lbCnt  = document.getElementById('lb-content');
    const lbInfo = document.getElementById('lb-info');

    function lbOpen(key) {
      lbIdx = filteredKeys.indexOf(key);
      if (lbIdx === -1) return;
      lb.classList.add('open');
      document.body.style.overflow = 'hidden';
      lbLoad(lbIdx, 0);
    }

    function lbClose() {
      lb.classList.remove('open');
      document.body.style.overflow = '';
      _lbCleanup();
    }

    function _lbCleanup() {
      if (lbActiveMedia) {
        lbActiveMedia.pause?.();
        lbActiveMedia.src = '';
        lbActiveMedia = null;
      }
    }

    // Build the media element (img or video) for an item
    async function _buildMedia(item) {
      const url = `/api/preview/${item.channel_id}/${item.msg_id}`;
      if (item.type === 'video') {
        const vid = document.createElement('video');
        vid.src = url; vid.controls = true; vid.autoplay = true; vid.playsInline = true;
        return vid;
      }
      return new Promise((resolve) => {
        const img = new Image();
        img.onload  = () => resolve(img);
        img.onerror = () => {
          const d = document.createElement('div');
          d.textContent = 'Preview not available';
          d.style.cssText = 'color:rgba(255,255,255,.4);font-size:13px;padding:40px';
          resolve(d);
        };
        img.src = url;
      });
    }

    async function lbLoad(idx, dir) {
      if (lbNavigating) return;
      lbNavigating = true;

      const key  = filteredKeys[idx];
      const item = items.get(key);
      if (!item) { lbNavigating = false; return; }

      lbInfo.textContent = `${item.filename}  ·  ${item.size_readable || hrSize(item.size)}  ·  ${idx + 1} / ${filteredKeys.length}`;

      // If no direction (first open), just load with spinner
      if (dir === 0 || !lbCnt.firstChild) {
        lbCnt.innerHTML = '<div class="lb-spinner"></div>';
        const el = await _buildMedia(item);
        if (item.type === 'video') lbActiveMedia = el;
        lbCnt.innerHTML = '';
        lbCnt.appendChild(el);
        el.style.animation = 'lb-fadein 0.3s ease both';
        lbNavigating = false;
        // Preload neighbours
        _preload(idx + 1); _preload(idx - 1);
        return;
      }

      // Directional slide: outgoing and incoming layers
      const outgoing = lbCnt.firstChild;
      const SLIDE_DUR = 320; // ms

      // Animate outgoing out
      outgoing.style.animation = `lb-slide-out-${dir > 0 ? 'left' : 'right'} ${SLIDE_DUR}ms cubic-bezier(0.4,0,0.2,1) both`;

      // Load new media concurrently while outgoing slides away
      const [el] = await Promise.all([
        _buildMedia(item),
        new Promise(r => setTimeout(r, SLIDE_DUR * 0.35)), // slight delay for feel
      ]);

      _lbCleanup();
      if (item.type === 'video') lbActiveMedia = el;

      lbCnt.innerHTML = '';
      lbCnt.appendChild(el);
      el.style.animation = `lb-slide-in-${dir > 0 ? 'right' : 'left'} ${SLIDE_DUR}ms cubic-bezier(0.4,0,0.2,1) both`;

      lbNavigating = false;
      _preload(idx + 1); _preload(idx - 1);
    }

    // Preload image into browser cache (no-op for video)
    function _preload(idx) {
      const k = filteredKeys[idx];
      if (!k) return;
      const it = items.get(k);
      if (!it || it.type === 'video') return;
      const p = new Image();
      p.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
    }

    function lbNav(dir) {
      const next = lbIdx + dir;
      if (next < 0 || next >= filteredKeys.length) return;
      lbIdx = next;
      lbLoad(lbIdx, dir);
    }

    // Controls
    document.getElementById('lb-close').onclick = lbClose;
    document.getElementById('lb-prev').onclick = () => lbNav(-1);
    document.getElementById('lb-next').onclick = () => lbNav(1);
    document.getElementById('lb-bg').onclick = lbClose;

    // Touch swipe
    let touchX = 0;
    lb.addEventListener('touchstart', e => { touchX = e.touches[0].clientX; }, { passive: true });
    lb.addEventListener('touchend', e => {
      const dx = e.changedTouches[0].clientX - touchX;
      if (Math.abs(dx) > 40) lbNav(dx < 0 ? 1 : -1);
    }, { passive: true });

    window.openPreview = (key) => lbOpen(key);

    window.toggleView = () => {
      viewMode = viewMode === 'grid' ? 'gallery' : 'grid';
      document.getElementById('mgrid').classList.toggle('gallery-mode', viewMode === 'gallery');
      document.getElementById('vwtbtn').textContent = viewMode === 'gallery' ? '☷ Grid view' : '🖼️ Gallery view';
      if (viewMode === 'gallery') {
        selItems.clear();
        updSelCount();
        document.getElementById('mgrid').style.paddingTop = '0';
        document.getElementById('mgrid').style.paddingBottom = '0';
      } else {
        updViewport();
      }
    };

    window.openGallery = () => {
      const ids = [...selChs].join(',');
      window.open('/gallery' + (ids ? '?channels=' + ids : ''), '_blank');
    };

    // ── Download ─────────────────────────────────────────────────────────────
    window.doDl = async () => {
      const dItems = [...selItems].map(k => items.get(k)).filter(Boolean)
        .map(i => ({ channel_id: i.channel_id, msg_id: i.msg_id, filename: i.filename }));
      if (!dItems.length) return;
      toast(`Starting download of ${dItems.length} files…`, 'ok');
      const { job_id } = await api('/api/download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: dItems })
      });
      openDrawer(job_id, dItems.length);
      [...selItems].forEach(k => document.querySelector(`.mc[data-key="${k}"]`)?.classList.add('dl'));
    };

    function openDrawer(job_id, count) {
      document.getElementById('pdrawer').classList.add('open');
      const card = document.createElement('div');
      card.className = 'pitem'; card.id = `j-${job_id}`;
      card.innerHTML = `
        <div class="pn">📦 ${count} file${count !== 1 ? 's' : ''}</div>
        <div class="pb-wrap"><div class="pb" id="pb-${job_id}"></div></div>
        <div class="pm"><span id="pc-${job_id}">Queued…</span><span id="pp-${job_id}">0%</span></div>
        <div class="perr" id="pe-${job_id}" style="display:none"></div>
        <button class="pcancel" onclick="cancelJob('${job_id}')">Cancel</button>`;
      document.getElementById('plist').prepend(card);
      const es = new EventSource(`/api/download/${job_id}/progress`);
      es.onmessage = e => {
        const d = JSON.parse(e.data);
        const pct = d.total ? Math.round(d.done / d.total * 100) : (d.pct || 0);
        document.getElementById(`pb-${job_id}`).style.width = pct + '%';
        document.getElementById(`pp-${job_id}`).textContent = pct + '%';
        const label = d.status === 'done' ? `✅ ${d.done} saved${d.skipped ? ' (' + d.skipped + ' skipped)' : ''}`
          : d.status === 'cancelled' ? '🚫 Cancelled'
            : d.flood_wait ? `⏳ Rate limited ${d.flood_wait}s`
              : d.current || `${d.done || 0}/${d.total || 0}`;
        document.getElementById(`pc-${job_id}`).textContent = label;
        if (d.errors?.length) {
          const pe = document.getElementById(`pe-${job_id}`);
          pe.style.display = 'block'; pe.textContent = d.errors.slice(-1)[0];
        }
        if (d.status === 'done') { es.close(); toast(`Download complete — ${d.done} files`, 'ok'); document.querySelector(`#j-${job_id} .pcancel`)?.remove(); }
        if (d.status === 'cancelled' || d.status === 'error') es.close();
      };
      es.onerror = () => es.close();
    }

    window.cancelJob = async job_id => {
      await api(`/api/download/${job_id}/cancel`, { method: 'POST' });
    };
    window.closeDr = () => document.getElementById('pdrawer').classList.remove('open');

    // ── yt-dlp ───────────────────────────────────────────────────────────────
    window.doYtdlp = async () => {
      const url = document.getElementById('ext-url').value.trim();
      const fmt = document.getElementById('ext-fmt').value;
      if (!url) return toast('Enter a URL', 'err');
      try {
        const { job_id } = await api('/api/ytdlp', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url, fmt })
        });
        const hist = document.getElementById('ext-history');
        const card = document.createElement('div');
        card.className = 'ext-item'; card.id = `ext-${job_id}`;
        card.innerHTML = `
          <div class="ext-url">${esc(url)}</div>
          <div id="ext-st-${job_id}" style="color:var(--muted);font-size:11px">Queued…</div>
          <div class="ext-prog"><div class="ext-pbar" id="ext-pb-${job_id}"></div></div>
          <button class="pcancel" style="margin-top:6px" onclick="cancelJob('${job_id}')">Cancel</button>`;
        hist.prepend(card);
        const es = new EventSource(`/api/download/${job_id}/progress`);
        es.onmessage = e => {
          const d = JSON.parse(e.data);
          document.getElementById(`ext-pb-${job_id}`).style.width = (d.pct || 0) + '%';
          document.getElementById(`ext-st-${job_id}`).textContent =
            d.status === 'done' ? '✅ Done' : d.status === 'cancelled' ? '🚫 Cancelled' : (d.current || 'Running…');
          if (d.status === 'done') { es.close(); toast('Download complete', 'ok'); document.querySelector(`#ext-${job_id} .pcancel`)?.remove(); }
          if (d.status === 'cancelled' || d.status === 'error') es.close();
        };
        es.onerror = () => es.close();
      } catch (e) { toast(e.message, 'err'); }
    };

    // ── Keyboard shortcuts ────────────────────────────────────────────────────
    document.addEventListener('keydown', e => {
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
        if (e.key === 'Enter' && !e.shiftKey) {
          if (document.getElementById('va')?.classList.contains('active')) doAuth();
          else if (document.getElementById('vo')?.classList.contains('active')) doOtp();
        }
        return;
      }
      if (e.key === 'Escape') { lbClose(); closeDr(); }
      if (lb.classList.contains('open')) {
        if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { e.preventDefault(); lbNav(-1); return; }
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') { e.preventDefault(); lbNav(1); return; }
      }
      if ((e.metaKey || e.ctrlKey) && e.key === 'a') { e.preventDefault(); selAll(); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { if (selItems.size) doDl(); }
    });

    document.getElementById('msearch')?.addEventListener('input', debounce(e => searchMedia(e.target.value), 300));

    // ── Error helpers ─────────────────────────────────────────────────────────
    function showErr(id, m) { const el = document.getElementById(id); el.textContent = m; el.style.display = 'block'; }
    function hideErr(id) { document.getElementById(id).style.display = 'none'; }

    // ── Boot ──────────────────────────────────────────────────────────────────
    (async () => {
      try {
        const r = await api('/api/auth/status');
        if (r.authenticated) { sv('vapp'); initApp(); }
        else sv('va');
      } catch { sv('va'); }
    })();

  })(); // end idx-page IIFE
}

// ════════════════════════════════════════════════════════════════════════════
// GALLERY PAGE
// ════════════════════════════════════════════════════════════════════════════
if (document.body.classList.contains('gal-page')) {
  (function () {
    let items = [];
    let cur = 0;

    const params = new URLSearchParams(location.search);
    const channels = params.get('channels') || '';

    const thumbObs = new IntersectionObserver(entries => {
      entries.forEach(e => {
        if (!e.isIntersecting) return;
        const card = e.target;
        const item = items[+card.dataset.idx];
        if (!item) return;
        const media = card.querySelector('img, video');
        if (media && !media.src) {
          media.src = item.thumb;
          if (item.type === 'video') media.poster = item.thumb;
        }
        thumbObs.unobserve(card);
      });
    }, { rootMargin: '400px' });

    async function loadGallery() {
      try {
        const r = await fetch(`/api/gallery-data${channels ? '?channels=' + channels : ''}`);
        items = await r.json();
      } catch { items = []; }
      render();
    }

    function render() {
      const grid = document.getElementById('grid');
      if (!grid) return;
      grid.innerHTML = '';

      const photos = items.filter(i => i.type === 'photo').length;
      const videos = items.filter(i => i.type === 'video').length;
      const hct = document.getElementById('hct');
      if (hct) hct.textContent = `${photos} photo${photos !== 1 ? 's' : ''} · ${videos} video${videos !== 1 ? 's' : ''}`;

      if (!items.length) {
        grid.innerHTML = '<div class="empty"><div class="empty-icon">🖼</div><div>No media in cache yet</div></div>';
        return;
      }

      items.forEach((item, i) => {
        const card = document.createElement('div');
        card.className = 'card' + (i % 5 === 1 ? ' wide' : '') + (i % 7 === 3 ? ' tall' : '');
        card.dataset.idx = i;
        card.addEventListener('click', () => openLb(i));
        const isVid = item.type === 'video';
        card.innerHTML = `
          ${isVid ? `<video class="card-media" preload="none" muted playsinline></video>
            <div class="play-ring"><button class="play-btn" aria-label="Play">
              <svg viewBox="0 0 20 20"><path d="M5 3l13 7-13 7z"/></svg></button></div>`
            : `<img class="card-media" alt="${esc(item.title)}" loading="lazy">`}
          <div class="badge">${item.type.toUpperCase()}</div>
          <div class="card-caption">
            <div class="card-title">${esc(item.title)}</div>
            ${item.caption ? `<div class="card-sub">${esc(item.caption.slice(0, 80))}</div>` : ''}
          </div>`;
        grid.appendChild(card);
        thumbObs.observe(card);
      });
    }

    const lb = document.getElementById('lb');

    window.openLb = idx => {
      cur = idx;
      renderLb();
      lb?.classList.add('open');
      document.body.style.overflow = 'hidden';
    };

    window.closeLb = () => {
      lb?.classList.remove('open');
      document.body.style.overflow = '';
      const v = lb?.querySelector('video');
      if (v) { v.pause(); v.src = ''; }
    };

    window.nav = (dir, e) => {
      e?.stopPropagation();
      const box = document.getElementById('lbbox');
      if (!box) return;
      box.style.opacity = '0';
      setTimeout(() => {
        const v = box.querySelector('video');
        if (v) { v.pause(); v.src = ''; }
        cur = ((cur + dir) % items.length + items.length) % items.length;
        renderLb();
        box.style.opacity = '1';
      }, 150);
    };

    function renderLb() {
      const item = items[cur];
      const box = document.getElementById('lbbox');
      const cap = document.getElementById('lbcap');
      const cnt = document.getElementById('lbcnt');
      if (!item || !box) return;

      box.innerHTML = item.type === 'video'
        ? `<video src="${item.preview}" poster="${item.thumb}" controls autoplay
             style="max-width:min(96vw,1000px);max-height:78vh;border-radius:16px"></video>`
        : `<img src="${item.preview}" alt="${esc(item.title)}"
             style="max-width:min(96vw,1000px);max-height:78vh;border-radius:16px;object-fit:contain">`;

      let sub = item.sub || '';
      try { if (sub) sub = new Date(sub).toLocaleDateString('en-IN', { year: 'numeric', month: 'short', day: 'numeric' }); } catch { }

      if (cap) cap.innerHTML = `
        <div class="lb-title">${esc(item.title)}</div>
        ${sub || item.size ? `<div class="lb-sub">${esc(sub)}${sub && item.size ? ' · ' : ''}${item.size ? hrSize(item.size) : ''}</div>` : ''}
        ${item.caption ? `<div class="lb-full">${esc(item.caption)}</div>` : ''}`;

      if (cnt) cnt.textContent = `${cur + 1} / ${items.length}`;
    }

    window.onBg = e => { if (e.target === lb) closeLb(); };

    window.toggleMode = () => {
      const h = document.documentElement;
      const dark = getComputedStyle(h).getPropertyValue('--bg').trim().startsWith('#0');
      if (dark) {
        h.style.setProperty('--bg', '#f0f2f8'); h.style.setProperty('--text', '#1a1d2e');
        h.style.setProperty('--border', '#dde0ee'); h.style.setProperty('--muted', '#72788a');
      } else {
        ['--bg', '--text', '--border', '--muted'].forEach(p => h.style.removeProperty(p));
      }
    };

    document.addEventListener('keydown', e => {
      if (!lb?.classList.contains('open')) return;
      if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') nav(-1);
      if (e.key === 'ArrowRight' || e.key === 'ArrowDown') nav(1);
      if (e.key === 'Escape') closeLb();
    });

    let tx = 0;
    lb?.addEventListener('touchstart', e => { tx = e.touches[0].clientX; }, { passive: true });
    lb?.addEventListener('touchend', e => { const dx = e.changedTouches[0].clientX - tx; if (Math.abs(dx) > 50) nav(dx < 0 ? 1 : -1); });

    loadGallery();

  })(); // end gal-page IIFE
}