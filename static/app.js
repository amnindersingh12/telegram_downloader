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
    let folders = [];        // [{name, emoji}]
    let activeFolder = '';   // '' = All

    function initApp() {
      allChs = []; selChs.clear(); items.clear(); selItems.clear(); updSelCount();
      folders = []; activeFolder = '';
      document.getElementById('chlist').innerHTML =
        '<div style="padding:8px;display:grid;gap:6px">' +
        '<div class="sk" style="height:56px"></div>'.repeat(5) + '</div>';
      renderFolderTabs();
      const es = new EventSource('/api/channels');
      let chRenderPending = false;
      es.onmessage = e => {
        const d = JSON.parse(e.data);
        if (d.folder_list) {
          folders = d.folder_list;
          renderFolderTabs();
          return;
        }
        if (d.done) { es.close(); renderChannelList(); return; }
        allChs.push(d);
        if (!chRenderPending) {
          chRenderPending = true;
          requestAnimationFrame(() => { chRenderPending = false; renderChannelList(); });
        }
      };
      es.onerror = () => es.close();
      updViewport();
    }

    function renderFolderTabs() {
      let el = document.getElementById('folder-tabs');
      if (!el) {
        const parent = document.getElementById('sp-channels');
        const ss = parent.querySelector('.ss');
        el = document.createElement('div');
        el.id = 'folder-tabs';
        el.className = 'ftabs';
        parent.insertBefore(el, ss);
      }
      const tabs = [{name: '', emoji: '', label: 'All'}];
      folders.forEach(f => tabs.push({...f, label: (f.emoji ? f.emoji + ' ' : '') + f.name}));
      el.innerHTML = tabs.map(t =>
        `<button class="ftab${t.name === activeFolder ? ' active' : ''}" onclick="setFolder('${esc(t.name)}')">${esc(t.label)}</button>`
      ).join('');
    }

    window.setFolder = name => {
      activeFolder = name;
      renderFolderTabs();
      renderChannelList();
    };

    // ── Channel list ────────────────────────────────────────────────────────
    function chColor(t) {
      return COLORS[Math.abs([...t].reduce((a, c) => a + c.charCodeAt(0), 0)) % COLORS.length];
    }

    function renderChannelList() {
      const list = document.getElementById('chlist');
      list.innerHTML = '';
      const searchQ = (document.querySelector('#sp-channels .sw input')?.value || '').toLowerCase();
      let filtered = allChs;
      if (activeFolder) {
        filtered = filtered.filter(c => c.folders && c.folders.includes(activeFolder));
      }
      if (searchQ) {
        filtered = filtered.filter(c => c.title.toLowerCase().includes(searchQ));
      }
      if (!filtered.length) {
        list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--muted);font-size:12px">No channels</div>';
        return;
      }
      filtered.forEach(ch => appendCh(ch, list));
    }

    function appendCh(ch, list) {
      if (!list) list = document.getElementById('chlist');
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
      const folderBadge = ch.folders?.length
        ? `<span class="ch-folder">${esc(ch.folders[0])}</span>` : '';
      div.innerHTML = `
        <div class="av" style="background:${color}20;color:${color}">${ch.title.charAt(0).toUpperCase()}</div>
        <div class="ci"><div class="cn">${esc(ch.title)}</div><div class="cm">${mem} ${folderBadge}</div></div>
        <div class="ck">${sel ? chk() : ''}</div>`;
      list.appendChild(div);
    }

    function toggleCh(id, el) {
      if (selChs.has(id)) { selChs.delete(id); el.classList.remove('sel'); el.querySelector('.ck').innerHTML = ''; }
      else { selChs.add(id); el.classList.add('sel'); el.querySelector('.ck').innerHTML = chk(); }
      const btn = document.getElementById('vmbtn');
      btn.textContent = `${selChs.size} selected`;
      updSelAllBtn();
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

    window.selAllChs = () => {
      let visible = allChs;
      if (activeFolder) {
        visible = visible.filter(c => c.folders && c.folders.includes(activeFolder));
      }
      const visibleIds = visible.map(c => c.id);
      const allSelected = visibleIds.length > 0 && visibleIds.every(id => selChs.has(id));

      if (allSelected) {
        visibleIds.forEach(id => selChs.delete(id));
      } else {
        visibleIds.forEach(id => selChs.add(id));
      }

      document.getElementById('vmbtn').textContent = `${selChs.size} selected`;
      updSelAllBtn();
      renderChannelList();
      if (selChs.size > 0) loadMedia();
      else {
        items.clear(); selItems.clear(); allMediaKeys = [];
        const grid = document.getElementById('mgrid');
        grid.innerHTML = ''; grid.style.display = 'none';
        document.getElementById('empty').style.display = 'flex';
        updPillCounts();
      }
    };

    function updSelAllBtn() {
      const btn = document.getElementById('selall-ch');
      if (!btn) return;
      let visible = allChs;
      if (activeFolder) visible = visible.filter(c => c.folders && c.folders.includes(activeFolder));
      const allSel = visible.length > 0 && visible.every(c => selChs.has(c.id));
      btn.textContent = allSel ? 'Deselect All' : 'Select All';
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
      renderChannelList();
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
    let vsRAF = null;

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
      if (!grid) return;

      if (!filteredKeys.length) {
        grid.innerHTML = '';
        grid.style.height = '';
        return;
      }

      // Recalc viewport if needed (first render)
      if (!viewportHeight) {
        viewportHeight = grid.clientHeight || 600;
        const cardW = (viewMode === 'gallery') ? 250 : 152;
        columns = Math.floor((grid.clientWidth - 12) / (cardW + 8)) || 1;
      }

      const gap = 8;
      const rowH = (viewMode === 'gallery') ? 258 : 160;
      const totalRows = Math.ceil(filteredKeys.length / columns);
      const totalH = totalRows * rowH;

      // Set grid to full scroll height
      grid.style.height = totalH + 'px';
      grid.style.position = 'relative';

      const st = grid.scrollTop;
      const buffer = 3; // extra rows above/below
      const startRow = Math.max(0, Math.floor(st / rowH) - buffer);
      const endRow = Math.min(totalRows, Math.ceil((st + viewportHeight) / rowH) + buffer);

      const startIdx = startRow * columns;
      const endIdx = Math.min(endRow * columns, filteredKeys.length);
      const visibleKeys = filteredKeys.slice(startIdx, endIdx);
      const visibleSet = new Set(visibleKeys);

      // Remove out-of-view cards
      for (const child of Array.from(grid.children)) {
        if (!visibleSet.has(child.dataset.key)) {
          grid.removeChild(child);
        }
      }

      // Track existing
      const existing = new Map();
      for (const c of grid.children) existing.set(c.dataset.key, c);

      // Add/position visible cards
      visibleKeys.forEach((key, i) => {
        const item = items.get(key);
        if (!item) return;

        let el = existing.get(key);
        if (!el) {
          el = makeCard(key, item);
          thumbObs.observe(el);
          el.dataset.obs = '1';
          grid.appendChild(el);
        }

        // Position via CSS grid — use transform for GPU acceleration
        const globalIdx = startIdx + i;
        const row = Math.floor(globalIdx / columns);
        const col = globalIdx % columns;
        el.style.position = 'absolute';
        el.style.width = `calc((100% - ${(columns - 1) * gap}px) / ${columns})`;
        el.style.height = (rowH - gap) + 'px';
        el.style.left = `calc(${col} * (100% - ${(columns - 1) * gap}px) / ${columns} + ${col * gap}px)`;
        el.style.top = (row * rowH) + 'px';
      });
    }

    const mgrid = document.getElementById('mgrid');
    if (mgrid) {
      mgrid.addEventListener('scroll', () => {
        if (vsRAF) return;
        vsRAF = requestAnimationFrame(() => { vsRAF = null; renderVirtual(); });
      }, { passive: true });
    }

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

      // Search Filter — fuzzy across filename, caption, date
      if (currentSearch) {
        const words = currentSearch.split(/\s+/).filter(Boolean);
        const scored = [];
        for (const k of keys) {
          const it = items.get(k);
          if (!it) continue;
          const hay = `${it.filename || ''} ${it.caption || ''} ${it.date || ''} ${it.type || ''}`.toLowerCase();
          // Every word must appear somewhere
          if (!words.every(w => hay.includes(w))) continue;
          // Score: exact filename match > filename starts-with > filename contains > caption match
          let score = 0;
          const fn = (it.filename || '').toLowerCase();
          const cap = (it.caption || '').toLowerCase();
          for (const w of words) {
            if (fn === w) score += 100;
            else if (fn.startsWith(w)) score += 50;
            else if (fn.includes(w)) score += 20;
            else if (cap.includes(w)) score += 5;
          }
          scored.push({ k, score });
        }
        scored.sort((a, b) => b.score - a.score);
        keys = scored.map(s => s.k);
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

    // Thumbnail loading with concurrency throttle
    const THUMB_CONCURRENCY = 8;
    let thumbActive = 0;
    const thumbQueue = [];

    function drainThumbQueue() {
      while (thumbActive < THUMB_CONCURRENCY && thumbQueue.length) {
        const job = thumbQueue.shift();
        thumbActive++;
        job().finally(() => { thumbActive--; drainThumbQueue(); });
      }
    }

    function loadThumb(cardEl, item) {
      if (!item) return;
      const url = `/api/thumb/${item.channel_id}/${item.msg_id}`;

      thumbQueue.push(() => new Promise(resolve => {
        const nth = cardEl.querySelector('.nth');
        if (!nth) { resolve(); return; }

        if (item.type === 'video') {
          // Video: set poster from thumb, lazy-load full video on hover
          const vid = document.createElement('video');
          vid.poster = url; vid.muted = true; vid.loop = true;
          vid.setAttribute('playsinline', '');
          vid.className = 'card-vid';
          nth.replaceWith(vid);
          cardEl.style.setProperty('--bg-img', `url(${url})`);
          cardEl.onmouseenter = () => {
            if (!vid.src) { vid.src = `/api/preview/${item.channel_id}/${item.msg_id}`; vid.load(); }
            vid.play().catch(() => {});
          };
          cardEl.onmouseleave = () => vid.pause();
          resolve();
          return;
        }

        // Photo/doc: load thumbnail image
        const img = new Image();
        img.onload = () => {
          cardEl.style.setProperty('--bg-img', `url(${url})`);
          nth.replaceWith(img);
          resolve();
        };
        img.onerror = () => resolve();
        img.src = url;
      }));
      drainThumbQueue();
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

    // ── Download (to user's device via browser) ──────────────────────────────
    window.doDl = async () => {
      const dItems = [...selItems].map(k => items.get(k)).filter(Boolean);
      if (!dItems.length) return;

      toast(`Downloading ${dItems.length} file${dItems.length > 1 ? 's' : ''} to your device…`, 'ok');

      // Show progress drawer
      const job_id = 'local-' + Date.now();
      document.getElementById('pdrawer').classList.add('open');
      const card = document.createElement('div');
      card.className = 'pitem'; card.id = `j-${job_id}`;
      card.innerHTML = `
        <div class="pn">💾 ${dItems.length} file${dItems.length !== 1 ? 's' : ''} → your device</div>
        <div class="pb-wrap"><div class="pb" id="pb-${job_id}"></div></div>
        <div class="pm"><span id="pc-${job_id}">Starting…</span><span id="pp-${job_id}">0%</span></div>
        <div class="perr" id="pe-${job_id}" style="display:none"></div>`;
      document.getElementById('plist').prepend(card);

      let done = 0;
      let errors = 0;

      for (const item of dItems) {
        const fname = item.filename || `${item.msg_id}`;
        try {
          document.getElementById(`pc-${job_id}`).textContent = fname;

          // Use a hidden <a> to trigger a browser save-as download
          const link = document.createElement('a');
          link.href = `/api/file/${item.channel_id}/${item.msg_id}`;
          link.download = fname;
          link.style.display = 'none';
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);

          // Small delay between files so browser can handle each one
          if (dItems.length > 1) await new Promise(r => setTimeout(r, 800));
        } catch (e) {
          errors++;
          const pe = document.getElementById(`pe-${job_id}`);
          pe.style.display = 'block';
          pe.textContent = `${fname}: ${e.message}`;
        }

        done++;
        const pct = Math.round(done / dItems.length * 100);
        document.getElementById(`pb-${job_id}`).style.width = pct + '%';
        document.getElementById(`pp-${job_id}`).textContent = pct + '%';
      }

      document.getElementById(`pc-${job_id}`).textContent = errors
        ? `✅ ${done - errors} saved, ❌ ${errors} failed`
        : `✅ ${done} file${done !== 1 ? 's' : ''} sent to browser`;
      toast(`Download complete — ${done} files`, 'ok');
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

    document.getElementById('msearch')?.addEventListener('input', debounce(e => searchMedia(e.target.value), 80));

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