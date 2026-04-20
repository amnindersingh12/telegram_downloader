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
    let lastSelKey = null;   // For shift-click
    let lastSelChId = null;  // For sidebar shift-click
    let filter = 'all';
    let stream = null;
    let paused = false;
    let renderBuf = [];
    let cardIdx = 0;
    let viewMode = 'gallery';
    const gap = 12;

    const COLORS = ['#4f8eff', '#ff6b8a', '#35d47b', '#ffb84f', '#a78bff', '#ff9b3d', '#0ecfcf'];

    const thumbObs = new IntersectionObserver(entries => {
      entries.forEach(e => {
        if (e.isIntersecting) {
          loadThumb(e.target, items.get(e.target.dataset.key));
          thumbObs.unobserve(e.target);
        }
      });
    }, { rootMargin: '2500px' });

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

    async function initApp() {
      allChs = []; selChs.clear(); items.clear(); selItems.clear(); updSelCount();
      folders = []; activeFolder = '';
      
      // Initialize persistent sync activity log - and await other mirrors
      await loadMirrors();
      loadSyncRules();
      
      // Add sync activity card - if it doesn't exist, it will be prepended (top)
      if (!document.getElementById('job-sync_activity')) {
        addMirrorCard('sync_activity', 'Live', 'Sync', 'Running');
      }
      
      const esSync = new EventSource('/api/download/sync_activity/progress');
      esSync.onmessage = e => {
        const d = JSON.parse(e.data);
        if (d.logs) updateMirrorLogs('sync_activity', d.logs);
      };

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
        // Deduplicate: remove if exists, then push (to update)
        allChs = allChs.filter(c => c.id !== d.id);
        allChs.push(d);
        if (!chRenderPending) {
          chRenderPending = true;
          requestAnimationFrame(() => { chRenderPending = false; renderChannelList(); });
        }
      };
      es.onerror = () => es.close();
      updViewport();
      startLiveUpdates();
      initHoverPreview();
      startPerformanceMonitoring();
    }

    function startPerformanceMonitoring() {
      setInterval(async () => {
        try {
          const d = await api('/api/health');
          const el = document.getElementById('q-status');
          if (el) {
            if (d.thumb_queue > 0) {
              el.textContent = `⚡ Prefetching ${d.thumb_queue} thumbs...`;
              el.style.display = 'block';
            } else {
              el.style.display = 'none';
            }
          }
        } catch {}
      }, 5000);
    }

    function initHoverPreview() {
      const tp = document.getElementById('true-preview');
      let peekT;

      window.showTruePreview = (key, e) => {
        clearTimeout(peekT);
        peekT = setTimeout(() => {
          const it = items.get(key);
          if (!it) return;
          
          tp.innerHTML = '';
          if (it.type === 'video') {
            const v = document.createElement('video');
            v.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
            v.autoplay = true; v.muted = true; v.loop = true;
            tp.appendChild(v);
          } else if (it.type === 'photo') {
            const img = document.createElement('img');
            img.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
            tp.appendChild(img);
          } else {
            return; // No peek for docs
          }
          
          tp.style.display = 'block';
          moveTruePreview(e);
        }, 300); // 300ms debounce
      };

      window.hideTruePreview = () => {
        clearTimeout(peekT);
        tp.style.display = 'none';
        tp.innerHTML = '';
      };

      window.moveTruePreview = (e) => {
        if (tp.style.display !== 'block') return;
        let x = e.clientX + 20;
        let y = (e.type === 'keydown' ? e.clientY + 50 : e.clientY + 20);
        
        const pad = 15;
        if (x + 450 > window.innerWidth) x = e.clientX - 470;
        if (x < pad) x = pad;
        if (y + tp.offsetHeight > window.innerHeight) y = window.innerHeight - tp.offsetHeight - pad;
        if (y < pad) y = pad;
        
        tp.style.left = x + 'px'; tp.style.top = y + 'px';
      };

      window.updGridSize = (val) => {
        document.documentElement.style.setProperty('--row-h', val + 'px');
        renderVirtual();
      };

      window.peekByKeys = (dir, e) => {
        if (!filteredKeys.length) return;
        
        // Remove old focus
        document.querySelectorAll('.mc.kb-focus').forEach(el => el.classList.remove('kb-focus'));

        const mgrid = document.getElementById('mgrid');
        let columns = Math.floor(mgrid.clientWidth / (parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220));
        if (columns < 1) columns = 1;

        if (dir === 'left') peekIdx--;
        else if (dir === 'right') peekIdx++;
        else if (dir === 'up') peekIdx -= columns;
        else if (dir === 'down') peekIdx += columns;

        if (peekIdx < 0) peekIdx = 0;
        if (peekIdx >= filteredKeys.length) peekIdx = filteredKeys.length - 1;

        const key = filteredKeys[peekIdx];
        const item = items.get(key);
        if (!item) return;

        // Ensure visible
        const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220;
        const row = Math.floor(peekIdx / columns);
        const targetTop = row * rowH;
        
        if (targetTop < mgrid.scrollTop) mgrid.scrollTop = targetTop;
        else if (targetTop + rowH > mgrid.scrollTop + mgrid.clientHeight) mgrid.scrollTop = targetTop + rowH - mgrid.clientHeight;

        // Show preview
        const cards = document.querySelectorAll('.mc');
        let cardEl = null;
        for (const c of cards) { if (c.dataset.key === key) { cardEl = c; break; } }
        
        let rect = { left: 100, top: 100 };
        if (cardEl) {
          cardEl.classList.add('kb-focus');
          rect = cardEl.getBoundingClientRect();
        }

        // Trigger preview
        clearTimeout(peekT);
        tp.innerHTML = '';
        if (item.type === 'video') {
          const v = document.createElement('video');
          v.src = `/api/preview/${item.channel_id}/${item.msg_id}`;
          v.autoplay = true; v.muted = true; v.loop = true;
          tp.appendChild(v);
        } else if (item.type === 'photo') {
          const img = document.createElement('img');
          img.src = `/api/preview/${item.channel_id}/${item.msg_id}`;
          tp.appendChild(img);
        }
        tp.style.display = 'block';
        tp.style.left = (rect.left + 50) + 'px';
        tp.style.top = (rect.top + 50) + 'px';
      };
    }

    function startLiveUpdates() {
      if (window.updateES) window.updateES.close();
      const es = new EventSource('/api/updates');
      window.updateES = es;
      es.onmessage = e => {
        const ev = JSON.parse(e.data);
        if (ev.type === 'new_message') {
          const ch = allChs.find(c => c.id === ev.channel_id);
          if (ch) {
            ch.unread = (ch.unread || 0) + 1;
            ch.pulsing = true;
            renderChannelList();
            setTimeout(() => { ch.pulsing = false; renderChannelList(); }, 600);
          }
        }
      };
      es.onerror = () => { es.close(); setTimeout(startLiveUpdates, 5000); };
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
      
      const hasUnreadTab = folders.some(f => f.name.toLowerCase() === 'unread');
      const tabs = [
        {name: '', emoji: '🏠', label: 'All'}
      ];
      
      // Only add virtual unread if user doesn't have an official one
      if (!hasUnreadTab) {
        tabs.push({name: 'unread_virtual', emoji: '🔔', label: 'Unread'});
      }
      
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

    let currentVisibleChs = [];

    function renderChannelList() {
      const list = document.getElementById('chlist');
      if (!list) return;

      list.innerHTML = '';
      const searchQ = (document.querySelector('#sp-channels .sw input')?.value || '').toLowerCase();
      let filtered = allChs;
      
      const isActiveUnread = activeFolder === 'unread_virtual' || activeFolder.toLowerCase() === 'unread';

      if (isActiveUnread) {
        // Show ONLY unread channels
        filtered = filtered.filter(c => (c.unread || 0) > 0);
      } else if (activeFolder === '') {
        // All view: Show ONLY read channels (per user request)
        filtered = filtered.filter(c => (c.unread || 0) === 0);
      } else {
        // Custom folder view
        filtered = filtered.filter(c => c.folders && c.folders.includes(activeFolder));
      }

      if (searchQ) {
        filtered = filtered.filter(c => c.title.toLowerCase().includes(searchQ));
      }

      currentVisibleChs = filtered.map(c => c.id);
      
      if (!filtered.length) {
        list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--muted);font-size:12px">No channels</div>';
        return;
      }

      filtered.forEach(ch => appendCh(ch, list));
      updMirDatalist();
    }

    function updMirDatalist() {
      const dlAll = document.getElementById('ch-datalist-all');
      const dlTarget = document.getElementById('ch-datalist-target');
      if (!dlAll || !dlTarget) return;

      const items = allChs.map(ch =>
        `<option value="${ch.id}">${esc(ch.title)} (${ch.type}${ch.username ? ', @' + ch.username : ''})</option>`
      );
      dlAll.innerHTML = items.join('');

      const targetItems = allChs
        .filter(ch => ch.can_post)
        .map(ch =>
          `<option value="${ch.id}">${esc(ch.title)} (Your ${ch.type})</option>`
        );
      dlTarget.innerHTML = targetItems.join('');
    }

    function appendCh(ch, list) {
      const color = chColor(ch.title);
      const sel = selChs.has(ch.id);
      const div = document.createElement('div');
      div.className = 'ch' + (sel ? ' sel' : '');
      div.dataset.id = ch.id;
      div.onclick = (e) => toggleCh(ch.id, div, e);
      const mem = ch.members
        ? `${ch.members > 999 ? (ch.members / 1000).toFixed(1) + 'k' : ch.members} members`
        : ch.type;
      const folderBadge = ch.folders?.length
        ? `<span class="ch-folder">${esc(ch.folders[0])}</span>` : '';
      
      const unreadIndicator = ch.unread > 0 
        ? `<div class="ch-dot ${ch.pulsing ? 'pulse' : ''}"></div>` 
        : '';

      div.innerHTML = `
        <div class="av" style="background:${color}15;color:${color}">${ch.title.charAt(0).toUpperCase()}</div>
        <div class="ci"><div class="cn">${esc(ch.title)}</div><div class="cm">${mem} ${folderBadge}</div></div>
        <div class="cr">${unreadIndicator}<div class="ck">${sel ? chk() : ''}</div></div>`;
      list.appendChild(div);
    }

    function toggleCh(id, el, e) {
      if (e && e.shiftKey && lastSelChId && currentVisibleChs.includes(lastSelChId)) {
        const idxA = currentVisibleChs.indexOf(lastSelChId);
        const idxB = currentVisibleChs.indexOf(id);
        const [start, end] = idxA < idxB ? [idxA, idxB] : [idxB, idxA];
        for (let i = start; i <= end; i++) selChs.add(currentVisibleChs[i]);
      } else {
        if (selChs.has(id)) {
          selChs.delete(id);
          lastSelChId = null;
        } else {
          selChs.add(id);
          lastSelChId = id;
        }
      }
      
      const btn = document.getElementById('vmbtn');
      if (btn) btn.textContent = `${selChs.size} selected`;
      updSelAllBtn();
      renderChannelList();

      // Auto-load immediately on selection change
      if (selChs.size > 0) loadMedia();
      else {
        // Clear grid when nothing selected
        items.clear(); selItems.clear(); allMediaKeys = [];
        const grid = document.getElementById('mgrid');
        if (grid) {
          grid.innerHTML = ''; grid.style.display = 'none';
        }
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

      const btn = document.getElementById('vmbtn');
      if (btn) btn.textContent = `${selChs.size} selected`;
      updSelAllBtn();
      renderChannelList();
      if (selChs.size > 0) loadMedia();
      else {
        items.clear(); selItems.clear(); allMediaKeys = [];
        const grid = document.getElementById('mgrid');
        if (grid) {
          grid.innerHTML = ''; grid.style.display = 'none';
        }
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

      const sf = document.querySelector('.sf');
      if (sf) {
        if (selChs.size > 0) sf.classList.add('active');
        else sf.classList.remove('active');
      }
      updSelCount();
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
      document.querySelectorAll('.sidebar .sidebar-pane').forEach(p => p.classList.remove('active'));
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
      const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220;
      columns = Math.floor((grid.clientWidth - 24) / rowH) || 1;
      renderVirtual();
    }

    function renderVirtual() {
      const grid = document.getElementById('mgrid');
      if (!grid) return;

      if (!filteredKeys.length) {
        grid.innerHTML = ''; grid.style.height = '';
        return;
      }

      const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220;
      columns = Math.floor((grid.clientWidth - 24) / rowH) || 1;
      const totalRows = Math.ceil(filteredKeys.length / columns);
      const totalH = totalRows * rowH;

      let spacer = grid.querySelector('.vs-spacer');
      if (!spacer) {
        spacer = document.createElement('div');
        spacer.className = 'vs-spacer';
        spacer.style.width = '1px';
        spacer.style.position = 'absolute';
        grid.prepend(spacer);
      }
      spacer.style.height = totalH + 'px';
      grid.style.position = 'relative';

      const st = grid.scrollTop;
      const vh = grid.clientHeight || 800;
      const buffer = 4;
      const startRow = Math.max(0, Math.floor(st / rowH) - buffer);
      const endRow = Math.min(totalRows, Math.ceil((st + vh) / rowH) + buffer);

      const startIdx = startRow * columns;
      const endIdx = Math.min(endRow * columns, filteredKeys.length);
      const visibleKeys = filteredKeys.slice(startIdx, endIdx);
      const visibleSet = new Set(visibleKeys);

      // Remove out-of-view cards
      for (const child of Array.from(grid.children)) {
        if (child.classList.contains('lasso') || child.classList.contains('vs-spacer')) continue;
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

        const globalIdx = startIdx + i;
        const row = Math.floor(globalIdx / columns);
        const col = globalIdx % columns;
        el.style.position = 'absolute';
        
        const totalGapWidth = (columns - 1) * gap;
        const itemWidth = (grid.clientWidth - 24 - totalGapWidth) / columns;
        
        el.style.width = itemWidth + 'px';
        el.style.height = (rowH - gap) + 'px';
        el.style.left = (12 + col * (itemWidth + gap)) + 'px';
        el.style.top = (row * rowH + 12) + 'px';
      });
    }

    const mgrid = document.getElementById('mgrid');
    if (mgrid) {
      mgrid.addEventListener('scroll', () => {
        if (vsRAF) return;
        vsRAF = requestAnimationFrame(() => { vsRAF = null; renderVirtual(); });
      }, { passive: true });

      // ── Lasso Selection (Drag-to-select) ───────────────────────────────────
      let lasso = null, startX, startY;
      let initialSel = new Set();

      mgrid.onmousedown = e => {
        if (e.target.closest('button, input, select, .mpreview')) return;
        
        const rect = mgrid.getBoundingClientRect();
        startX = e.clientX - rect.left;
        startY = e.clientY - rect.top + mgrid.scrollTop;
        
        lasso = document.createElement('div');
        lasso.className = 'lasso';
        mgrid.appendChild(lasso);
        
        initialSel = new Set(selItems);
        
        const onMove = ev => {
          if (!lasso) return;
          const currentX = ev.clientX - rect.left;
          const currentY = ev.clientY - rect.top + mgrid.scrollTop;
          
          const left = Math.min(startX, currentX);
          const top = Math.min(startY, currentY);
          const width = Math.abs(startX - currentX);
          const height = Math.abs(startY - currentY);
          
          lasso.style.left = left + 'px';
          lasso.style.top = top + 'px';
          lasso.style.width = width + 'px';
          lasso.style.height = height + 'px';
          
          const lassoBounds = { left, top, right: left + width, bottom: top + height };
          const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220;
          const totalGapWidth = (columns - 1) * gap;
          const itemWidth = (mgrid.clientWidth - 24 - totalGapWidth) / columns;
          const colW = itemWidth + gap;
          
          const rStart = Math.max(0, Math.floor((lassoBounds.top - 12) / rowH));
          const rEnd = Math.floor((lassoBounds.bottom - 12) / rowH);
          const cStart = Math.max(0, Math.floor((lassoBounds.left - 12) / colW));
          const cEnd = Math.min(columns - 1, Math.floor((lassoBounds.right - 12) / colW));
          
          const isMod = ev.shiftKey || ev.ctrlKey || ev.metaKey;
          const lassoed = new Set();
          for (let r = rStart; r <= rEnd; r++) {
            for (let c = cStart; c <= cEnd; c++) {
              const idx = r * columns + c;
              if (idx >= 0 && idx < filteredKeys.length) lassoed.add(filteredKeys[idx]);
            }
          }
          if (!isMod) selItems.clear();
          if (isMod) initialSel.forEach(k => selItems.add(k));
          lassoed.forEach(k => selItems.add(k));
          renderVirtual();
          updSelCount();

          const threshold = 40;
          const speed = 15;
          const rectScroll = mgrid.getBoundingClientRect();
          if (ev.clientY < rectScroll.top + threshold) mgrid.scrollTop -= speed;
          else if (ev.clientY > rectScroll.bottom - threshold) mgrid.scrollTop += speed;
        };
        
        const onUp = () => {
          if (lasso) { lasso.remove(); lasso = null; }
          document.removeEventListener('mousemove', onMove);
          document.removeEventListener('mouseup', onUp);
        };
        
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
      };
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

      if (filter !== 'all') {
        keys = keys.filter(k => {
          const it = items.get(k);
          return it && it.type === filter;
        });
      }

      if (currentSearch) {
        const words = currentSearch.split(/\s+/).filter(Boolean);
        const scored = [];
        for (const k of keys) {
          const it = items.get(k);
          if (!it) continue;
          const hay = `${it.filename || ''} ${it.caption || ''} ${it.date || ''} ${it.type || ''}`.toLowerCase();
          if (!words.every(w => hay.includes(w))) continue;
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
      if (grid) {
        grid.style.display = 'block';
        grid.innerHTML = '';
        grid.style.paddingTop = '0'; grid.style.paddingBottom = '0';
        grid.onscroll = renderVirtual;
      }
      document.getElementById('empty').style.display = 'none';
      document.getElementById('sbar').classList.add('active');
      document.getElementById('pause-btn').style.display = 'block';
      document.getElementById('stop-btn').style.display = 'block';
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
          document.getElementById('stop-btn').style.display = 'none';
          updPillCounts(); applyFilters(); return;
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
      stream.onerror = () => { if (stream) { stream.close(); stream = null; } document.getElementById('sbar-txt').textContent = `${cnt} items`; };
    };

    window.togglePause = () => {
      paused = !paused;
      document.getElementById('pause-btn').textContent = paused ? '▶ Resume' : '⏸ Pause';
      if (!paused) renderVirtual();
    };

    window.stopStream = () => {
      if (stream) {
        stream.close(); stream = null;
        document.getElementById('sbar-txt').textContent = 'Stopped';
        document.getElementById('pause-btn').style.display = 'none';
        document.getElementById('stop-btn').style.display = 'none';
        toast('Media loading stopped', '');
      }
    };

    function makeCard(key, item) {
      const sel = selItems.has(key);
      const el = document.createElement('div');
      el.className = 'mc' + (sel ? ' sel' : '');
      el.dataset.key = key;
      el.onclick = (e) => {
        if (e.target.closest('.mchk')) {
          e.stopPropagation(); toggleSel(key, el, e);
        } else if (e.target.closest('.mpreview')) {
          e.stopPropagation(); openPreview(key);
        } else {
          if (e.ctrlKey || e.metaKey || selItems.size > 0 || window.innerWidth <= 768) toggleSel(key, el, e);
          else openPreview(key);
        }
      };
      el.onmouseenter = (e) => showTruePreview(key, e);
      el.onmouseleave = () => hideTruePreview();
      el.onmousemove = (e) => moveTruePreview(e);
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
          const vid = document.createElement('video');
          vid.poster = url; vid.muted = true; vid.loop = true;
          vid.setAttribute('playsinline', '');
          vid.className = 'card-vid';
          nth.replaceWith(vid);
          cardEl.style.setProperty('--bg-img', `url(${url})`);
          let playT;
          cardEl.onmouseenter = () => {
            playT = setTimeout(() => {
              if (!vid.src) { vid.src = `/api/preview/${item.channel_id}/${item.msg_id}`; vid.load(); }
              vid.play().catch(() => {});
            }, 400);
          };
          cardEl.onmouseleave = () => { clearTimeout(playT); vid.pause(); };
          resolve(); return;
        }
        const img = new Image();
        img.onload = () => { cardEl.style.setProperty('--bg-img', `url(${url})`); nth.replaceWith(img); resolve(); };
        img.onerror = () => resolve();
        img.src = url;
      }));
      drainThumbQueue();
    }

    function toggleSel(key, el, e) {
      if (e && e.shiftKey && lastSelKey && filteredKeys.includes(lastSelKey)) {
        const idxA = filteredKeys.indexOf(lastSelKey);
        const idxB = filteredKeys.indexOf(key);
        const [start, end] = idxA < idxB ? [idxA, idxB] : [idxB, idxA];
        for (let i = start; i <= end; i++) selItems.add(filteredKeys[i]);
      } else {
        if (selItems.has(key)) { selItems.delete(key); lastSelKey = null; }
        else { selItems.add(key); lastSelKey = key; }
      }
      renderVirtual(); updSelCount();
    }

    window.selAll = () => {
      const allKeys = [...items.keys()];
      const allSel = allKeys.every(k => selItems.has(k));
      if (allSel) allKeys.forEach(k => selItems.delete(k));
      else allKeys.forEach(k => selItems.add(k));
      renderVirtual(); updSelCount();
    };

    function updSelCount() {
      const n = selItems.size;
      const el = document.getElementById('selc');
      if (el) el.textContent = n;
      const bb = document.querySelector('.bb');
      const sf = document.querySelector('.sf');
      if (bb) {
        if (n > 0) {
          bb.classList.add('active');
          if (sf && sf.classList.contains('active')) bb.classList.add('shifted');
          else bb.classList.remove('shifted');
        } else { bb.classList.remove('active', 'shifted'); }
      }
      let totalSize = 0;
      selItems.forEach(k => { const it = items.get(k); if (it) totalSize += it.size || 0; });
      const sizeStr = n > 0 ? ` · ${hrSize(totalSize)}` : '';
      const bbc = document.querySelector('.bbc');
      if (bbc) bbc.innerHTML = `<b>${n}</b> item${n !== 1 ? 's' : ''}${sizeStr} selected`;
      const btn = document.getElementById('dlbtn');
      if (btn) btn.disabled = n === 0;
      const sa = document.getElementById('selall-btn');
      if (sa) {
        const totalVisible = filteredKeys.length;
        const allSel = totalVisible > 0 && n >= totalVisible;
        sa.innerHTML = allSel ? '<span>✓</span> Deselect All' : '<span>✓</span> Select All';
        sa.classList.toggle('all-selected', allSel);
      }
    }

    window.searchMedia = q => { currentSearch = q.toLowerCase(); applyFilters(); };
    window.sortMedia = by => { currentSort = by; applyFilters(); };
    
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
    function lbClose() { lb.classList.remove('open'); document.body.style.overflow = ''; _lbCleanup(); }
    function _lbCleanup() { if (lbActiveMedia) { lbActiveMedia.pause?.(); lbActiveMedia.src = ''; lbActiveMedia = null; } }
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
        img.onerror = () => { const d = document.createElement('div'); d.textContent = 'Preview not available'; d.style.cssText = 'color:rgba(255,255,255,.4);font-size:13px;padding:40px'; resolve(d); };
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
      if (dir === 0 || !lbCnt.firstChild) {
        lbCnt.innerHTML = '<div class="lb-spinner"></div>';
        const el = await _buildMedia(item);
        if (item.type === 'video') lbActiveMedia = el;
        lbCnt.innerHTML = ''; lbCnt.appendChild(el);
        el.style.animation = 'lb-fadein 0.3s ease both';
        lbNavigating = false; _preload(idx+1); _preload(idx-1); return;
      }
      const outgoing = lbCnt.firstChild;
      const SLIDE_DUR = 320;
      outgoing.style.animation = `lb-slide-out-${dir > 0 ? 'left' : 'right'} ${SLIDE_DUR}ms cubic-bezier(0.4,0,0.2,1) both`;
      const [el] = await Promise.all([_buildMedia(item), new Promise(r => setTimeout(r, SLIDE_DUR * 0.35))]);
      _lbCleanup(); if (item.type === 'video') lbActiveMedia = el;
      lbCnt.innerHTML = ''; lbCnt.appendChild(el);
      el.style.animation = `lb-slide-in-${dir > 0 ? 'right' : 'left'} ${SLIDE_DUR}ms cubic-bezier(0.4,0,0.2,1) both`;
      lbNavigating = false; _preload(idx+1); _preload(idx-1);
    }
    function _preload(idx) {
      const k = filteredKeys[idx]; if (!k) return;
      const it = items.get(k); if (!it || it.type === 'video') return;
      const p = new Image(); p.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
    }
    function lbNav(dir) { const next = lbIdx + dir; if (next < 0 || next >= filteredKeys.length) return; lbIdx = next; lbLoad(lbIdx, dir); }

    const lbCloseBtn = document.getElementById('lb-close');
    if (lbCloseBtn) lbCloseBtn.onclick = lbClose;
    const lbPrevBtn = document.getElementById('lb-prev');
    if (lbPrevBtn) lbPrevBtn.onclick = () => lbNav(-1);
    const lbNextBtn = document.getElementById('lb-next');
    if (lbNextBtn) lbNextBtn.onclick = () => lbNav(1);
    const lbBgBtn = document.getElementById('lb-bg');
    if (lbBgBtn) lbBgBtn.onclick = lbClose;

    let touchXLb = 0;
    lb.addEventListener('touchstart', e => { touchXLb = e.touches[0].clientX; }, { passive: true });
    lb.addEventListener('touchend', e => { const dx = e.changedTouches[0].clientX - touchXLb; if (Math.abs(dx) > 40) lbNav(dx < 0 ? 1 : -1); }, { passive: true });

    window.openPreview = (key) => lbOpen(key);
    window.toggleView = () => {
      viewMode = viewMode === 'grid' ? 'gallery' : 'grid';
      document.getElementById('mgrid').classList.toggle('gallery-mode', viewMode === 'gallery');
      document.getElementById('vwtbtn').textContent = viewMode === 'gallery' ? '☷ Grid view' : '🖼️ Gallery view';
      if (viewMode === 'gallery') { selItems.clear(); updSelCount(); document.getElementById('mgrid').style.paddingTop = '0'; document.getElementById('mgrid').style.paddingBottom = '0'; }
      else { updViewport(); }
    };
    window.openGallery = () => { const ids = [...selChs].join(','); window.open('/gallery' + (ids ? '?channels=' + ids : ''), '_blank'); };

    window.doDl = async () => {
      const dItems = [...selItems].map(k => items.get(k)).filter(Boolean);
      if (!dItems.length) return;
      toast(`Downloading ${dItems.length} file${dItems.length > 1 ? 's' : ''} to your device…`, 'ok');
      const job_id = 'local-' + Date.now();
      document.getElementById('pdrawer').classList.add('open');
      const card = document.createElement('div');
      card.className = 'pitem'; card.id = `j-${job_id}`;
      card.innerHTML = `<div class="pn">💾 ${dItems.length} file${dItems.length !== 1 ? 's' : ''} → your device</div><div class="pb-wrap"><div class="pb" id="pb-${job_id}"></div></div><div class="pm"><span id="pc-${job_id}">Starting…</span><span id="pp-${job_id}">0%</span></div><div class="perr" id="pe-${job_id}" style="display:none"></div>`;
      document.getElementById('plist').prepend(card);
      let done = 0, errors = 0;
      for (const item of dItems) {
        const fname = item.filename || `${item.msg_id}`;
        try {
          document.getElementById(`pc-${job_id}`).textContent = fname;
          const link = document.createElement('a');
          link.href = `/api/file/${item.channel_id}/${item.msg_id}`;
          link.download = fname; link.style.display = 'none';
          document.body.appendChild(link); link.click(); document.body.removeChild(link);
          if (dItems.length > 1) await new Promise(r => setTimeout(r, 800));
        } catch (e) { errors++; const pe = document.getElementById(`pe-${job_id}`); pe.style.display = 'block'; pe.textContent = `${fname}: ${e.message}`; }
        done++; const pct = Math.round(done / dItems.length * 100);
        document.getElementById(`pb-${job_id}`).style.width = pct + '%';
        document.getElementById(`pp-${job_id}`).textContent = pct + '%';
      }
      document.getElementById(`pc-${job_id}`).textContent = errors ? `✅ ${done - errors} saved, ❌ ${errors} failed` : `✅ ${done} file${done !== 1 ? 's' : ''} sent to browser`;
      toast(`Download complete — ${done} files`, 'ok');
    };

    function updateMirrorLogs(id, logs) {
      const box = document.getElementById(`logs-${id}`); if (!box) return;
      const atBottom = box.scrollHeight - box.scrollTop <= box.clientHeight + 40;
      box.innerHTML = logs.map(l => `<div style="word-wrap:break-word;word-break:break-all;margin-bottom:2px" title="${esc(l)}">${esc(l)}</div>`).join('');
      if (atBottom) box.scrollTop = box.scrollHeight;
    }

    window.openDrawer = (job_id, count) => {
      document.getElementById('pdrawer').classList.add('open');
      const card = document.createElement('div');
      card.className = 'pitem'; card.id = `j-${job_id}`;
      card.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px"><div class="pn">📦 ${count} file${count !== 1 ? 's' : ''}</div><button class="vbtn" style="width:auto;padding:3px 8px;font-size:10px;border-radius:6px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1)" onclick="toggleLogs('${job_id}')">Logs</button></div><div class="pb-wrap"><div class="pb" id="pb-${job_id}"></div></div><div class="pm"><span id="pc-${job_id}">Queued…</span><span id="pp-${job_id}">0%</span></div><div id="logs-${job_id}" class="m-logs" style="display:none"></div><div class="perr" id="pe-${job_id}" style="display:none"></div><button class="pcancel" onclick="cancelJob('${job_id}')">Cancel</button>`;
      document.getElementById('plist').prepend(card);
      const es = new EventSource(`/api/download/${job_id}/progress`);
      es.onmessage = e => {
        const d = JSON.parse(e.data);
        const pct = d.total ? Math.round(d.done / d.total * 100) : (d.pct || 0);
        document.getElementById(`pb-${job_id}`).style.width = pct + '%';
        document.getElementById(`pp-${job_id}`).textContent = pct + '%';
        const label = d.status === 'done' ? `✅ ${d.done} saved${d.skipped ? ' (' + d.skipped + ' skipped)' : ''}` : d.status === 'cancelled' ? '🚫 Cancelled' : d.flood_wait ? `⏳ Rate limited ${d.flood_wait}s` : d.current || `${d.done || 0}/${d.total || 0}`;
        document.getElementById(`pc-${job_id}`).textContent = label;
        if (d.errors?.length) { const pe = document.getElementById(`pe-${job_id}`); pe.style.display = 'block'; pe.textContent = d.errors.slice(-1)[0]; }
        if (d.status === 'done') { es.close(); toast(`Download complete — ${d.done} files`, 'ok'); document.querySelector(`#j-${job_id} .pcancel`)?.remove(); }
        if (d.status === 'cancelled' || d.status === 'error') es.close();
        if (d.logs) updateMirrorLogs(job_id, d.logs);
      };
      es.onerror = () => es.close();
    }

    window.cancelJob = async job_id => {
      try {
        const st = document.getElementById(`st-${job_id}`) || document.getElementById(`pc-${job_id}`); if (st) st.textContent = 'Cancelling…';
        const r = await api(`/api/download/${job_id}/cancel`, { method: 'POST' });
        if (r.cancelled) { toast('Cancellation requested', 'ok'); return; }
        const chId = parseInt(job_id);
        if (!isNaN(chId)) { await api('/api/mirror/sync/stop', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source_id: chId, target_id: 0 }) }); toast('Live Sync stopped', 'ok'); loadSyncRules(); }
      } catch (e) { console.error('Cancel failed', e); }
    };

    window.cancelAllJobs = async () => {
      const { cancelled } = await api('/api/jobs/cancel-all', { method: 'POST' });
      document.querySelectorAll('.pitem, .ext-item').forEach(el => { if (!el.textContent.includes('✅') && !el.textContent.includes('🚫')) { const st = el.querySelector('.ext-status') || el.querySelector('.pm span'); if (st) st.textContent = 'Cancelling…'; } });
      toast(`Requested cancellation for ${cancelled} jobs`, cancelled > 0 ? 'warn' : '');
    };
    window.resetActivity = async () => {
      if (!confirm('🛑 WARNING: This will permanently stop ALL active sync rules and CLEAR all activity history. Continue?')) return;
      try { await api('/api/jobs/reset', { method: 'POST' }); toast('All activity and logs cleared', 'ok'); document.getElementById('plist').innerHTML = ''; document.getElementById('mir-history').innerHTML = ''; document.getElementById('mir-syncs').innerHTML = '<div style="font-size:11px;color:var(--muted)">No active syncs</div>'; addMirrorCard('sync_activity', 'Live', 'Sync', 'Running'); } catch (e) { toast(e.message, 'err'); }
    };
    window.closeDr = () => document.getElementById('pdrawer').classList.remove('open');

    window.doYtdlp = async () => {
      const url = document.getElementById('ext-url').value.trim();
      const fmt = document.getElementById('ext-fmt').value;
      if (!url) return toast('Enter a URL', 'err');
      try {
        const { job_id } = await api('/api/ytdlp', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ url, fmt }) });
        const hist = document.getElementById('ext-history');
        const card = document.createElement('div'); card.className = 'ext-item'; card.id = `ext-${job_id}`;
        card.innerHTML = `<div class="ext-url">${esc(url)}</div><div id="ext-st-${job_id}" style="color:var(--muted);font-size:11px">Queued…</div><div class="ext-prog"><div class="ext-pbar" id="ext-pb-${job_id}"></div></div><button class="pcancel" style="margin-top:6px" onclick="cancelJob('${job_id}')">Cancel</button>`;
        hist.prepend(card);
        const es = new EventSource(`/api/download/${job_id}/progress`);
        es.onmessage = e => {
          const d = JSON.parse(e.data); document.getElementById(`ext-pb-${job_id}`).style.width = (d.pct || 0) + '%';
          document.getElementById(`ext-st-${job_id}`).textContent = d.status === 'done' ? '✅ Done' : d.status === 'cancelled' ? '🚫 Cancelled' : (d.current || 'Running…');
          if (d.status === 'done') { es.close(); toast('Download complete', 'ok'); document.querySelector(`#ext-${job_id} .pcancel`)?.remove(); }
          if (d.status === 'cancelled' || d.status === 'error') es.close();
        };
        es.onerror = () => es.close();
      } catch (e) { toast(e.message, 'err'); }
    };

    window.doMirror = async (btn) => {
      const srcRaw = document.getElementById('mir-src').value.trim();
      const dst = document.getElementById('mir-dst').value.trim();
      const lim = +document.getElementById('mir-lim').value || null;
      if (!srcRaw || !dst) return toast('Enter both source and target', 'err');
      const sources = srcRaw.split(',').map(s => s.trim()).filter(Boolean);
      const isSync = document.getElementById('mir-sync').checked;
      if (btn) { btn.disabled = true; btn.textContent = 'Starting...'; }
      try {
        for (const src of sources) {
          try {
            const { job_id } = await api('/api/mirror/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source_id: src, target_id: dst, limit: lim }) });
            addMirrorCard(job_id, src, dst);
            if (isSync) { await api('/api/mirror/sync/start', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source_id: src, target_id: dst }) }); toast(`Live Sync enabled for ${src}`, 'ok'); loadSyncRules(); }
            const es = new EventSource(`/api/download/${job_id}/progress`);
            document.getElementById('pdrawer').classList.add('open');
            es.onmessage = e => {
              const d = JSON.parse(e.data); updateMirrorCard(job_id, d);
              const box = document.getElementById(`logs-${job_id}`);
              if (box && box.style.display === 'none') toggleLogs(job_id);
            };
            es.onerror = () => es.close();
          } catch (e) { toast(`${src}: ${e.message}`, 'err'); }
        }
      } finally { if (btn) { btn.disabled = false; btn.textContent = '👯 Start Mirroring'; } }
    };

    function addMirrorCard(id, src, dst, status='Queued…', current='', logs=[]) {
      const hist = document.getElementById('mir-history'); if (document.getElementById(`job-${id}`)) return;
      const card = document.createElement('div'); card.className = 'ext-item'; card.id = `job-${id}`;
      const isSync = id === 'sync_activity';
      card.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;gap:10px"><div class="ext-url" style="min-width:0;flex:1;font-size:13px" title="${esc(src)} → ${esc(dst)}">${isSync ? '✨ Live Sync Activity' : '👯 ' + esc(src) + ' ➜ ' + esc(dst)}</div><button class="vbtn" style="width:auto;flex-shrink:0;padding:4px 12px;font-size:11px;border-radius:20px;background:var(--accent);border:1px solid var(--accent);color:#fff" onclick="toggleLogs('${id}')">Hide Logs</button></div><div id="st-${id}" class="ext-status" style="color:var(--muted)">${isSync ? 'Monitoring background rules...' : status}</div><div class="ext-prog" ${isSync ? 'style="display:none"' : ''}><div class="ext-pbar" id="pb-${id}"></div></div><div id="logs-${id}" class="m-logs" style="display:block"></div>${isSync ? '' : `<button class="pcancel" style="width:100%;margin-top:10px;border-radius:8px;padding:6px" onclick="cancelJob('${id}')">Cancel Job</button>`}`;
      hist.prepend(card); if (logs.length) updateMirrorLogs(id, logs);
    }

    function updateMirrorCard(id, d) {
      const pb = document.getElementById(`pb-${id}`); if (pb) pb.style.width = (d.pct || 0) + '%';
      const st = document.getElementById(`st-${id}`); if (!st) return;
      let speedText = '';
      if (d.status === 'running' && d.done !== undefined) {
        const now = Date.now(); const stats = jobStats.get(id) || { lastDone: d.done, lastTime: now, start: now };
        const elapsed = (now - stats.lastTime) / 1000;
        if (elapsed > 1.5) {
          const delta = d.done - stats.lastDone; const speed = delta / elapsed;
          if (speed > 0) { speedText = ` · ${speed.toFixed(1)} items/s`; if (d.total) { const remaining = d.total - d.done; const eta = Math.round(remaining / speed); if (eta > 0) speedText += ` · ETA ${eta}s`; } }
          jobStats.set(id, { lastDone: d.done, lastTime: now, start: stats.start });
        }
      }
      if (d.status === 'done') { st.textContent = '✅ Sync Complete'; st.style.color = 'var(--ok)'; document.querySelector(`#job-${id} .pcancel`)?.remove(); }
      else if (d.status === 'error') { st.textContent = '❌ Error: ' + (d.current || 'Unknown'); st.style.color = 'var(--err)'; }
      else if (d.status === 'cancelled') { st.textContent = '🚫 Cancelled'; }
      else { 
        let statusHtml = `Cloned <b>${d.done || 0}</b> / ${d.total || '?'}<span style="opacity:0.6">${speedText}</span>`;
        if (d.status === 'running' && !d.total) statusHtml = `<span style="color:var(--accent)">🔍 Scanning messages...</span>`;
        st.innerHTML = statusHtml + (d.current ? `<div style="font-size:10px;margin-top:2px;opacity:0.7">${esc(d.current)}</div>` : ''); 
      }
      if (d.logs) updateMirrorLogs(id, d.logs);
    }

    window.toggleLogs = (id) => {
      const box = document.getElementById(`logs-${id}`); if (!box) return;
      const btn = document.querySelector(`#job-${id} .vbtn`);
      const isVisible = box.style.display !== 'none'; box.style.display = isVisible ? 'none' : 'block';
      if (btn && btn.textContent.includes('Logs')) { btn.textContent = isVisible ? 'Show Logs' : 'Hide Logs'; btn.style.background = isVisible ? 'var(--adim)' : 'var(--accent)'; btn.style.color = isVisible ? 'var(--accent)' : '#fff'; }
    }

    async function loadMirrors() {
      try {
        const mirrors = await api('/api/mirrors');
        // Sort mirrors: running/queued first, then newest first
        mirrors.sort((a,b) => {
          const statusOrder = { 'running': 0, 'queued': 1, 'done': 2, 'error': 3, 'cancelled': 4 };
          const sa = statusOrder[a.status] ?? 99;
          const sb = statusOrder[b.status] ?? 99;
          if (sa !== sb) return sa - sb;
          return (b.id && a.id) ? b.id.localeCompare(a.id) : 0;
        });

        // Prepend in reverse order so that the first items in the sorted list end up at the top
        [...mirrors].reverse().forEach(m => {
          addMirrorCard(m.id, m.source_id, m.target_id, m.status, '', m.logs || []);
          updateMirrorCard(m.id, m);
          
          if (m.status !== 'done' && m.status !== 'error' && m.status !== 'cancelled') {
            const es = new EventSource(`/api/download/${m.id}/progress`); 
            es.onmessage = e => { 
                const d = JSON.parse(e.data); 
                updateMirrorCard(m.id, d); 
                if (d.logs && d.logs.length > 0) {
                    const box = document.getElementById(`logs-${m.id}`);
                    if (box && box.style.display === 'none') {
                        box.style.display = 'block';
                        const btn = document.querySelector(`#job-${m.id} .vbtn`);
                        if (btn) btn.textContent = 'Hide Logs';
                    }
                }
            }; 
            es.onerror = () => es.close(); 
          }
        });
      } catch (e) { console.error('Failed to load mirrors', e); }
    }

    async function loadSyncRules() {
      const box = document.getElementById('mir-syncs'); if (!box) return;
      try {
        const rules = await api('/api/mirror/sync/list');
        if (!rules.length) { box.innerHTML = '<div style="font-size:11px;color:var(--muted)">No active syncs</div>'; return; }
        box.innerHTML = rules.map(r => r.targets.map(t => `<div class="ext-item sync-rule-card"><div style="display:flex;justify-content:space-between;align-items:center;gap:10px"><div style="font-size:12px;display:flex;align-items:center;gap:8px;min-width:0;flex:1"><span class="sb-dot" style="flex-shrink:0"></span><span style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;min-width:0" title="${esc(r.source_id)} ➜ ${esc(t)}">${esc(r.source_id)} ➜ ${esc(t)}</span></div><button class="vbtn" style="width:auto;flex-shrink:0;padding:3px 10px;font-size:10px;background:var(--err);border-radius:6px" onclick="stopSync('${r.source_id}', '${t}')">Stop</button></div></div>`).join('')).join('');
      } catch (e) { console.error('Sync Rules load failed', e); }
    }

    window.stopSync = async (src, dst) => { try { await api('/api/mirror/sync/stop', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source_id: src, target_id: dst }) }); toast('Sync stopped', 'ok'); loadSyncRules(); } catch (e) { toast(e.message, 'err'); } };

    window.showMobilePane = (paneName, btnEl) => {
      document.querySelectorAll('.m-nav-item').forEach(b => b.classList.remove('active'));
      if (btnEl) btnEl.classList.add('active');
      else {
        const idx = ['channels','main','activity','tools'].indexOf(paneName);
        if (idx >= 0) { const btn = document.querySelectorAll('.m-nav-item')[idx]; if (btn) btn.classList.add('active'); }
      }
      const sidebar = document.querySelector('.sidebar'), main = document.querySelector('.main'), pd = document.querySelector('.pd');
      if (sidebar) sidebar.classList.remove('m-active'); if (main) main.classList.remove('m-active'); if (pd) pd.classList.remove('m-active');
      const stabs = document.querySelector('.stabs'); if (stabs) stabs.style.display = 'flex';
      if (paneName === 'channels') { if (sidebar) sidebar.classList.add('m-active'); const tab = document.querySelector('.stab:nth-child(1)'); if (tab) switchSTab('channels', tab); }
      else if (paneName === 'main') { if (main) main.classList.add('m-active'); updViewport(); }
      else if (paneName === 'activity') { if (pd) pd.classList.add('m-active'); }
      else if (paneName === 'tools') { if (sidebar) sidebar.classList.add('m-active'); const tab = document.querySelector('.stab:nth-child(2)'); if (tab) switchSTab('external', tab); }
    };
    if (window.innerWidth <= 768) { const main = document.querySelector('.main'); if (main) main.classList.add('m-active'); const btn = document.querySelectorAll('.m-nav-item')[1]; if (btn) showMobilePane('main', btn); }

    (function() {
      function setupResizer(id, varName, minW, maxW, isRight = false) {
        const g = document.getElementById(id); if (!g) return;
        let dragging = false, frame;
        g.addEventListener('mousedown', e => { e.preventDefault(); dragging = true; g.classList.add('active'); document.body.classList.add('is-dragging'); });
        window.addEventListener('mousemove', e => {
          if (!dragging) return;
          if (frame) cancelAnimationFrame(frame);
          frame = requestAnimationFrame(() => {
            let w; if (isRight) w = Math.min(Math.max(minW, window.innerWidth - e.clientX), maxW); else w = Math.min(Math.max(minW, e.clientX), maxW);
            document.documentElement.style.setProperty(varName, w + 'px');
          });
        });
        window.addEventListener('mouseup', () => { if (!dragging) return; dragging = false; g.classList.remove('active'); document.body.classList.remove('is-dragging'); if (frame) cancelAnimationFrame(frame); });
      }
      setupResizer('gutter', '--sidebar-w', 240, 800, false); setupResizer('p-gutter', '--drawer-w', 250, 800, true);
    })();

    document.addEventListener('keydown', e => {
      const isInput = e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA';
      if (isInput) { if (e.key === 'Enter' && !e.shiftKey) { if (document.getElementById('va')?.classList.contains('active')) doAuth(); else if (document.getElementById('vo')?.classList.contains('active')) doOtp(); } return; }
      if (e.key === 'Escape') { lbClose(); hideTruePreview(); }
      const lb = document.getElementById('lb'); const isLb = lb.classList.contains('active') || lb.classList.contains('open');
      if (isLb) { if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { e.preventDefault(); lbNav(-1); return; } if (e.key === 'ArrowRight' || e.key === 'ArrowDown') { e.preventDefault(); lbNav(1); return; } return; }
      if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(e.key)) { e.preventDefault(); peekByKeys(e.key.replace('Arrow', '').toLowerCase(), e); return; }
      if (e.key === ' ') { e.preventDefault(); const key = filteredKeys[peekIdx]; if (key) toggleSel(key, document.querySelector(`.mc[data-key="${key}"]`), e); }
      if (e.key === 'Enter' && !e.metaKey && !e.ctrlKey) { const key = filteredKeys[peekIdx]; if (key) openPreview(key); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'a') { e.preventDefault(); selAll(); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { if (selItems.size) doDl(); }
    });
    document.getElementById('msearch')?.addEventListener('input', debounce(e => searchMedia(e.target.value), 250));
    function showErr(id, m) { const el = document.getElementById(id); el.textContent = m; el.style.display = 'block'; }
    function hideErr(id) { document.getElementById(id).style.display = 'none'; }

    (async () => { try { const r = await api('/api/auth/status'); if (r.authenticated) { sv('vapp'); initApp(); } else sv('va'); } catch { sv('va'); } })();
  })();
}