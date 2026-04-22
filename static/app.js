// ════════════════════════════════════════════════════════════════════════════
// INDEX PAGE
// ════════════════════════════════════════════════════════════════════════════
if (document.body.classList.contains('idx-page')) {
  (function () {
    // ── State ──────────────────────────────────────────────────────────────
    let allChs = [];
    let selChs = new Set();
    let mediaRegistry = new Map(); // GLOBAL RAM CACHE: key="cid_mid" → item object
    let items = new Map();          // Currently active/visible items
    let selItems = new Set();
    let lastSelKey = null;   // For shift-click
    let lastSelChId = null;  // For sidebar shift-click
    let filter = 'all';
    let stream = null;
    let paused = false;
    let renderBuf = [];
    let cardIdx = 0;
    let viewMode = 'gallery';
    let loadNonce = 0;
    let currentSearch = '';
    let currentSort = '';
    let allMediaKeys = [];
    let filteredKeys = [];
    let viewportHeight = 0;
    let columns = 1;
    let vsRAF = null;
    let peekIdx = -1;
    const gap = 24;

    // ── IndexedDB Caching ────────────────────────────────────────────────
    const DB_NAME = 'tgrab_cache';
    const STORE_NAME = 'media_items';
    let idb = null;

    async function initDB() {
      return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, 2);
        req.onupgradeneeded = e => {
          const db = e.target.result;
          if (!db.objectStoreNames.contains(STORE_NAME)) {
            const store = db.createObjectStore(STORE_NAME, { keyPath: 'key' });
            store.createIndex('channel_id', 'channel_id', { unique: false });
          }
          if (!db.objectStoreNames.contains('channels')) {
            db.createObjectStore('channels', { keyPath: 'id' });
          }
          if (!db.objectStoreNames.contains('thumbs')) {
            db.createObjectStore('thumbs', { keyPath: 'key' });
          }
        };
        req.onsuccess = e => { idb = e.target.result; resolve(idb); };
        req.onerror = e => reject(e.target.error);
      });
    }

    async function storeThumb(key, blob) {
      if (!idb) return;
      const tx = idb.transaction('thumbs', 'readwrite');
      tx.objectStore('thumbs').put({ key, blob });
    }

    async function getThumb(key) {
      if (!idb) return null;
      return new Promise(resolve => {
        const req = idb.transaction('thumbs', 'readonly').objectStore('thumbs').get(key);
        req.onsuccess = () => resolve(req.result?.blob || null);
        req.onerror = () => resolve(null);
      });
    }

    async function cacheChannels(chs) {
      if (!idb) return;
      const tx = idb.transaction('channels', 'readwrite');
      const store = tx.objectStore('channels');
      chs.forEach(c => store.put(c));
    }

    async function getCachedChannels() {
      if (!idb) return [];
      return new Promise(resolve => {
        const tx = idb.transaction('channels', 'readonly');
        const store = tx.objectStore('channels');
        const req = store.getAll();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => resolve([]);
      });
    }

    async function cacheBatch(itemsArray) {
      if (!idb) return;
      const tx = idb.transaction(STORE_NAME, 'readwrite');
      const store = tx.objectStore(STORE_NAME);
      itemsArray.forEach(it => {
        const key = `${it.channel_id}_${it.msg_id}`;
        store.put({ key, ...it });
      });
    }

    async function getCachedItems(channelIds) {
      if (!idb) return [];
      
      // Try RAM first if we have it
      if (mediaRegistry.size > 0) {
        const results = [];
        mediaRegistry.forEach(it => {
          if (channelIds.includes(it.channel_id)) results.push(it);
        });
        if (results.length > 0) return results;
      }

      return new Promise((resolve) => {
        const tx = idb.transaction(STORE_NAME, 'readonly');
        const store = tx.objectStore(STORE_NAME);
        const index = store.index('channel_id');
        let results = [];
        let count = 0;
        
        channelIds.forEach(cid => {
          const req = index.getAll(IDBKeyRange.only(cid));
          req.onsuccess = () => {
            results = results.concat(req.result);
            count++;
            if (count === channelIds.length) resolve(results);
          };
          req.onerror = () => { count++; if (count === channelIds.length) resolve(results); };
        });
      });
    }


    const COLORS = ['#4f8eff', '#ff6b8a', '#35d47b', '#ffb84f', '#a78bff', '#ff9b3d', '#0ecfcf'];

    // ── High-Performance Observers ──────────────────────────────────────────
    const thumbObs = new IntersectionObserver(entries => {
      entries.forEach(e => {
        if (e.isIntersecting) {
          loadThumb(e.target, items.get(e.target.dataset.key));
          thumbObs.unobserve(e.target);
        }
      });
    }, { rootMargin: '100px' }); // Targeted pre-fetch to reduce concurrent I/O

    const prefetchPreview = (key) => {
      const item = items.get(key);
      if (!item || item._prefetched) return;
      const img = new Image();
      img.src = `/api/preview/${item.channel_id}/${item.msg_id}`;
      item._prefetched = true;
    };

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
      await initDB();
      
      // Instant load channels from cache
      const cachedChs = await getCachedChannels();
      if (cachedChs.length) {
        allChs = cachedChs;
        renderChannelList();
        renderFolderTabs();
      }

      
      // Add sync activity card - if it doesn't exist, it will be prepended (top)
      if (!document.getElementById('job-sync_activity')) {
        addMirrorCard('sync_activity', 'Live', 'Sync', 'Running');
      }
      
      const esSync = new EventSource('/api/download/sync_activity/progress');
      esSync.onmessage = e => {
        const d = JSON.parse(e.data);
        if (d.logs) updateMirrorLogs('sync_activity', d.logs);
      };

      // System logs stream
      const logContainer = document.getElementById('system-logs');
      if (logContainer) {
        const esLogs = new EventSource('/api/logs/stream');
        esLogs.onmessage = e => {
          if (e.data.startsWith(':')) return;
          try {
            const d = JSON.parse(e.data);
            if (d.log) {
              const div = document.createElement('div');
              const parts = d.log.split(' ');
              const level = (parts[2] || 'INFO').toUpperCase();
              div.className = 'log-line ' + (['INFO','WARN','ERROR'].includes(level) ? level : 'INFO');
              div.textContent = d.log;
              logContainer.appendChild(div);
              if (logContainer.childNodes.length > 200) logContainer.removeChild(logContainer.firstChild);
              logContainer.scrollTop = logContainer.scrollHeight;
            }
          } catch(err) { console.debug('Log parse error', err); }
        };
        esLogs.onerror = () => { console.warn('Log stream lost'); };
      }

      if (allChs.length === 0) {
        document.getElementById('chlist').innerHTML =
          '<div style="padding:8px;display:grid;gap:6px">' +
          '<div class="sk" style="height:56px"></div>'.repeat(5) + '</div>';
      }
      renderFolderTabs();
      loadChannelsSSE();
      updViewport();
      startLiveUpdates();
    }

    let chES = null;
    function loadChannelsSSE() {
      if (chES) chES.close();
      const es = new EventSource('/api/channels');
      chES = es;
      let chRenderPending = false;
      es.onmessage = e => {
        const d = JSON.parse(e.data);
        if (d.error) {
            console.error('Channel Stream Error:', d.error);
            toast(d.error, 'err');
            return;
        }
        if (d.folder_list) {
          folders = d.folder_list;
          renderFolderTabs();
          return;
        }
        if (d.done) { 
          es.close(); 
          chES = null;
          renderChannelList(); 
          cacheChannels(allChs);
          return; 
        }
        // Deduplicate and update
        const idx = allChs.findIndex(c => c.id === d.id);
        if (idx !== -1) {
          allChs[idx] = { ...allChs[idx], ...d };
        } else {
          allChs.push(d);
        }
        
        if (!chRenderPending) {
          chRenderPending = true;
          requestAnimationFrame(() => { chRenderPending = false; renderChannelList(); });
        }
      };
      es.onerror = (e) => { 
        console.error('Channel SSE Error:', e);
        es.close(); 
        chES = null; 
      };
    }

    function renderFolderTabs() {
      const box = document.getElementById('ftabs');
      if (!box) return;

      const unreadCount = allChs.filter(c => (c.unread || 0) > 0).length;
      
      let html = `<button class="ftab ${activeFolder === '' ? 'active' : ''}" onclick="setFolder('')">All</button>`;
      
      if (unreadCount > 0) {
        html += `<button class="ftab ${activeFolder === 'unread' ? 'active' : ''}" onclick="setFolder('unread')">Unread (${unreadCount})</button>`;
      }

      folders.forEach(f => {
        const name = typeof f === 'string' ? f : f.name;
        const emoji = typeof f === 'string' ? '' : (f.emoji || '');
        html += `<button class="ftab ${activeFolder === name ? 'active' : ''}" onclick="setFolder('${name}')">${emoji} ${name}</button>`;
      });

      box.innerHTML = html;
    }

    // ── Previews & Hover Interactions ───────────────────────────────────
    if (!document.getElementById('true-preview')) {
        const tp = document.createElement('div');
        tp.id = 'true-preview';
        tp.className = 'true-preview-box';
        document.body.appendChild(tp);
    }
    const tp = document.getElementById('true-preview');
    let peekT;
    let previewAbort = null;

    window.showTruePreview = (key, e) => {
      clearTimeout(peekT);
      if (previewAbort) previewAbort.abort();
      previewAbort = new AbortController();
      const signal = previewAbort.signal;

      peekT = setTimeout(async () => {
        if (signal.aborted) return;
        const it = items.get(key) || mediaRegistry.get(key);
        if (!it) return;
        
        tp.innerHTML = '';
        const thumbBlob = await getThumb(key, signal);
        if (signal.aborted) return;
        const thumbUrl = thumbBlob ? URL.createObjectURL(thumbBlob) : null;
        
        if (it.type === 'video') {
          const v = document.createElement('video');
          if (thumbUrl) v.poster = thumbUrl;
          v.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
          v.autoplay = true; v.muted = true; v.loop = true;
          tp.appendChild(v);
        } else if (it.type === 'photo') {
          const img = new Image();
          if (thumbUrl) img.style.backgroundImage = `url(${thumbUrl})`;
          img.style.backgroundSize = 'cover';
          img.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
          img.decode().then(() => {
            if (signal.aborted) return;
            tp.appendChild(img);
            tp.style.display = 'block';
            moveTruePreview(e);
          }).catch(() => {
            // Fallback for browsers without decode or error cases
            if (signal.aborted) return;
            tp.appendChild(img);
            tp.style.display = 'block';
            moveTruePreview(e);
          });
          return; // Handled by decode promise
        } else return;
      }, 50); // Snappy but debounced
    };

    window.hideTruePreview = () => { clearTimeout(peekT); tp.style.display = 'none'; tp.innerHTML = ''; };

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
      
      const mgrid = document.getElementById('mgrid');
      let cols = Math.floor(mgrid.clientWidth / (parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220));
      if (cols < 1) cols = 1;

      if (dir === 'left') peekIdx--;
      else if (dir === 'right') peekIdx++;
      else if (dir === 'up') peekIdx -= cols;
      else if (dir === 'down') peekIdx += cols;

      if (peekIdx < 0) peekIdx = 0;
      if (peekIdx >= filteredKeys.length) peekIdx = filteredKeys.length - 1;

      const key = filteredKeys[peekIdx];
      const item = items.get(key);
      if (!item) return;

      // Update UI focus immediately for responsiveness
      document.querySelectorAll('.mc.kb-focus').forEach(el => el.classList.remove('kb-focus'));
      const cardEl = document.querySelector(`.mc[data-key="${key}"]`);
      if (cardEl) {
        cardEl.classList.add('kb-focus');
        const rect = cardEl.getBoundingClientRect();
        
        // Clear preview if navigating by keys
        clearTimeout(peekT);
        hideTruePreview();

        // Auto-scroll logic
        const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 150;
        const row = Math.floor(peekIdx / cols);
        const targetTop = row * rowH;
        if (targetTop < mgrid.scrollTop) mgrid.scrollTop = targetTop;
        else if (targetTop + rowH > mgrid.scrollTop + mgrid.clientHeight) mgrid.scrollTop = targetTop + rowH - mgrid.clientHeight;
      }
    };

    // Command Palette Logic
    let cpIdx = -1;
    let cpFiltered = [];

    window.toggleCP = (e) => {
      if (e) e.preventDefault();
      const overlay = document.getElementById('cp-overlay');
      const input = document.getElementById('cp-input');
      overlay.classList.add('active'); input.value = ''; input.focus();
      searchCP('');
    };

    window.closeCP = () => document.getElementById('cp-overlay').classList.remove('active');

    window.searchCP = (q) => {
      q = q.toLowerCase();
      const commands = [
        { text: 'View Photos', icon: '📷', type: 'Category', action: () => setFilter('photo') },
        { text: 'View Videos', icon: '🎬', type: 'Category', action: () => setFilter('video') },
        { text: 'View Documents', icon: '📄', type: 'Category', action: () => setFilter('document') },
        { text: 'View All Media', icon: '🖼️', type: 'Category', action: () => setFilter('all') },
        { text: 'Wipe Cache', icon: '🧹', type: 'System', action: () => { if(confirm('Wipe cache?')) wipeCache(); } }
      ];
      const chs = allChs.map(c => ({ text: c.title, icon: '📡', type: 'Channel', action: () => { selectChannel(c.id); } }));
      cpFiltered = [...commands, ...chs].filter(it => it.text.toLowerCase().includes(q) || it.type.toLowerCase().includes(q)).slice(0, 15);
      cpIdx = cpFiltered.length > 0 ? 0 : -1;
      renderCP();
    };

    function renderCP() {
      const box = document.getElementById('cp-results');
      if (!box) return;
      box.innerHTML = cpFiltered.map((it, i) => `
        <div class="cp-item ${i === cpIdx ? 'active' : ''}" onclick="execCP(${i})">
          <span class="cp-item-icon">${it.icon}</span>
          <span class="cp-item-text">${esc(it.text)}</span>
          <span class="cp-item-type">${it.type}</span>
        </div>`).join('');
      const active = box.querySelector('.cp-item.active');
      if (active) active.scrollIntoView({ block: 'nearest' });
    }

    window.execCP = (i) => { 
      const it = cpFiltered[i]; 
      if (it) { 
        console.log('Executing CP Action:', it.text);
        it.action(); 
        closeCP(); 
      } 
    };

    const cpInput = document.getElementById('cp-input');
    if (cpInput) {
        cpInput.addEventListener('keydown', e => {
            if (!cpFiltered.length) return;
            if (e.key === 'ArrowDown') { e.preventDefault(); cpIdx = (cpIdx + 1) % cpFiltered.length; renderCP(); }
            else if (e.key === 'ArrowUp') { e.preventDefault(); cpIdx = (cpIdx - 1 + cpFiltered.length) % cpFiltered.length; renderCP(); }
            else if (e.key === 'Enter') { e.preventDefault(); execCP(cpIdx); }
            else if (e.key === 'Escape') { e.preventDefault(); closeCP(); }
        });
        cpInput.addEventListener('input', e => searchCP(e.target.value));
    }

    async function loadMedia(chs = null) {
      if (!chs) chs = Array.from(selChs);
      if (chs.length === 0) return;
      if (stream) { stream.close(); stream = null; }
      
      const nonce = ++loadNonce;
      items.clear(); allMediaKeys = []; filteredKeys = [];
      const grid = document.getElementById('mgrid');
      if (grid) grid.style.display = 'block';
      document.getElementById('empty').style.display = 'none';
      document.getElementById('sbar').classList.add('active');
      document.getElementById('pause-btn').style.display = 'block';
      const sbarTxt = document.getElementById('sbar-txt');
      const chIds = [...selChs];
      const ids = chIds.join(',');

      // 1. Try local cache (RAM + IndexedDB) first for instant results
      const localItems = await getCachedItems(chIds);
      if (localItems.length > 0) {
        localItems.forEach(it => {
          if (!it.hay) it.hay = `${it.filename || ''} ${it.caption || ''} ${it.date || ''} ${it.type || ''}`.toLowerCase();
          if (!mediaRegistry.has(it.key)) mediaRegistry.set(it.key, it);
          if (!items.has(it.key)) {
            items.set(it.key, it);
            allMediaKeys.push(it.key);
          }
        });
      }
      
      // 2. Hydrate from server if local cache is sparse
      if (items.size < 100) {
          sbarTxt.textContent = 'Hydrating library...';
          try {
              // Fetch a reasonable chunk for initial view
              const sort = currentSort || 'newest';
              const cacheData = await api(`/api/gallery-data?channels=${ids}&limit=2000&sort=${sort}`, { timeout: 10000 });
              if (nonce !== loadNonce) return;
              cacheData.forEach(it => {
                  const key = `${it.channel_id}_${it.msg_id}`;
                  it.key = key;
                  it.hay = `${it.filename || ''} ${it.caption || ''} ${it.date || ''} ${it.type || ''}`.toLowerCase();
                  if (!mediaRegistry.has(key)) mediaRegistry.set(key, it);
                  if (!items.has(key)) { items.set(key, it); allMediaKeys.push(key); }
              });
              // Persist to IDB in background
              cacheBatch(cacheData);
          } catch(e) { 
              console.warn('Cache hydration failed or timed out', e);
              if (nonce !== loadNonce) return;
              sbarTxt.textContent = 'Hydration deferred, scanning...';
          }
      }

      if (allMediaKeys.length > 0) {
        // Only sort if array is not too massive to avoid UI freeze
        if (allMediaKeys.length < 20000) {
            applyFilters(); // This handles sorting and filtering
        }
        sbarTxt.textContent = `${items.size} items (ready)`;
        updPillCounts(); applyFilters();
      } else sbarTxt.textContent = 'Scanning channel...';
      
      // 3. Revalidation Stream (only fetches what's missing)
      stream = new EventSource(`/api/media?channels=${ids}&type=all`);

      stream.onerror = (e) => {
          console.error('Media stream failed:', e);
          toast('Media connection lost. Retrying...', 'err');
          stream.close();
          stream = null;
          // Auto-retry once after 2 seconds
          setTimeout(() => { if (nonce === loadNonce) loadMedia(chs); }, 2000);
      };
      
      stream.onmessage = e => {
        if (nonce !== loadNonce) { stream.close(); return; }
        const d = JSON.parse(e.data);
        if (d.status) {
            document.getElementById('sbar-txt').textContent = d.status;
            return;
        }
        if (d.error) {
            console.error('Media Stream Error:', d.error);
            toast(d.error, 'err');
            return;
        }
        if (d.batch) {
          const fresh = [];
          d.batch.forEach(item => {
            const key = `${item.channel_id}_${item.msg_id}`;
            item.key = key;
            item.hay = `${item.filename || ''} ${item.caption || ''} ${item.date || ''} ${item.type || ''}`.toLowerCase();
            if (!mediaRegistry.has(key)) { mediaRegistry.set(key, item); items.set(key, item); allMediaKeys.push(key); fresh.push(item); }
          });
            if (fresh.length > 0) {
              cacheBatch(fresh);
              if (allMediaKeys.length < 30000) {
                applyFilters();
              }
              sbarTxt.textContent = `${items.size} items live`;
              updPillCounts(); 
              // Throttle UI updates
              if (!vsRAF) vsRAF = requestAnimationFrame(() => { vsRAF = null; applyFilters(); });
            }
          return;
        }
        if (d.done) {
          stream.close(); stream = null; sbarTxt.textContent = `${items.size} items`;
          document.getElementById('pause-btn').style.display = 'none'; document.getElementById('stop-btn').style.display = 'none';
          updPillCounts(); applyFilters(); return;
        }
        const key = `${d.channel_id}_${d.msg_id}`;
        if (!mediaRegistry.has(key)) {
          d.key = key; 
          mediaRegistry.set(key, d); 
          items.set(key, d); 
          allMediaKeys.push(key); 
          cacheBatch([d]);
          if (allMediaKeys.length % 100 === 0) { 
             if (allMediaKeys.length < 30000) applyFilters();
             if (!vsRAF) vsRAF = requestAnimationFrame(() => { vsRAF = null; applyFilters(); });
          }
        }
      };
    }
    
    window.togglePause = () => { paused = !paused; document.getElementById('pause-btn').textContent = paused ? '▶ Resume' : '⏸ Pause'; if (!paused) renderVirtual(); };
    window.stopStream = () => { if (stream) { stream.close(); stream = null; document.getElementById('sbar-txt').textContent = 'Stopped'; document.getElementById('pause-btn').style.display = 'none'; document.getElementById('stop-btn').style.display = 'none'; toast('Media loading stopped', ''); } };

    async function loadThumb(cardEl, item) {
      if (!item || item.type === 'document') return;
      const key = `${item.channel_id}_${item.msg_id}`;
      const cachedBlob = await getThumb(key);
      if (cachedBlob) { _applyThumb(cardEl, item, URL.createObjectURL(cachedBlob), false); return; }

      const url = `/api/thumb/${item.channel_id}/${item.msg_id}`;
      fetch(url, { priority: 'high' }).then(r => r.blob()).then(blob => {
        if (blob.size < 100) throw new Error('Short blob');
        storeThumb(key, blob);
        _applyThumb(cardEl, item, URL.createObjectURL(blob), true);
      }).catch(() => {
        const area = cardEl.querySelector('.m-thumb-area');
        if (area) area.style.filter = 'none';
      });
    }

    function makeCard(key, item) {
      const sel = selItems.has(key);
      const isDoc = item.type === 'document';
      const el = document.createElement('div');
      el.className = 'mc' + (sel ? ' sel' : '') + (isDoc ? ' mc-doc' : '');
      el.dataset.key = key;
      el.onclick = (e) => {
        if (e.target.closest('.mchk')) { e.stopPropagation(); toggleSel(key, el, e); }
        else if (e.target.closest('.mpreview')) { e.stopPropagation(); openPreview(key); }
        else {
          if (e.ctrlKey || e.metaKey || selItems.size > 0 || window.innerWidth <= 768) toggleSel(key, el, e);
          else openPreview(key);
        }
      };
      
      let hoverPrefT;
      el.onmouseenter = (e) => { 
        showTruePreview(key, e); 
        hoverPrefT = setTimeout(() => prefetchPreview(key), 80); 
      };
      el.onmouseleave = () => { hideTruePreview(); clearTimeout(hoverPrefT); };
      el.onmousemove = (e) => moveTruePreview(e);
      el.ondblclick = ev => { ev.stopPropagation(); openPreview(key); };

      const iconHtml = ICONS[item.type] || '❓';
      const cap = item.caption || '';
      
      // Only apply LQIP blur to photos; show videos and docs clearly from the start
      const lqip = item.st_b64 ? `url(data:image/jpeg;base64,${item.st_b64})` : 'none';
      const blur = (item.type === 'photo' && item.st_b64) ? 'blur(12px)' : 'none';

      el.innerHTML = `
        <div class="m-thumb-area" style="background-image: ${lqip}; background-size: cover; filter: ${blur}; transition: filter 0.6s cubic-bezier(0.4, 0, 0.2, 1), transform 0.3s ease;">
          <div class="nth"><div class="ti">${iconHtml}</div></div>
          <div class="mchk">${chk()}</div>
          <div class="tbadge ${item.type.charAt(0)}">${item.type.toUpperCase()}</div>
          <div class="mpreview">${ICONS.search}</div>
          ${cap ? `<div class="mcap" title="${esc(cap)}">${esc(cap.slice(0, 200))}</div>` : ''}
        </div>
        <div class="m-details">
          <div class="mfn" title="${esc(item.filename)}">${esc(item.filename)}</div>
          <div class="msz">${item.size_readable || hrSize(item.size)}</div>
        </div>`;
      el.style.position = 'absolute';
      return el;
    }

    // Observers moved to global scope

    function _applyThumb(cardEl, item, url, isNew) {
      const area = cardEl.querySelector('.m-thumb-area');
      if (area) {
        // High-res swap
        const temp = new Image();
        temp.onload = () => {
          area.style.backgroundImage = `url(${url})`;
          area.style.filter = 'none'; // Smoothly reveal
        };
        temp.src = url;
      }
      const nth = cardEl.querySelector('.nth');
      if (item.type === 'video') {
        const vid = document.createElement('video');
        vid.poster = url; vid.muted = true; vid.loop = true; vid.setAttribute('playsinline', ''); vid.className = 'card-vid';
        if (nth) nth.replaceWith(vid);
        cardEl.style.setProperty('--bg-img', `url(${url})`);
        if (area) area.style.filter = 'none'; // Ensure video cards are un-blurred immediately
        cardEl.onmouseenter = (e) => {
          showTruePreview(item.key, e);
          if (!vid.src) { vid.src = `/api/preview/${item.channel_id}/${item.msg_id}`; vid.load(); }
          vid.play().catch(() => {});
        };
        cardEl.onmouseleave = () => { hideTruePreview(); vid.pause(); };
      } else {
        const img = new Image();
        img.src = url;
        img.decode().then(() => {
          cardEl.style.setProperty('--bg-img', `url(${url})`); 
          if (nth) nth.replaceWith(img); 
          if (area) area.style.filter = 'none'; // Ensure un-blur
        }).catch(() => {
          // Fallback if decode fails or unsupported
          cardEl.style.setProperty('--bg-img', `url(${url})`); 
          if (nth) nth.replaceWith(img); 
          if (area) area.style.filter = 'none'; 
        });
      }
    }

    function toggleSel(key, el, e) {
      if (e && e.shiftKey && lastSelKey && filteredKeys.includes(lastSelKey)) {
        const idxA = filteredKeys.indexOf(lastSelKey), idxB = filteredKeys.indexOf(key);
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
      const n = selItems.size; const el = document.getElementById('selc'); if (el) el.textContent = n;
      const bb = document.querySelector('.bb'); if (bb) { if (n > 0) bb.classList.add('active'); else bb.classList.remove('active'); }
      let totalSize = 0; selItems.forEach(k => { const it = items.get(k); if (it) totalSize += it.size || 0; });
      const bbc = document.querySelector('.bbc'); if (bbc) bbc.innerHTML = `<b>${n}</b> item${n !== 1 ? 's' : ''} · ${hrSize(totalSize)} selected`;
    }

    function startLiveUpdates() {
      if (window.updateES) window.updateES.close();
      const es = new EventSource('/api/updates'); window.updateES = es;
      es.onmessage = e => {
        const ev = JSON.parse(e.data);
        if (ev.type === 'new_message') {
          const ch = allChs.find(c => c.id === ev.channel_id);
          if (ch) { ch.unread = (ch.unread || 0) + 1; renderChannelList(); }
          else {
            // New channel join detected automatically via message
            loadChannelsSSE();
          }
          if (ev.item && selChs.has(ev.channel_id)) {
            const key = `${ev.item.channel_id}_${ev.item.msg_id}`;
            if (!mediaRegistry.has(key)) {
              ev.item.key = key; ev.item.hay = (ev.item.filename + ' ' + ev.item.caption).toLowerCase();
              mediaRegistry.set(key, ev.item); items.set(key, ev.item); allMediaKeys.push(key);
              requestAnimationFrame(() => applyFilters());
            }
          }
        }
      };
      es.onerror = () => { es.close(); setTimeout(startLiveUpdates, 5000); };
    }

    // ── Global Initializer ──────────────────────────────────────────
    (async () => {
      try {
        const r = await api('/api/auth/status');
        if (r.authenticated) { sv('vapp'); initApp(); startLiveUpdates(); }
        else sv('va');
        if (window.lucide) lucide.createIcons();
      } catch { sv('va'); }
    })();

    document.addEventListener('keydown', e => {
      const isInput = e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA';
      if (isInput) return;
      if (e.key === 'Escape') { lbClose(); hideTruePreview(); closeCP(); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') { e.preventDefault(); toggleCP(); }
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight' || e.key === 'ArrowUp' || e.key === 'ArrowDown') { e.preventDefault(); peekByKeys(e.key.replace('Arrow', '').toLowerCase(), e); }
    });

    window.setFolder = name => {
      activeFolder = name;
      renderFolderTabs();
      renderChannelList();
    };

    // ── Channel list ────────────────────────────────────────────────────────
    const ICONS = {
      photo: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>`,
      video: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>`,
      document: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`,
      search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`
    };

    function hrSize(b) {
      if (b === 0) return '0 B';
      const k = 1024; const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(b) / Math.log(k));
      return parseFloat((b / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    }

    function chColor(t) {
      return COLORS[Math.abs([...t].reduce((a, c) => a + c.charCodeAt(0), 0)) % COLORS.length];
    }

    let currentVisibleChs = [];

    function renderChannelList() {
      const list = document.getElementById('chlist');
      if (!list) return;

      renderFolderTabs();
      list.innerHTML = '';
      const searchQ = (document.querySelector('#sp-channels .sw input')?.value || '').toLowerCase();
      let filtered = allChs;
      
      const isActiveUnread = activeFolder === 'unread_virtual' || activeFolder.toLowerCase() === 'unread';

      if (isActiveUnread) {
        // Show ONLY unread channels
        filtered = filtered.filter(c => (c.unread || 0) > 0);
      } else if (activeFolder === '') {
        // All view: Show ALL channels
        // (Removing the old "read only" filter as it confused users)
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
      
      const memCount = ch.members > 999 ? (ch.members / 1000).toFixed(1) + 'k' : (ch.members || 0);
      const memHtml = ch.members 
        ? `<span class="mem-icon">👤</span><span class="mem-count">${memCount}</span>`
        : `<span class="mem-type">${ch.type}</span>`;
      
      const folderBadge = ch.folders?.length
        ? `<span class="ch-folder">${esc(ch.folders[0])}</span>` : '';
      
      const unreadIndicator = ch.unread > 0 
        ? `<div class="ch-unread ${ch.pulsing ? 'pulse' : ''}">${ch.unread}</div>` 
        : '';

      div.innerHTML = `
        <div class="av" style="background:${color}15;color:${color}">
          ${ch.title.charAt(0).toUpperCase()}
          <div class="mem-badge">${memCount}</div>
        </div>
        <div class="ci">
          <div class="cn">${esc(ch.title)}</div>
          <div class="cm">${memHtml} ${folderBadge}</div>
        </div>
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
        if (selChs.has(id)) { selChs.delete(id); lastSelChId = null; }
        else { selChs.add(id); lastSelChId = id; }
      }
      const btn = document.getElementById('vmbtn');
      if (btn) btn.textContent = `${selChs.size} selected`;
      updSelAllBtn();
      renderChannelList();
      if (selChs.size > 0) loadMedia();
      else {
        items.clear(); selItems.clear(); allMediaKeys = [];
        const grid = document.getElementById('mgrid');
        if (grid) { grid.innerHTML = ''; grid.style.display = 'none'; }
        document.getElementById('empty').style.display = 'flex';
        updPillCounts();
      }
    }

    window.selectChannel = (id) => {
      // Normalize to number if it's a numeric string
      const numId = (typeof id === 'string' && /^-?\d+$/.test(id)) ? parseInt(id) : id;
      selChs.clear();
      selChs.add(numId);

      lastSelChId = numId;
      switchSTab('channels');
      updSelAllBtn();
      renderChannelList();
      loadMedia();
    };

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

      const sf = document.getElementById('ch-actions');
      if (sf) {
        if (selChs.size > 0) {
          sf.classList.add('active');
          const countEl = document.getElementById('vmbtn-count');
          if (countEl) countEl.textContent = selChs.size;
        } else {
          sf.classList.remove('active');
        }
      }
      updSelCount();
    }

    function updPillCounts() {
      const counts = { all: 0, photo: 0, video: 0, document: 0 };
      for (const item of items.values()) {
        counts.all++;
        if (counts[item.type] !== undefined) counts[item.type]++;
      }
      
      const select = document.getElementById('mfilter');
      if (select) {
        select.options[0].textContent = counts.all > 0 ? `All Media (${counts.all.toLocaleString()})` : 'All Media';
        select.options[1].textContent = counts.photo > 0 ? `📷 Photos (${counts.photo.toLocaleString()})` : '📷 Photos';
        select.options[2].textContent = counts.video > 0 ? `🎬 Videos (${counts.video.toLocaleString()})` : '🎬 Videos';
        select.options[3].textContent = counts.document > 0 ? `📄 Documents (${counts.document.toLocaleString()})` : '📄 Documents';
      }
    }

    window.filterChs = q => {
      renderChannelList();
    };

    window.switchSTab = (name, el) => {
      document.querySelectorAll('.stab').forEach(t => t.classList.remove('active'));
      if (el) el.classList.add('active');
      document.querySelectorAll('.sidebar .sidebar-pane').forEach(p => p.classList.remove('active'));
      document.getElementById('sp-' + name)?.classList.add('active');
      if (name === 'channels' || name === 'gallery') updViewport();
      // On mobile, hide the sidebar after selecting a tab
      document.querySelector('.sidebar')?.classList.remove('m-active');
    };


    function updViewport() {
      const grid = document.getElementById('mgrid');
      if (!grid) return;
      viewportHeight = grid.clientHeight;
      const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 150;
      columns = Math.floor((grid.clientWidth - 24) / rowH) || 1;
      renderVirtual();
    }

    function renderVirtual(dir = 'down') {
      const grid = document.getElementById('mgrid');
      if (!grid) return;

      if (!filteredKeys.length) {
        grid.innerHTML = ''; grid.style.height = '';
        return;
      }

      const zoomVal = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 220;
      const desiredWidth = zoomVal;
      const gap = 24;           // Matches --gap in CSS
      columns = Math.max(1, Math.floor((grid.clientWidth - 24) / (desiredWidth + gap)));
      const totalGapWidth = (columns - 1) * gap;
      const itemWidth = (grid.clientWidth - 24 - totalGapWidth) / columns;
      
      // Detect aspect ratio based on mode
      const isGallery = grid.classList.contains('gallery-mode');
      const rowH = isGallery ? (itemWidth * 9 / 16) + gap : itemWidth + gap;

      const totalRows = Math.ceil(filteredKeys.length / columns);
      const totalH = totalRows * rowH + 60;

      let spacer = grid.querySelector('.vs-spacer');
      if (!spacer) {
        spacer = document.createElement('div');
        spacer.className = 'vs-spacer';
        spacer.style.height = '1px';
        spacer.style.width = '1px';
        spacer.style.position = 'absolute';
        grid.prepend(spacer);
      }
      spacer.style.top = totalH + 'px'; // Push footer
      grid.style.position = 'relative';

      const st = grid.scrollTop;
      const vh = grid.clientHeight || 800;
      
      // ── Extreme Windowing ──
      const baseBuffer = 1; // Minimal buffer for memory efficiency
      const predBuffer = 3; 
      
      let startRow = Math.max(0, Math.floor(st / rowH) - baseBuffer);
      let endRow = Math.min(totalRows, Math.ceil((st + vh) / rowH) + baseBuffer);
      
      if (dir === 'down') endRow = Math.min(totalRows, endRow + predBuffer);
      else if (dir === 'up') startRow = Math.max(0, startRow - predBuffer);

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
      for (const c of grid.children) if (c.dataset.key) existing.set(c.dataset.key, c);

      // Positioning visible cards using GPU-accelerated Transforms
      const fragment = document.createDocumentFragment();
      let added = 0;

      visibleKeys.forEach((key, i) => {
        const item = items.get(key);
        if (!item) return;

        let el = existing.get(key);
        const isNew = !el;
        if (isNew) {
          el = makeCard(key, item);
          thumbObs.observe(el);
          fragment.appendChild(el);
          added++;
        }

        const globalIdx = startIdx + i;
        const row = Math.floor(globalIdx / columns);
        const col = globalIdx % columns;
        
        el.style.width = itemWidth + 'px';
        el.style.height = (rowH - gap) + 'px';
        
        const x = 12 + col * (itemWidth + gap);
        const y = 12 + row * rowH;
        el.style.left = x + 'px';
        el.style.top = y + 'px';
        el.style.transform = ''; // Clear any animation remnants
      });

      if (added > 0) grid.appendChild(fragment);
    }

    const mgrid = document.getElementById('mgrid');
    let lastST = 0;
    if (mgrid) {
      mgrid.addEventListener('scroll', () => {
        const st = mgrid.scrollTop;
        const dir = st > lastST ? 'down' : 'up';
        lastST = st;

        if (vsRAF) return;
        vsRAF = requestAnimationFrame(() => { 
          vsRAF = null; 
          renderVirtual(dir); 
        });
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
          const rowH = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--row-h')) || 150;
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

    // High-Precision Layout Watcher
    if (mgrid) {
      const ro = new ResizeObserver(() => {
        if (vsRAF) return;
        vsRAF = requestAnimationFrame(() => { vsRAF = null; renderVirtual(); });
      });
      ro.observe(mgrid);
    }

    window.addEventListener('resize', updViewport);

    // ── Media loading ───────────────────────────────────────────────────────
    window.setFilter = (val, el) => {
      filter = val;
      const select = document.getElementById('mfilter');
      if (select) select.value = val;
      
      document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
      if (el) el.classList.add('active');
      
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
          
          if (!it.hay) {
            it.hay = `${it.filename || ''} ${it.caption || ''} ${it.date || ''} ${it.type || ''}`.toLowerCase();
          }

          if (!words.every(w => it.hay.includes(w))) continue;
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
          if (!ia || !ib) return 0;
          if (currentSort === 'newest')    return (ib.msg_id || 0) - (ia.msg_id || 0);
          if (currentSort === 'oldest')    return (ia.msg_id || 0) - (ib.msg_id || 0);
          if (currentSort === 'size_desc') return (ib.size || 0) - (ia.size || 0);
          if (currentSort === 'size_asc')  return (ia.size || 0) - (ib.size || 0);
          if (currentSort === 'title') {
            const fa = (ia.filename || '').toLowerCase(), fb = (ib.filename || '').toLowerCase();
            return fa.localeCompare(fb);
          }
          return 0;
        });
      }

      filteredKeys = keys;
      renderVirtual();
    }

    const lb = document.getElementById('lb');
    const lbCnt = document.getElementById('lb-content');
    const lbInfo = document.getElementById('lb-info');
    let lbIdx = 0;
    let lbActiveMedia = null;

    function lbOpen(key) {
      lbIdx = filteredKeys.indexOf(key);
      if (lbIdx === -1) return;
      lb.classList.add('open');
      document.body.classList.add('lb-active');
      lbLoad(lbIdx);
    }

    function lbClose() {
      lb.classList.remove('open');
      document.body.classList.remove('lb-active');
      if (lbActiveMedia) { lbActiveMedia.pause?.(); lbActiveMedia.src = ''; lbActiveMedia = null; }
    }

    async function lbLoad(idx) {
      const key = filteredKeys[idx];
      const item = items.get(key);
      if (!item) return;

      lbCnt.innerHTML = '<div class="lb-loader"></div>';
      const counter = `<span style="font-weight:700; color:var(--accent); margin-right:12px">${idx + 1} / ${filteredKeys.length}</span>`;
      lbInfo.innerHTML = `${counter} ${esc(item.filename)} <span style="opacity:0.6; margin-left:12px">${hrSize(item.size)} · ${item.date}</span>`;

      const media = await _buildMedia(item);
      lbCnt.innerHTML = '';
      lbCnt.appendChild(media);
      lbActiveMedia = media;
      
      // Prefetch siblings
      _preload(idx + 1);
      _preload(idx - 1);
    }

    async function _buildMedia(item) {
      const url = `/api/preview/${item.channel_id}/${item.msg_id}`;
      if (item.type === 'video') {
        const vid = document.createElement('video');
        vid.src = url; vid.controls = true; vid.autoplay = true; vid.playsInline = true;
        return vid;
      }
      return new Promise((resolve) => {
        const img = new Image();
        img.onload = () => resolve(img);
        img.onerror = () => {
          const d = document.createElement('div');
          d.textContent = 'Preview not available';
          d.style.cssText = 'color:rgba(255,255,255,.4);font-size:13px;padding:40px';
          resolve(d);
        };
        img.src = url;
      });
    }

    function openPreview(key) { lbOpen(key); }
    window.openPreview = openPreview;

    window.searchMedia = q => { currentSearch = q.toLowerCase(); applyFilters(); };
    window.setSort = by => { 
        currentSort = by; 
        const select = document.getElementById('msort');
        if (select) select.value = by;
        
        // If we have items, just re-sort them locally first
        if (allMediaKeys.length > 0) {
            applyFilters();
        }
        
        // ALWAYS re-fetch from server to ensure we have the correct top items for the new sort
        if (selChs.size > 0) loadMedia();
    };
    window.sortMedia = window.setSort;

    function _preload(idx) {
      if (idx < 0 || idx >= filteredKeys.length) return;
      const k = filteredKeys[idx]; if (!k) return;
      const it = items.get(k); if (!it || it.type === 'video') return;
      if (it._lb_prefetched) return;
      
      const p = new Image(); 
      p.src = `/api/preview/${it.channel_id}/${it.msg_id}`;
      it._lb_prefetched = true;
    }
    function lbNav(dir) { 
        const next = lbIdx + dir; 
        if (next < 0 || next >= filteredKeys.length) return; 
        lbIdx = next; 
        lbLoad(lbIdx, dir); 
        // Aggressive sequential prefetching (+2, -2)
        _preload(lbIdx + 1); _preload(lbIdx + 2);
        _preload(lbIdx - 1); _preload(lbIdx - 2);
    }

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
      card.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px"><div class="pn">📦 ${count} file${count !== 1 ? 's' : ''}</div><button class="vbtn" style="width:auto;padding:3px 8px;font-size:10px" onclick="toggleLogs('${job_id}')">Logs</button></div><div class="pb-wrap"><div class="pb" id="pb-${job_id}"></div></div><div class="pm"><span id="pc-${job_id}">Queued…</span><span id="pp-${job_id}">0%</span></div><div id="logs-${job_id}" class="m-logs" style="display:none"></div><div class="perr" id="pe-${job_id}" style="display:none"></div><button class="pcancel" onclick="cancelJob('${job_id}')">Cancel</button>`;
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
    window.toggleActivity = () => {
      document.querySelector('.ws').classList.toggle('drawer-open');
    };
    window.toggleSidebar = () => {
      const ws = document.querySelector('.ws');
      const sb = document.querySelector('.sidebar');
      const icon = document.getElementById('sb-toggle-icon');
      ws.classList.toggle('sidebar-collapsed');
      const isCollapsed = sb.classList.toggle('collapsed');
      if (icon) icon.style.transform = isCollapsed ? 'rotate(180deg)' : 'rotate(0deg)';
      setTimeout(updViewport, 450);
    };
    window.closeDr = () => document.querySelector('.ws').classList.remove('drawer-open');
    window.resetActivity = async () => {
      if (!confirm('🛑 WARNING: This will permanently stop ALL active sync rules and CLEAR all activity history. Continue?')) return;
      try { await api('/api/jobs/reset', { method: 'POST' }); toast('All activity and logs cleared', 'ok'); document.getElementById('plist').innerHTML = ''; document.getElementById('mir-history').innerHTML = ''; document.getElementById('mir-syncs').innerHTML = '<div style="font-size:11px;color:var(--muted)">No active syncs</div>'; addMirrorCard('sync_activity', 'Live', 'Sync', 'Running'); } catch (e) { toast(e.message, 'err'); }
    };

    window.loadGlobalGallery = async () => {
      if (stream) { stream.close(); stream = null; }
      items.clear();
      mediaRegistry.clear();
      selItems.clear();
      allMediaKeys = [];
      filteredKeys = [];
      const mgrid = document.getElementById('mgrid');
      mgrid.innerHTML = '';
      mgrid.style.display = 'grid';
      document.getElementById('empty').style.display = 'none';
      const sbar = document.getElementById('sbar');
      sbar.style.display = 'flex';
      document.getElementById('sbar-txt').textContent = 'Loading Global Media Library...';
      try {
        const sort = currentSort || 'newest';
        const data = await api(`/api/gallery-data?limit=10000&sort=${sort}`);
        data.forEach(it => {
          const key = `${it.channel_id}_${it.msg_id}`;
          mediaRegistry.set(key, it);
          items.set(key, it); 
          allMediaKeys.push(key);
        });
        filteredKeys = [...allMediaKeys];
        currentSearch = '';
        currentSort = '';
        document.getElementById('sbar-txt').textContent = `Showing all ${allMediaKeys.length} items from cache`;
        renderVirtual(true);
        updViewport();
      } catch (e) { toast(e.message, 'err'); }
    };

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

    window.stopSync = async (src, dst) => { try { await api('/api/mirror/sync/stop', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ source_id: src, target_id: dst }) }); toast('Sync stopped', 'ok'); loadSyncRules(); } catch (e) { toast(e.message, "err"); } };

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
      const cp = document.getElementById('cp-overlay');
      const cpActive = cp && cp.classList.contains('active');

      // Command Palette Trigger
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') { e.preventDefault(); toggleCP(); return; }

      if (cpActive) {
        if (e.key === 'Escape') { e.preventDefault(); closeCP(); return; }
        // Arrow/Enter handled by input listener for robustness
        if (['ArrowUp', 'ArrowDown', 'Enter'].includes(e.key)) return; 
      }

      if (isInput) return;

      if (e.key === 'Escape') { lbClose(); hideTruePreview(); return; }
      
      const lb = document.getElementById('lb');
      const isLb = lb && (lb.classList.contains('active') || lb.classList.contains('open'));
      
      if (isLb) {
        if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') { e.preventDefault(); lbNav(-1); return; }
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') { e.preventDefault(); lbNav(1); return; }
        return;
      }

      if (['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(e.key)) {
        e.preventDefault();
        peekByKeys(e.key.replace('Arrow', '').toLowerCase(), e);
        return;
      }

      if (e.key === ' ') { e.preventDefault(); const key = filteredKeys[peekIdx]; if (key) openPreview(key); }
      if (e.key === 'Enter' && !e.metaKey && !e.ctrlKey) { const key = filteredKeys[peekIdx]; if (key) openPreview(key); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'a') { e.preventDefault(); selAll(); }
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { if (selItems.size) doDl(); }
    });

    document.getElementById('msearch')?.addEventListener('input', debounce(e => searchMedia(e.target.value), 250));

    function showErr(id, m) { const el = document.getElementById(id); if (el) { el.textContent = m; el.style.display = 'block'; } }
    function hideErr(id) { const el = document.getElementById(id); if (el) el.style.display = 'none'; }

    // Final entry point
    (async () => {
      try {
        const r = await api('/api/auth/status');
        if (r.authenticated) { sv('vapp'); initApp(); }
        else sv('va');
        if (window.lucide) lucide.createIcons();
      } catch { sv('va'); }
    })();

  })(); // Close main IIFE
} // Close idx-page check
