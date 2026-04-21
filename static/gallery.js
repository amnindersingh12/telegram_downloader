(function () {
  let items = [];
  let cur = 0;
  let offset = 0;
  const limit = 100;

  const params = new URLSearchParams(location.search);
  const channels = params.get('channels') || '';

  async function loadGallery() {
    try {
      const r = await fetch(`/api/gallery-data?offset=${offset}&limit=${limit}${channels ? '&channels=' + channels : ''}`);
      const data = await r.json();
      if (offset === 0) items = data;
      else items = items.concat(data);
      
      const btn = document.getElementById('load-more');
      if (btn) btn.style.display = data.length < limit ? 'none' : 'block';
    } catch { items = []; }
    render();
  }

  window.loadMore = () => {
    offset += limit;
    loadGallery();
  };

  function createCard(item, i, grid) {
    if (!item) return;
    const card = document.createElement('div');
    card.className = 'card' + (i % 5 === 1 ? ' wide' : '') + (i % 7 === 3 ? ' tall' : '');
    card.dataset.idx = i;
    card.addEventListener('click', () => openLb(i));
    const isVid = item.type === 'video';
    card.innerHTML = `
      ${isVid ? `<video class="card-media" poster="${item.thumb}" preload="none" muted playsinline loop></video>
        <div class="play-ring"><button class="play-btn" aria-label="Play">
          <svg viewBox="0 0 20 20"><path d="M5 3l13 7-13 7z"/></svg></button></div>`
        : `<img class="card-media" src="${item.thumb}" loading="lazy" fetchpriority="high" alt="${esc(item.title)}">`}
      <div class="badge">${item.type.toUpperCase()}</div>
      <div class="card-caption">
        <div class="card-title">${esc(item.title)}</div>
        ${item.caption ? `<div class="card-sub">${esc(item.caption.slice(0, 80))}</div>` : ''}
      </div>`;
    
    if (isVid) {
      card.addEventListener('mouseenter', () => {
        const v = card.querySelector('video');
        if (v) {
          if (!v.src && item.preview) {
            v.src = item.preview;
            v.load();
          }
          v.play().catch(() => {});
        }
      });
      card.addEventListener('mouseleave', () => {
        const v = card.querySelector('video');
        if (v) v.pause();
      });
    }
    grid.appendChild(card);
  }

  function render() {
    const grid = document.getElementById('grid');
    if (!grid) return;
    if (offset === 0) grid.innerHTML = '';

    const photos = items.filter(i => i.type === 'photo').length;
    const videos = items.filter(i => i.type === 'video').length;
    const hct = document.getElementById('hct');
    if (hct) hct.textContent = `${photos} photo${photos !== 1 ? 's' : ''} · ${videos} video${videos !== 1 ? 's' : ''}`;

    if (!items.length) {
      grid.innerHTML = '<div class="empty"><div class="empty-icon">🖼</div><div>No media in cache yet</div></div>';
      return;
    }

    const start = offset === 0 ? 0 : items.length - limit;
    for (let i = start; i < items.length; i++) {
      createCard(items[i], i, grid);
    }
  }

  function fullRender() {
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

    for (let i = 0; i < items.length; i++) {
      createCard(items[i], i, grid);
    }
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

  let es = null;
  function startLiveUpdates() {
    if (es) es.close();
    es = new EventSource('/api/updates');
    es.onmessage = e => {
      try {
        const d = JSON.parse(e.data);
        if (d.type === 'new_message' && d.item) {
          const item = d.item;
          if (channels) {
            const allowed = channels.split(',');
            if (!allowed.includes(String(item.channel_id))) return;
          }
          if (!items.some(i => i.msg_id === item.msg_id && i.channel_id === item.channel_id)) {
            items.unshift(item);
            offset++;
            // Prepend directly to DOM for smoothness
            const grid = document.getElementById('grid');
            if (grid) {
              const dummy = document.createElement('div');
              createCard(item, 0, dummy); // idx doesn't matter for first item prepend
              const card = dummy.firstElementChild;
              if (card) {
                grid.insertBefore(card, grid.firstChild);
                // Shift all dataset-idx in the grid
                const cards = grid.querySelectorAll('.card');
                cards.forEach((c, idx) => { c.dataset.idx = idx; });
              }
            }
          }
        }
      } catch {}
    };
    es.onerror = () => { es.close(); setTimeout(startLiveUpdates, 5000); };
  }

  startLiveUpdates();
  loadGallery();

})(); 
