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
