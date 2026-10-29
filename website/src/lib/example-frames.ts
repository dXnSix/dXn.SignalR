// Starts the live examples' frames one after another. Each frame is a copy of
// the library's examples app, and each copy starts its own web runtime (a
// WebAssembly module of several MB and the app's code), so frames started
// together share the page's main thread and wait on one another; and a cold
// cache fetches the module once per frame. A frame (`remark-examples`) holds
// its address in `data-src` (the home page's hero frame too): the next one starts when the one before reports
// that it shows something (`dxn-example-ready`, or the height of its content),
// fails, or has had `GIVE_UP` ms. Only frames near the viewport start.
const GIVE_UP = 25000;

interface Frame {
  el: HTMLIFrameElement;
  wanted: boolean;
}

let started = false;

export function startExampleFrames() {
  if (started) return;
  started = true;

  // The home page's hero frame comes first in the page, and is wanted at once.
  const frames: Frame[] = [...document.querySelectorAll<HTMLIFrameElement>('iframe[data-src]')].map((el) => ({
    el,
    wanted: el.closest('.hero-example') !== null,
  }));
  if (!frames.length) return;

  let busy: HTMLIFrameElement | null = null;
  let giveUp: ReturnType<typeof setTimeout> | undefined;

  const next = () => {
    if (busy) return;
    const frame = frames.find((f) => f.wanted && f.el.dataset.src);
    if (!frame) return;
    const el = frame.el;
    busy = el;
    el.src = el.dataset.src!;
    delete el.dataset.src;
    el.closest('figure')?.setAttribute('data-state', 'loading');
    clearTimeout(giveUp);
    giveUp = setTimeout(() => done(el), GIVE_UP);
  };

  const done = (el: HTMLIFrameElement) => {
    el.removeAttribute('data-pending');
    el.closest('figure')?.setAttribute('data-state', 'ready');
    if (busy !== el) return;
    clearTimeout(giveUp);
    busy = null;
    next();
  };

  window.addEventListener('message', (event) => {
    if (event.origin !== location.origin) return;
    const type = event.data?.type;
    if (type !== 'dxn-example-ready' && type !== 'dxn-example-height') return;
    for (const { el } of frames) {
      if (el.contentWindow === event.source && !el.dataset.src) done(el);
    }
  });

  const mark = (f: Frame) => {
    f.wanted = true;
    next();
  };
  next();
  if (!('IntersectionObserver' in window)) {
    frames.forEach(mark);
    return;
  }
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        observer.unobserve(entry.target);
        const f = frames.find((x) => x.el === entry.target);
        if (f) mark(f);
      }
    },
    { rootMargin: '400px 0px' },
  );
  for (const f of frames) if (!f.wanted) observer.observe(f.el);
}
