// ```example fences embed a live example: an iframe of the examples app the
// library publishes with its site, at `<base>examples/index.html?demo=<name>`
// (the page by name: the site serves no directory with a trailing slash). The fence
// holds `key: value` lines:
//
//   demo: trims                       the example to show (required)
//   height: 260                       the frame's height in pixels (default 260); an app that
//                                     reports its content's height (below) overrides it
//   caption: A trim drawing on.       a line under the frame (optional)
//   title: Trims                      the frame's accessible name (default: the caption, or the demo)
//
// An example may size its frame to its content: the app posts
// `{ type: 'dxn-example-height', height }` (CSS pixels) to its parent window and the page's
// script (DocsLayout) sets the frame's height to it, so the page scrolls rather than the frame.
//
// The frame has no `src` yet: `data-src` holds it, and `lib/example-frames.ts` starts the frames one
// after another as they near the viewport, each when the one before shows something.
//
// The app itself is built by the library's own workflow into `public/examples/`;
// the link checker fails the build when it is missing.
const escapeHtml = (text) =>
  text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

const KEYS = new Set(['demo', 'height', 'caption', 'title']);

function parse(value, where) {
  const fields = {};
  for (const line of value.split('\n')) {
    if (!line.trim()) continue;
    const at = line.indexOf(':');
    const key = at === -1 ? '' : line.slice(0, at).trim();
    if (!KEYS.has(key)) throw new Error(`${where}: example block: unknown line "${line.trim()}"`);
    fields[key] = line.slice(at + 1).trim();
  }
  if (!/^[a-z0-9-]+$/.test(fields.demo ?? '')) {
    throw new Error(`${where}: example block needs "demo: <name>" (lowercase letters, digits and dashes)`);
  }
  const height = fields.height === undefined ? 260 : Number(fields.height);
  if (!Number.isInteger(height) || height < 80 || height > 1600) {
    throw new Error(`${where}: example block height must be a whole number of pixels from 80 to 1600`);
  }
  return { ...fields, height };
}

export function remarkExamples({ base = '/' } = {}) {
  const prefix = base.endsWith('/') ? base : `${base}/`;
  return (tree, file) => {
    const where = file?.path ?? file?.history?.[0] ?? 'markdown';
    const walk = (node) => {
      if (!node.children) return;
      node.children = node.children.map((child) => {
        if (child.type === 'code' && child.lang === 'example') {
          const { demo, height, caption, title } = parse(child.value, where);
          const name = title || caption || demo;
          const figcaption = caption ? `<figcaption>${escapeHtml(caption)}</figcaption>` : '';
          return {
            type: 'html',
            value:
              `<figure class="doc-example" data-pagefind-ignore>` +
              `<iframe title="${escapeHtml(name)}" data-src="${prefix}examples/index.html?demo=${demo}" data-pending style="height: ${height}px"></iframe>` +
              `${figcaption}</figure>`,
          };
        }
        walk(child);
        return child;
      });
    };
    walk(tree);
  };
}
