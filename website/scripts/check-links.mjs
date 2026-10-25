// Checks every internal link and asset reference in the built site.
// Usage: node scripts/check-links.mjs [dist-dir]
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';

const dist = resolve(process.argv[2] ?? process.env.SITE_OUT_DIR ?? 'dist');
const baseArg = process.env.PUBLIC_SITE_BASE;
const base = (() => {
  if (baseArg === '/') return '/';
  if (baseArg) return `/${baseArg.replace(/^\/+|\/+$/g, '')}/`;
  // Default: the first configured library's repo path, read from the emitted canonical link.
  const index = readFileSync(join(dist, 'index.html'), 'utf8');
  const href = index.match(/<link rel="icon" href="([^"]*)logo\.svg"/)?.[1];
  return href ?? '/';
})();

const walk = (dir) =>
  readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    return statSync(path).isDirectory() ? walk(path) : [path];
  });

// The site's own pages: an examples app published into `examples/` (the
// library's live examples) links its files relatively, as built.
const examples = join(dist, 'examples');
const pages = walk(dist).filter((p) => p.endsWith('.html') && !p.startsWith(`${examples}/`));
const ids = new Map();
const problems = [];

function exists(pathname) {
  const rel = decodeURIComponent(pathname.slice(base.length)).replace(/\/$/, '');
  const candidates = [rel, `${rel}.html`, join(rel, 'index.html')];
  return candidates.map((c) => join(dist, c)).find((c) => existsSync(c) && statSync(c).isFile());
}

function idsOf(file) {
  if (!ids.has(file)) {
    const html = readFileSync(file, 'utf8');
    ids.set(file, new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1])));
  }
  return ids.get(file);
}

for (const page of pages) {
  const html = readFileSync(page, 'utf8');
  const label = page.slice(dist.length + 1);
  for (const [, , url] of html.matchAll(/\s(href|src)="([^"]+)"/g)) {
    if (/^(?:[a-z][a-z\d+.-]*:|\/\/|#|mailto:)/i.test(url) && !url.startsWith('#')) continue;
    const [pathAndQuery, hash] = url.split('#');
    // A query (an example's `?demo=`) names what a page shows, not another file.
    const pathPart = pathAndQuery.split('?')[0];
    let target = page;
    if (pathPart) {
      if (!pathPart.startsWith('/')) { problems.push(`${label}: relative URL ${url}`); continue; }
      if (!pathPart.startsWith(base)) { problems.push(`${label}: ${url} outside base ${base}`); continue; }
      target = exists(pathPart);
      if (!target) { problems.push(`${label}: broken link ${url}`); continue; }
    }
    if (hash && target.endsWith('.html') && !idsOf(target).has(decodeURIComponent(hash))) {
      problems.push(`${label}: missing anchor ${url}`);
    }
  }
}

if (problems.length) {
  console.error(`${problems.length} link problem(s):\n${[...new Set(problems)].map((p) => `  ${p}`).join('\n')}`);
  process.exit(1);
}
console.log(`Checked ${pages.length} pages: all internal links and anchors resolve.`);
