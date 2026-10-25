import { existsSync } from 'node:fs';
import { dirname, relative, resolve, sep } from 'node:path';
import { CONTENT_DIR } from '../../site.config.mjs';

// Pages link to each other as relative `.md` paths, so a source file reads
// correctly on GitHub and in an editor. This maps a link's target file back to
// the route that renders it, and fails the build when the target is missing.
//
// Content lives in `index.md` (route `/`) and `docs/<slug>.md` (route
// `/docs/<slug>`) under the content directory, so a route is the path relative
// to it, minus the extension, with `index` dropped.

const CONTENT_ROOT = resolve(process.cwd(), CONTENT_DIR);

const ASSET = /\.(?:png|jpe?g|gif|svg|webp|avif|ico|pdf|zip|gz|txt|json|toml|ya?ml|rs|css|js|mjs|ts)$/i;

const isExternal = (url) => /^[a-z][a-z\d+.-]*:/i.test(url) || url.startsWith('//');

export function remarkDocLinks({ base = '/' } = {}) {
  const prefix = base.replace(/\/+$/, '');

  return (tree, file) => {
    const from = file?.path ?? file?.history?.[0];
    if (!from) return;
    const fromDir = dirname(from);

    const rewrite = (node) => {
      const url = node.url;
      if (!url || isExternal(url) || url.startsWith('#')) return;
      if (url.startsWith('/')) {
        throw new Error(`${relative(process.cwd(), from)}: site-root link "${url}"; use a relative .md path instead`);
      }

      const hashAt = url.indexOf('#');
      const target = hashAt === -1 ? url : url.slice(0, hashAt);
      const hash = hashAt === -1 ? '' : url.slice(hashAt);
      if (!target || ASSET.test(target)) return;

      if (!target.endsWith('.md')) {
        throw new Error(`${relative(process.cwd(), from)}: link "${url}" must point at a .md file`);
      }
      const absolute = resolve(fromDir, target);
      if (!existsSync(absolute)) {
        const message = `${relative(process.cwd(), from)}: broken link "${url}"`;
        // While pages are still being written, `DOCS_LINKS=warn` reports instead of failing.
        if (process.env.DOCS_LINKS === 'warn') console.warn(`[doc-links] ${message}`);
        else throw new Error(message);
        return;
      }
      const rel = relative(CONTENT_ROOT, absolute.slice(0, -'.md'.length));
      if (rel.startsWith('..')) {
        throw new Error(`${relative(process.cwd(), from)}: link "${url}" leaves the content directory`);
      }

      const route = `/${rel.split(sep).join('/')}`.replace(/\/index$/, '');
      node.url = `${prefix}${route || '/'}${hash}`;
    };

    const walk = (node) => {
      if (node.type === 'link' || node.type === 'definition') rewrite(node);
      for (const child of node.children ?? []) walk(child);
    };
    walk(tree);
  };
}
