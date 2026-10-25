import { resolve } from 'node:path';
import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import pagefind from 'astro-pagefind';
import { unified } from '@astrojs/markdown-remark';
import { rehypeHeadingIds } from '@astrojs/markdown-remark';
import { remarkCallouts } from './src/lib/remark-callouts.js';
import { remarkDocLinks } from './src/lib/remark-doc-links.js';
import { remarkMermaid } from './src/lib/remark-mermaid.js';
import { remarkExamples } from './src/lib/remark-examples.js';
import { rehypeHeadingAnchors } from './src/lib/rehype-heading-anchors.js';
import { rehypeTableScroll } from './src/lib/rehype-table-scroll.js';
import { shikiConfig, defaultHighlightLang } from './src/lib/markdown.js';
import { REPO_NAME } from './site.config.mjs';

// A GitHub Pages project site lives at https://dxnsix.github.io/<repo>/.
// `PUBLIC_SITE_BASE=/` serves from the root instead (previews, custom domains).
const configuredBase = process.env.PUBLIC_SITE_BASE ?? `/${REPO_NAME}/`;
const BASE = configuredBase === '/' ? '/' : `/${configuredBase.replace(/^\/+|\/+$/g, '')}/`;

export default defineConfig({
  site: process.env.SITE_URL || 'https://dxnsix.github.io',
  base: BASE,
  outDir: resolve(process.cwd(), process.env.SITE_OUT_DIR || './dist'),
  output: 'static',
  trailingSlash: 'never',

  integrations: [pagefind()],

  markdown: {
    // Astro 7 defaults to Sätteri; the remark/rehype pipeline is opt-in.
    processor: unified({
      remarkPlugins: [remarkMermaid, [remarkExamples, { base: BASE }], remarkCallouts, [remarkDocLinks, { base: BASE }]],
      rehypePlugins: [rehypeHeadingIds, rehypeHeadingAnchors, rehypeTableScroll],
    }),
    shikiConfig,
    defaultHighlightLang,
  },

  vite: {
    plugins: [tailwindcss()],
  },
});
