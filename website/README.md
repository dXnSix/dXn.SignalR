# dXn.SignalR documentation site

The source of https://dxnsix.github.io/dXn.SignalR/. An [Astro](https://astro.build) static site: a landing
page plus a documentation section with sidebar, table of contents, search (Pagefind) and
prev/next links, in the style of the gpui-kit documentation.

```bash
pnpm install
pnpm dev            # http://localhost:4321/dXn.SignalR/
pnpm build          # static site in dist/ plus the search index
pnpm check:links    # verify every internal link and anchor in dist/
```

## Where things live

| Path | What |
|---|---|
| `content/index.md` | Landing page: hero, facts, feature cards, quick tour (frontmatter drives the hero) |
| `content/docs/*.md` | Documentation pages; `group` and `order` in the frontmatter build the sidebar |
| `site.config.mjs` | Which library this site documents |
| `src/` | Layouts, components, styles and the Markdown plugins |

## Writing pages

A documentation page needs this frontmatter, then plain Markdown with no `# H1`:

```yaml
---
title: Aliases and references
description: One sentence for search results and the meta tag.
group: Concepts      # sidebar group, in order of its lowest `order`
order: 40            # ascending across the library; steps of 10
---
```

- Link between pages with a relative path to the `.md` file (`[Aliases](./aliases.md#cycles)`).
  The build rewrites it and **fails on a broken link**; site-root paths are rejected.
- Callouts: `:::tip Title`, text, `:::` on consecutive lines (`tip`, `info`, `note`, `warning`,
  `danger`; paragraph text only).
- Code fences use `rust`, `toml`, `json`, `bash`, `csharp`, `typescript`, `javascript`, `html`,
  `yaml`, `diff` or `text`. Add `{3,5-7}` after the language to highlight lines.
- Diagrams: a `mermaid` fence renders as SVG in the browser and follows the light/dark theme.
  Prefer `flowchart TB` for diagrams with many nodes; the reading column is narrow.

## Deployment

`.github/workflows/docs.yml` builds this directory on every pull request and deploys to GitHub Pages
on a push to the default branch. Pages must be set to **Source: GitHub Actions** once, in the
repository settings. `PUBLIC_SITE_BASE=/` builds for a site served from the root (a custom domain).
