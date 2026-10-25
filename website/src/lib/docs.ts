import { getCollection, type CollectionEntry } from 'astro:content';
import { LIBRARY_ID } from '../../site.config.mjs';
import { libraryById, type Library } from './libraries';

export type Page = CollectionEntry<'pages'>;

export interface SidebarItem {
  text: string;
  link?: string;
  items?: SidebarItem[];
}

export const library: Library = libraryById(LIBRARY_ID);

const BASE = import.meta.env.BASE_URL.replace(/\/$/, '');

export const docRoute = (page: Page) => `${BASE}/${page.id}`;

/** The landing page, the documentation pages in reading order, and the sidebar. */
export async function loadDocs() {
  const all = await getCollection('pages');
  const landing = all.find((p) => p.id === 'index');
  if (!landing) throw new Error('Missing content/index.md');

  const pages = all
    .filter((p) => p.id.startsWith('docs/'))
    .sort((a, b) => (a.data.order ?? 9999) - (b.data.order ?? 9999) || a.id.localeCompare(b.id));

  // Groups appear in the order of their first (lowest-order) page.
  const groups = new Map<string, SidebarItem[]>();
  for (const page of pages) {
    const name = page.data.group ?? 'Guides';
    if (!groups.has(name)) groups.set(name, []);
    groups.get(name)!.push({ text: page.data.title, link: docRoute(page) });
  }
  const sidebar: SidebarItem[] = [...groups].map(([text, items]) => ({ text, items }));

  return { landing, pages, sidebar };
}
