import type { APIRoute } from 'astro';
import { loadDocs, docRoute } from '../lib/docs';

export const GET: APIRoute = async ({ site }) => {
  const { pages } = await loadDocs();
  const root = import.meta.env.BASE_URL;
  const urls = [root, ...pages.map(docRoute)].map((path) => new URL(path, site).href);
  const body = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls
    .map((loc) => `  <url><loc>${loc}</loc></url>`)
    .join('\n')}\n</urlset>\n`;
  return new Response(body, { headers: { 'Content-Type': 'application/xml; charset=utf-8' } });
};
