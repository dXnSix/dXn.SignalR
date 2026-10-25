import type { APIRoute } from 'astro';

export const GET: APIRoute = ({ site }) => {
  const sitemap = site ? new URL(`${import.meta.env.BASE_URL.replace(/\/$/, '')}/sitemap.xml`, site).href : '';
  return new Response(`User-agent: *\nAllow: /\n${sitemap ? `Sitemap: ${sitemap}\n` : ''}`, {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
};
