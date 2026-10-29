import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';
import { CONTENT_DIR } from '../site.config.mjs';

const link = z.object({
  title: z.string(),
  text: z.string(),
  link: z.string().optional(),
});

const pages = defineCollection({
  loader: glob({ pattern: '**/*.md', base: CONTENT_DIR }),
  schema: z.object({
    title: z.string().min(1),
    description: z.string().min(1),

    // Documentation pages
    group: z.string().optional(),
    order: z.number().optional(),

    // Landing page
    eyebrow: z.string().optional(),
    headline: z.string().optional(),
    lede: z.string().optional(),
    install: z.string().optional(),
    facts: z.array(z.object({ label: z.string(), value: z.string() })).optional(),
    heroTitle: z.string().optional(),
    heroLang: z.string().optional(),
    heroCode: z.string().optional(),
    heroExample: z.string().regex(/^[a-z0-9-]+$/).optional(),
    heroExampleTitle: z.string().optional(),
    features: z.array(link).optional(),
    next: z.array(link).optional(),
  }),
});

export const collections = { pages };
