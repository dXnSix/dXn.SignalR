import { readFileSync } from 'node:fs';
import { join } from 'node:path';

// The site ships its own Shiki themes; `global.css` derives its `--code-*`
// values from them, so a stock theme would put the code block's own colours out
// of step with the chrome around it. Read from the project root, which is where
// Astro runs.
const theme = (name) =>
  JSON.parse(readFileSync(join(process.cwd(), 'src', `${name}.theme.json`), 'utf8'));

export const shikiConfig = {
  themes: {
    light: theme('light'),
    dark: theme('dark'),
  },
  defaultColor: 'light',
  langs: ['rust', 'toml', 'json', 'bash', 'csharp', 'typescript', 'javascript', 'html', 'yaml', 'diff', 'text'],
  langAlias: { rs: 'rust', sh: 'bash', shell: 'bash', cs: 'csharp', ts: 'typescript', js: 'javascript' },
};

export const defaultHighlightLang = 'text';
