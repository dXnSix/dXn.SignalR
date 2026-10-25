export interface Library {
  id: 'dtcg' | 'reactive' | 'signalr' | 'vectoranimation';
  /** Display name, also the brand in the project switcher. */
  name: string;
  /** GitHub repository name; also the GitHub Pages path. */
  repoName: string;
  /** One line shown in the project switcher. */
  summary: string;
  repo: string;
  /** Published GitHub Pages site. */
  site: string;
  /** API docs URL, if the crate is published to a registry (none are today: they are git-tag dependencies). */
  apiDocs?: string;
  crate: string;
  /** Branch the Pages workflow deploys from. */
  branch: string;
}

const OWNER = 'dXnSix';

const make = (lib: Omit<Library, 'repo' | 'site'>): Library => ({
  ...lib,
  repo: `https://github.com/${OWNER}/${lib.repoName}`,
  site: `https://${OWNER.toLowerCase()}.github.io/${lib.repoName}/`,
});

export const libraries: Library[] = [
  make({
    id: 'dtcg',
    name: 'dXn.Dtcg',
    repoName: 'dXn.Dtcg',
    summary: 'Design tokens: resolve DTCG 2025.10 documents into typed Rust structs',
    crate: 'dxn_dtcg',
    branch: 'main',
  }),
  make({
    id: 'reactive',
    name: 'dXn.Reactive',
    repoName: 'dXn.Reactive',
    summary: 'Reactive state: signals, commands and incremental observable collections',
    crate: 'dxn_reactive',
    branch: 'main',
  }),
  make({
    id: 'signalr',
    name: 'dXn.SignalR',
    repoName: 'dXn.SignalR',
    summary: 'A SignalR client for Rust, native and in the browser',
    crate: 'dxn-signalr',
    branch: 'master',
  }),
  make({
    id: 'vectoranimation',
    name: 'dXn.VectorAnimation',
    repoName: 'dXn.VectorAnimation',
    summary: 'Animated vector graphics: SVG and Lottie in one scene model, played natively in NSH apps',
    crate: 'dxn_vector_animation',
    branch: 'main',
  }),
];

export const GITHUB_ORG = `https://github.com/${OWNER}`;

export const libraryById = (id: string): Library => {
  const lib = libraries.find((l) => l.id === id);
  if (!lib) throw new Error(`Unknown library: ${id}`);
  return lib;
};
