// Shared constants — mirrors src/config.ts without Node.js deps.

export const RECORD_TYPE = 'fm.teal.alpha.feed.play';
export const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue';
export const MAX_PDS_BATCH_SIZE = 200;
export const POINTS_PER_RECORD = 3;

// __WEB_VERSION__ is injected at build time by vite.config.ts → define.__WEB_VERSION__
// Keep this file free of import statements so it stays side-effect-free.
declare const __WEB_VERSION__: string;
export const CLIENT_AGENT = `malachite/v${__WEB_VERSION__} (web)`;
