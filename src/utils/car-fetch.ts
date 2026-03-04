// Redirect to the single source of truth in src/core/.
// src/lib/sync.ts (legacy CLI path) imports from here; keep this shim so no
// other files need updating.
export * from '../core/car-fetch.js';
