// Shared implementation lives in src/core/ — no duplication.
// src/core/tid.ts already uses globalThis.crypto which works in both Node 20+
// and browsers, so no browser-specific shim is needed.
export * from '$core/tid.js';
