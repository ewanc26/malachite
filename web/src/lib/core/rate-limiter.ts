// Shared implementation lives in src/core/ — no duplication.
// RateLimiter covers both Node and browser (globalThis.crypto works in both).
export * from '$core/rate-limiter.js';
// Backwards-compatible alias for any code that referenced BrowserRateLimiter.
export { RateLimiter as BrowserRateLimiter } from '$core/rate-limiter.js';
