/**
 * @ewanc26/tid — AT Protocol TID generation
 *
 * Zero-dependency, spec-compliant TID (Timestamp Identifier) generation for
 * the AT Protocol. Works in Node.js 20+ and all modern browsers.
 *
 * Spec: https://atproto.com/specs/tid
 */

// ─── Base-32 codec ───────────────────────────────────────────────────────────
// AT Protocol uses a custom base-32 alphabet, not standard RFC 4648.

const S32 = '234567abcdefghijklmnopqrstuvwxyz';

// Reverse lookup built once at module load.
const S32_MAP: Record<string, number> = {};
for (let i = 0; i < S32.length; i++) S32_MAP[S32[i]] = i;

function s32encode(n: number): string {
  if (n === 0) return '2';
  let s = '';
  let v = n;
  while (v > 0) { s = S32[v % 32] + s; v = Math.floor(v / 32); }
  return s;
}

function s32decode(s: string): number {
  let n = 0;
  for (const ch of s) n = n * 32 + (S32_MAP[ch] ?? 0);
  return n;
}

// ─── Validation ──────────────────────────────────────────────────────────────

/**
 * A valid TID is exactly 13 characters in the AT Protocol base-32 alphabet,
 * and the first character must not exceed the high bit (i.e. it encodes a
 * non-negative 64-bit integer, so the leading digit is in 2–i).
 */
const TID_RE = /^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$/;

/**
 * Returns `true` if `tid` is a well-formed AT Protocol TID.
 */
export function validateTid(tid: string): boolean {
  return TID_RE.test(tid);
}

// ─── Validation helpers ───────────────────────────────────────────────────────

/**
 * Error thrown by `ensureValidTid` when a TID fails validation.
 */
export class InvalidTidError extends Error {
  constructor(message: string, public readonly tid?: string) {
    super(message);
    this.name = 'InvalidTidError';
  }
}

/**
 * Assert that `tid` is a valid AT Protocol TID.
 * Throws `InvalidTidError` if the format check fails.
 */
export function ensureValidTid(tid: string): asserts tid is string {
  if (!validateTid(tid)) {
    throw new InvalidTidError(`Invalid TID format: "${tid}"`, tid);
  }
}

// ─── Decode ──────────────────────────────────────────────────────────────────

export interface DecodedTid {
  /** Microseconds since the Unix epoch. */
  timestampUs: number;
  /** Clock identifier (0–31). */
  clockId: number;
  /** Convenience: the timestamp as a `Date` (millisecond precision). */
  date: Date;
}

/**
 * Decode a TID into its constituent parts.
 * Throws a `TypeError` if the TID is malformed.
 */
export function decodeTid(tid: string): DecodedTid {
  if (!validateTid(tid)) throw new TypeError(`Invalid TID: "${tid}"`);
  const timestampUs    = s32decode(tid.slice(0, 11));
  const decodedClockId = s32decode(tid.slice(11));
  return { timestampUs, clockId: decodedClockId, date: new Date(Math.floor(timestampUs / 1000)) };
}

/**
 * Decode a TID and return only the microsecond timestamp.
 * Prefer `decodeTid` for new code.
 */
export function decodeTidTimestamp(tid: string): number {
  return decodeTid(tid).timestampUs;
}

/**
 * Decode a TID and return only the clock identifier.
 * Prefer `decodeTid` for new code.
 */
export function decodeTidClockId(tid: string): number {
  return decodeTid(tid).clockId;
}

// ─── Monotonic clock ─────────────────────────────────────────────────────────

// Module-level state — one clock per JS context (process or browser tab).
let lastUs = 0;
let clockId = (() => {
  const buf = new Uint8Array(1);
  (globalThis.crypto ?? (globalThis as unknown as { webcrypto: Crypto }).webcrypto)
    .getRandomValues(buf);
  return buf[0] % 32;
})();
let generatedCount = 0;

function nextUs(targetUs: number): number {
  const us = targetUs <= lastUs ? lastUs + 1 : targetUs;
  lastUs = us;
  generatedCount++;
  return us;
}

function makeTid(us: number): string {
  return s32encode(us).padStart(11, '2') + s32encode(clockId).padStart(2, '2');
}

// ─── Generation ──────────────────────────────────────────────────────────────

/**
 * Generate a TID for a historical timestamp.
 *
 * Accepts either an ISO 8601 string or a `Date` object. Monotonicity is
 * guaranteed — if records arrive out of order the clock is bumped forward so
 * every call produces a strictly increasing TID within the same JS context.
 *
 * @example
 * const tid = generateTID('2023-11-01T12:00:00Z');
 * const tid = generateTID(new Date());
 */
export function generateTID(source: string | Date): string {
  const ms = typeof source === 'string' ? new Date(source).getTime() : source.getTime();
  return makeTid(nextUs(ms * 1000));
}

/**
 * Generate a TID for the current wall-clock time.
 *
 * @example
 * const tid = generateNextTID();
 */
export function generateNextTID(): string {
  return makeTid(nextUs(Date.now() * 1000));
}

// ─── Comparison ──────────────────────────────────────────────────────────────

/**
 * Compare two TIDs lexicographically.
 *
 * Because the AT Protocol base-32 alphabet is lexicographically ordered by
 * timestamp, string comparison is sufficient and correct.
 *
 * Returns `-1`, `0`, or `1` (suitable as a `Array.prototype.sort` comparator).
 */
export function compareTids(a: string, b: string): -1 | 0 | 1 {
  if (a < b) return -1;
  if (a > b) return  1;
  return 0;
}

// ─── Array utilities ─────────────────────────────────────────────────────────

/**
 * Returns `true` if every TID in the array is strictly greater than the one
 * before it (lexicographic / chronological order).
 */
export function areMonotonic(tids: string[]): boolean {
  for (let i = 1; i < tids.length; i++) {
    if (tids[i] <= tids[i - 1]) return false;
  }
  return true;
}

// ─── Clock state ─────────────────────────────────────────────────────────────

/**
 * Snapshot of the module-level clock state.
 * Intended for debugging and test assertions.
 */
export interface TidClockState {
  lastTimestampUs: number;
  clockId: number;
  generatedCount: number;
}

/**
 * Return a snapshot of the current clock state.
 * Useful for asserting counts in tests.
 */
export function getTidClockState(): TidClockState {
  return { lastTimestampUs: lastUs, clockId, generatedCount };
}

// ─── Testing utilities ───────────────────────────────────────────────────────

/**
 * Reset the module-level monotonic clock to zero.
 * Resets `lastTimestampUs` and `generatedCount` but preserves `clockId`.
 *
 * **Only use this in tests.** Calling it in production risks generating
 * duplicate or non-monotonic TIDs.
 */
export function resetTidClock(): void {
  lastUs = 0;
  generatedCount = 0;
}

/**
 * Seed the clock with a fixed starting timestamp and clock identifier,
 * making all subsequent TID generation deterministic.
 *
 * - `startUs` becomes the floor for the next generated timestamp (µs).
 * - `newClockId` overrides the random clock identifier (0–31, default 0).
 * - `generatedCount` is reset to 0.
 *
 * **Only use this in tests.**
 *
 * @example
 * seedTidClock(1_000_000_000_000_000, 0);
 * const tid1 = generateNextTID();
 * seedTidClock(1_000_000_000_000_000, 0);
 * const tid2 = generateNextTID();
 * // tid1 === tid2
 */
export function seedTidClock(startUs = 0, newClockId = 0): void {
  lastUs = startUs;
  clockId = newClockId % 32;
  generatedCount = 0;
}
