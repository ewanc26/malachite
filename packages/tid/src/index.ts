/**
 * @malachite/tid — AT Protocol TID generation
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
  const timestampUs = s32decode(tid.slice(0, 11));
  const clockId     = s32decode(tid.slice(11));
  return { timestampUs, clockId, date: new Date(Math.floor(timestampUs / 1000)) };
}

// ─── Monotonic clock ─────────────────────────────────────────────────────────

// Module-level state — one clock per JS context (process or browser tab).
let lastUs = 0;
const CLOCK_ID = (() => {
  const buf = new Uint8Array(1);
  (globalThis.crypto ?? (globalThis as unknown as { webcrypto: Crypto }).webcrypto)
    .getRandomValues(buf);
  return buf[0] % 32;
})();

function nextUs(targetUs: number): number {
  const us = targetUs <= lastUs ? lastUs + 1 : targetUs;
  lastUs = us;
  return us;
}

function makeTid(us: number): string {
  return s32encode(us).padStart(11, '2') + s32encode(CLOCK_ID).padStart(2, '2');
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

// ─── Testing utilities ───────────────────────────────────────────────────────

/**
 * Reset the module-level monotonic clock to zero.
 *
 * **Only use this in tests.** Calling it in production risks generating
 * duplicate or non-monotonic TIDs.
 */
export function resetTidClock(): void {
  lastUs = 0;
}
