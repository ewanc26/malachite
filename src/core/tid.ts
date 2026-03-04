/**
 * TID (Timestamp Identifier) generation — environment-agnostic.
 *
 * Browser-safe: uses crypto.getRandomValues / globalThis.crypto.
 * In Node.js 20+ the Web Crypto API is available via globalThis.crypto as well.
 *
 * Spec: https://atproto.com/specs/tid
 */

// Base-32 alphabet used by AT Protocol (not standard base32)
const S32_CHARS = '234567abcdefghijklmnopqrstuvwxyz';

function s32encode(n: number): string {
  if (n === 0) return '2';
  let s = '';
  let val = n;
  while (val > 0) {
    s = S32_CHARS[val % 32] + s;
    val = Math.floor(val / 32);
  }
  return s;
}

// In-memory monotonic state (reset on each process/page load)
let lastTimestampUs = 0;
const clockId = (() => {
  const buf = new Uint8Array(1);
  (globalThis.crypto ?? (globalThis as any).webcrypto).getRandomValues(buf);
  return buf[0] % 32;
})();

/**
 * Generate a TID from an ISO 8601 timestamp.
 * Guarantees monotonicity — timestamps that arrive out of order are bumped
 * forward so every call produces a strictly increasing TID.
 */
export async function generateTIDFromISO(isoString: string, _context?: string): Promise<string> {
  let timestamp = new Date(isoString).getTime() * 1000; // ms → µs
  if (timestamp <= lastTimestampUs) timestamp = lastTimestampUs + 1;
  lastTimestampUs = timestamp;
  return s32encode(timestamp).padStart(11, '2') + s32encode(clockId).padStart(2, '2');
}

export function resetTidClock(): void {
  lastTimestampUs = 0;
}
