/**
 * Browser-compatible TID (Timestamp Identifier) generation for ATProto.
 *
 * Re-implements the monotonic TID clock from src/utils/tid-clock.ts without
 * any Node.js dependencies (no fs, no crypto module — uses crypto.getRandomValues).
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

// In-memory monotonic state
let lastTimestampUs = 0;
const clockId = (() => {
  const buf = new Uint8Array(1);
  crypto.getRandomValues(buf);
  return buf[0] % 32;
})();

export async function generateTIDFromISO(isoString: string, _context?: string): Promise<string> {
  let timestamp = new Date(isoString).getTime() * 1000; // ms → µs

  // Monotonicity: never go backwards
  if (timestamp <= lastTimestampUs) {
    timestamp = lastTimestampUs + 1;
  }
  lastTimestampUs = timestamp;

  const timestampStr = s32encode(timestamp).padStart(11, '2');
  const clockIdStr = s32encode(clockId).padStart(2, '2');
  return timestampStr + clockIdStr;
}

export function resetTidClock(): void {
  lastTimestampUs = 0;
}
