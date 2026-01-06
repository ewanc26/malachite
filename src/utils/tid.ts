/**
 * TID (Timestamp Identifier) generation for ATProto
 * Based on: https://atproto.com/specs/record-key#record-key-type-tid
 */

const B32_CHARS = '234567abcdefghijklmnopqrstuvwxyz';

/**
 * Generate a TID from a Date object
 * TID encodes Unix microseconds in base32
 */
export function generateTID(date: Date): string {
  // Convert to Unix microseconds
  // JS only gives us millisecond precision, so we multiply by 1000
  const unixMicros = Math.floor(date.getTime() * 1000);
  
  let tid = '';
  for (let i = 0; i < 13; i++) {
    // Extract 5 bits at a time (base32)
    const shift = 60 - (i * 5);
    const index = Math.floor(unixMicros / Math.pow(2, shift)) % 32;
    tid += B32_CHARS[index];
  }
  
  return tid;
}

/**
 * Generate a TID from an ISO 8601 timestamp string
 */
export function generateTIDFromISO(isoString: string): string {
  return generateTID(new Date(isoString));
}
