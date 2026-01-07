/**
 * TID (Timestamp Identifier) generation for ATProto
 * Based on: https://atproto.com/specs/tid
 * 
 * Per the spec: "If the local clock has only millisecond precision, the timestamp 
 * should be padded." Our implementation pads to ensure 11 characters for the timestamp
 * portion and 2 characters for the clockid, for a total of exactly 13 characters.
 * 
 * This implementation uses the official ATProto s32encode function and properly handles
 * historical dates (like 2005 scrobbles) by padding to the required length.
 */

import { s32encode } from '@atproto/common-web/dist/util.js';

/**
 * Generate a TID from a Date object
 * 
 * TID format (13 characters total):
 * - Characters 0-10: timestamp in microseconds, base32-encoded
 * - Characters 11-12: clock ID, base32-encoded
 * 
 * The timestamp is padded with '2' (representing 0 in base32) to ensure exactly 11 characters.
 * The clockid is similarly padded to ensure exactly 2 characters.
 * 
 * @param date - The date to generate a TID from
 * @returns A valid 13-character TID string
 */
export function generateTID(date: Date): string {
  // Convert to Unix microseconds (JS Date.getTime() returns milliseconds)
  // Per spec: multiply by 1000 to pad millisecond precision
  const unixMicros = date.getTime() * 1000;
  
  // Use a fixed clockid of 0 for deterministic TID generation from timestamps
  // This ensures the same playedTime always generates the same TID (important for deduplication)
  const clockid = 0;
  
  // Encode timestamp and clockid with proper padding
  // Timestamp should be 11 characters, clockid should be 2 characters
  // Padding character is '2' which represents 0 in base32
  const timestampStr = s32encode(unixMicros).padStart(11, '2');
  const clockidStr = s32encode(clockid).padStart(2, '2');
  
  return timestampStr + clockidStr;
}

/**
 * Generate a TID from an ISO 8601 timestamp string
 * 
 * @param isoString - ISO 8601 formatted datetime string
 * @returns A valid 13-character TID string
 */
export function generateTIDFromISO(isoString: string): string {
  return generateTID(new Date(isoString));
}
