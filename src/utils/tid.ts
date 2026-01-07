/**
 * TID (Timestamp Identifier) generation for ATProto
 * Based on: https://atproto.com/specs/record-key#record-key-type-tid
 * 
 * This uses the official ATProto TID implementation from @atproto/common-web
 * to ensure compatibility and avoid precision issues with large numbers.
 */

import { TID } from '@atproto/common-web';

/**
 * Generate a TID from a Date object
 * Uses the official ATProto TID.fromTime method
 */
export function generateTID(date: Date): string {
  // Convert to Unix microseconds
  // The ATProto implementation expects microseconds
  const unixMicros = date.getTime() * 1000;
  
  // Use a fixed clockid of 0 for deterministic TID generation from timestamps
  // This ensures the same playedTime always generates the same TID
  const clockid = 0;
  
  return TID.fromTime(unixMicros, clockid).toString();
}

/**
 * Generate a TID from an ISO 8601 timestamp string
 */
export function generateTIDFromISO(isoString: string): string {
  return generateTID(new Date(isoString));
}
