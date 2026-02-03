import type { PlayRecord } from '../types.js';

/**
 * Batch sizing configuration constants
 */
export const TARGET_LATENCY_MS = 2000; // Target 2s per request
export const MIN_BATCH_SIZE = 10;
export const MAX_BATCH_SIZE = 100; // AT Protocol maximum

/**
 * Create a unique key for a play record based on its essential properties
 * This is used to identify duplicates
 */
export function createRecordKey(record: PlayRecord): string {
  const artist = record.artists[0]?.artistName || '';
  const track = record.trackName;
  const timestamp = record.playedTime;

  // Normalize strings to handle case and whitespace differences
  const normalizedArtist = artist.toLowerCase().trim();
  const normalizedTrack = track.toLowerCase().trim();

  return `${normalizedArtist}|||${normalizedTrack}|||${timestamp}`;
}

/**
 * Batch size optimizer for adaptive network performance
 */
export class BatchSizeOptimizer {
  private batchSize: number;
  private consecutiveFastRequests = 0;
  private consecutiveSlowRequests = 0;
  private readonly targetLatency: number;
  private readonly minBatchSize: number;
  private readonly maxBatchSize: number;

  constructor(
    initialBatchSize: number = 25,
    targetLatency: number = TARGET_LATENCY_MS,
    minBatchSize: number = MIN_BATCH_SIZE,
    maxBatchSize: number = MAX_BATCH_SIZE
  ) {
    this.batchSize = initialBatchSize;
    this.targetLatency = targetLatency;
    this.minBatchSize = minBatchSize;
    this.maxBatchSize = maxBatchSize;
  }

  /**
   * Get current batch size
   */
  getCurrentBatchSize(): number {
    return this.batchSize;
  }

  /**
   * Alias for getCurrentBatchSize() for backward compatibility
   */
  getBatchSize(): number {
    return this.getCurrentBatchSize();
  }

  /**
   * Update batch size based on request latency
   * Returns information about the adjustment
   */
  update(requestLatency: number): { changed: boolean; oldSize: number; newSize: number } {
    const oldSize = this.batchSize;

    if (requestLatency < this.targetLatency) {
      // Request was fast - try to increase batch size
      this.consecutiveFastRequests++;
      this.consecutiveSlowRequests = 0;

      if (this.consecutiveFastRequests >= 3 && this.batchSize < this.maxBatchSize) {
        this.batchSize = Math.min(this.maxBatchSize, Math.floor(this.batchSize * 1.5));
        this.consecutiveFastRequests = 0;
      }
    } else {
      // Request was slow - decrease batch size
      this.consecutiveSlowRequests++;
      this.consecutiveFastRequests = 0;

      if (this.consecutiveSlowRequests >= 2 && this.batchSize > this.minBatchSize) {
        this.batchSize = Math.max(this.minBatchSize, Math.floor(this.batchSize * 0.7));
        this.consecutiveSlowRequests = 0;
      }
    }

    return {
      changed: oldSize !== this.batchSize,
      oldSize,
      newSize: this.batchSize,
    };
  }

  /**
   * Handle request completion (alias for update for backward compatibility)
   */
  onRequest(requestLatency: number): void {
    this.update(requestLatency);
  }

  /**
   * Reset to initial state
   */
  reset(newBatchSize?: number): void {
    if (newBatchSize !== undefined) {
      this.batchSize = Math.min(Math.max(newBatchSize, this.minBatchSize), this.maxBatchSize);
    }
    this.consecutiveFastRequests = 0;
    this.consecutiveSlowRequests = 0;
  }
}
