/**
 * Browser-compatible publisher.
 * Mirrors src/lib/publisher.ts without Node.js deps.
 * Uses in-memory rate limiting and progress callbacks instead of console.log.
 */

import type { Agent } from '@atproto/api';
import type { PlayRecord } from '../types.js';
import { RECORD_TYPE, MAX_PDS_BATCH_SIZE, POINTS_PER_RECORD } from '../config.js';
import { BrowserRateLimiter } from './rate-limiter.js';
import { generateTIDFromISO } from './tid.js';
import { normalizeHeaders, isRateLimitError } from './rate-limit-headers.js';

export interface PublishProgress {
  batchIndex: number;
  totalBatches: number;
  recordsProcessed: number;
  totalRecords: number;
  successCount: number;
  errorCount: number;
  currentBatchSize: number;
  message: string;
}

export interface PublisherCallbacks {
  onProgress: (p: PublishProgress) => void;
  onLog: (level: 'info' | 'success' | 'warn' | 'error' | 'progress', msg: string) => void;
  isCancelled: () => boolean;
}

/** Sleep for up to `ms` milliseconds, waking early if `isCancelled()` becomes true. */
function cancellableSleep(ms: number, isCancelled: () => boolean): Promise<void> {
  return new Promise((resolve) => {
    const end = Date.now() + ms;
    const tick = () => {
      if (isCancelled() || Date.now() >= end) { resolve(); return; }
      setTimeout(tick, Math.min(50, end - Date.now()));
    };
    tick();
  });
}

function normalizeResponseHeaders(response: any): Record<string, string> {
  const headers: Record<string, string> = {};
  if (response?.headers) {
    if (typeof response.headers.forEach === 'function') {
      response.headers.forEach((v: string, k: string) => { headers[k] = v; });
    } else {
      Object.assign(headers, response.headers);
    }
  }
  return headers;
}

export async function publishRecords(
  agent: Agent,
  records: PlayRecord[],
  dryRun: boolean,
  callbacks: PublisherCallbacks
): Promise<{ successCount: number; errorCount: number; cancelled: boolean }> {
  const { onProgress, onLog, isCancelled } = callbacks;

  // Abort controller so we can cut off any in-flight fetch the instant Stop is pressed.
  const ac = new AbortController();
  const cancelPoll = setInterval(() => { if (isCancelled()) ac.abort(); }, 50);
  const total = records.length;

  if (dryRun) {
    onLog('info', `[DRY RUN] Would publish ${total} records`);
    const preview = records.slice(0, 5);
    preview.forEach((r, i) => {
      onLog('info', `  ${i + 1}. ${r.artists[0]?.artistName} – ${r.trackName} (${r.playedTime.slice(0, 10)})`);
    });
    if (total > 5) onLog('info', `  …and ${total - 5} more`);
    return { successCount: total, errorCount: 0, cancelled: false };
  }

  const rl = new BrowserRateLimiter({ headroom: 0.15 });
  let currentBatchSize = 50; // probe batch
  let currentDelay = 500;
  let successCount = 0;
  let errorCount = 0;
  let batchCounter = 0;
  let i = 0;
  const startTime = Date.now();

  onLog('info', `Publishing ${total.toLocaleString()} records to ATProto…`);
  onLog('warn', 'Do not close this tab while publishing.');

  while (i < total) {
    if (isCancelled()) {
      onLog('warn', 'Import cancelled by user.');
      return { successCount, errorCount, cancelled: true };
    }

    const batch = records.slice(i, Math.min(i + currentBatchSize, total));
    batchCounter++;
    const pct = ((i / total) * 100).toFixed(1);

    onProgress({
      batchIndex: batchCounter,
      totalBatches: Math.ceil(total / currentBatchSize),
      recordsProcessed: i,
      totalRecords: total,
      successCount,
      errorCount,
      currentBatchSize: batch.length,
      message: `[${pct}%] Batch ${batchCounter} — records ${i + 1}–${Math.min(i + batch.length, total)}`
    });

    const writes = await Promise.all(
      batch.map(async (record) => ({
        $type: 'com.atproto.repo.applyWrites#create',
        collection: RECORD_TYPE,
        rkey: await generateTIDFromISO(record.playedTime, 'web:import'),
        value: record
      }))
    );

    const batchPoints = batch.length * POINTS_PER_RECORD;
    await rl.waitForPermit(batchPoints, isCancelled);
    if (isCancelled()) {
      clearInterval(cancelPoll);
      onLog('warn', 'Import cancelled by user.');
      return { successCount, errorCount, cancelled: true };
    }

    try {
      const response = await agent.com.atproto.repo.applyWrites(
        { repo: agent.did ?? '', writes: writes as any },
        { signal: ac.signal }
      );

      successCount += response.data.results?.length ?? batch.length;

      // Learn rate limits from response headers
      const rawHeaders = normalizeResponseHeaders(response);
      if (Object.keys(rawHeaders).length > 0) {
        const norm = normalizeHeaders(rawHeaders);
        rl.updateFromHeaders(norm);

        // After first response, optimise batch size
        if (batchCounter === 1) {
          const cap = rl.getServerCapacity();
          if (cap) {
            const remaining = rl.getActualRemaining();
            const pointsPerSec = cap.limit / cap.windowSeconds;
            const recsPerSec = pointsPerSec / POINTS_PER_RECORD * 0.8;
            currentBatchSize = Math.min(
              MAX_PDS_BATCH_SIZE,
              Math.max(10, Math.floor(recsPerSec * 45))
            );
            currentDelay = Math.max(500, Math.floor((currentBatchSize / recsPerSec) * 1000));
            onLog('info', `📊 Server: ${cap.limit} pts/${cap.windowSeconds}s — optimised to ${currentBatchSize} records/batch`);
            onLog('info', `   Remaining quota: ${remaining.toLocaleString()}/${cap.limit.toLocaleString()}`);
          }
        }
      }

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const rps = (successCount / ((Date.now() - startTime) / 1000)).toFixed(1);
      onLog('progress', `✓ Batch ${batchCounter} — ${successCount}/${total} records (${rps} rec/s, ${elapsed}s)`);

      i += batch.length;
    } catch (err: any) {
      if (ac.signal.aborted || isCancelled()) {
        clearInterval(cancelPoll);
        onLog('warn', 'Import cancelled by user.');
        return { successCount, errorCount, cancelled: true };
      }
      if (isRateLimitError(err)) {
        onLog('warn', '⚠️  Rate limit hit — waiting for quota reset…');
        // Extract headers from error if present
        const errHeaders = err?.response?.headers ?? err?.headers ?? {};
        if (Object.keys(errHeaders).length > 0) {
          rl.updateFromHeaders(normalizeHeaders(errHeaders));
        }
        await rl.waitForPermit(batchPoints);
        continue; // retry same batch
      }

      // Non-retryable error
      errorCount += batch.length;
      onLog('error', `✗ Batch ${batchCounter} failed: ${err.message ?? err}`);
      i += batch.length;
    }

    if (i < total) {
      await cancellableSleep(currentDelay, isCancelled);
    }
  }

  clearInterval(cancelPoll);
  return { successCount, errorCount, cancelled: false };
}
