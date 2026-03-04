/**
 * ATProto record publisher — environment-agnostic.
 * All progress/logging is surfaced via callbacks; no console.log or UI deps.
 * The CLI wrapper in src/lib/publisher.ts adapts this to terminal UI.
 */

import type { Agent } from '@atproto/api';
import type { PlayRecord } from './types.js';
import { RECORD_TYPE, MAX_PDS_BATCH_SIZE, POINTS_PER_RECORD } from './config.js';
import { RateLimiter } from './rate-limiter.js';
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

function extractHeaders(response: unknown): Record<string, string> {
  const headers: Record<string, string> = {};
  const r = response as any;
  if (r?.headers) {
    if (typeof r.headers.forEach === 'function') {
      r.headers.forEach((v: string, k: string) => { headers[k] = v; });
    } else {
      Object.assign(headers, r.headers);
    }
  }
  return headers;
}

/** Simple retry wrapper for transient network failures. */
async function withRetry<T>(
  fn: () => Promise<T>,
  maxAttempts = 3,
  onRetry?: (attempt: number, err: Error) => void
): Promise<T> {
  let lastErr!: Error;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err: unknown) {
      lastErr = err as Error;
      const isTransient = /fetch failed|ECONNRESET|ETIMEDOUT|ENOTFOUND|network|timeout|503|502|504/i.test(lastErr.message ?? '');
      if (!isTransient || attempt === maxAttempts) throw lastErr;
      onRetry?.(attempt, lastErr);
      await new Promise((r) => setTimeout(r, 1000 * attempt));
    }
  }
  throw lastErr;
}

export async function publishRecords(
  agent: Agent,
  records: PlayRecord[],
  dryRun: boolean,
  callbacks: PublisherCallbacks,
  context = 'publish'
): Promise<{ successCount: number; errorCount: number; cancelled: boolean }> {
  const { onProgress, onLog, isCancelled } = callbacks;
  const total = records.length;

  if (dryRun) {
    onLog('info', `[DRY RUN] Would publish ${total.toLocaleString()} records`);
    records.slice(0, 5).forEach((r, i) => {
      onLog('info', `  ${i + 1}. ${r.artists[0]?.artistName} – ${r.trackName} (${r.playedTime.slice(0, 10)})`);
    });
    if (total > 5) onLog('info', `  …and ${total - 5} more`);
    return { successCount: total, errorCount: 0, cancelled: false };
  }

  // Abort controller so in-flight fetches are cancelled immediately on stop.
  const ac = new AbortController();
  const cancelPoll = setInterval(() => { if (isCancelled()) ac.abort(); }, 50);

  const rl = new RateLimiter({ headroom: 0.15 });
  let currentBatchSize = 50; // probe batch
  let currentDelay = 500;
  let successCount = 0;
  let errorCount = 0;
  let batchCounter = 0;
  let i = 0;
  const startTime = Date.now();

  onLog('info', `Publishing ${total.toLocaleString()} records to ATProto…`);

  try {
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
        message: `[${pct}%] Batch ${batchCounter} — records ${i + 1}–${Math.min(i + batch.length, total)}`,
      });

      const writes = await Promise.all(
        batch.map(async (record) => ({
          $type: 'com.atproto.repo.applyWrites#create',
          collection: RECORD_TYPE,
          rkey: await generateTIDFromISO(record.playedTime, context),
          value: record,
        }))
      );

      const batchPoints = batch.length * POINTS_PER_RECORD;
      await rl.waitForPermit(batchPoints, isCancelled);
      if (isCancelled()) {
        onLog('warn', 'Import cancelled by user.');
        return { successCount, errorCount, cancelled: true };
      }

      try {
        const response = await withRetry(
          () => agent.com.atproto.repo.applyWrites(
            { repo: agent.did ?? '', writes: writes as any },
            { signal: ac.signal }
          ),
          3,
          (attempt, err) => onLog('warn', `⚠️  Batch ${batchCounter} failed (attempt ${attempt}/3): ${err.message} — retrying…`)
        );

        successCount += (response.data as any).results?.length ?? batch.length;

        // Learn rate limits from response headers
        const rawHeaders = extractHeaders(response);
        if (Object.keys(rawHeaders).length > 0) {
          const norm = normalizeHeaders(rawHeaders);
          rl.updateFromHeaders(norm);

          if (batchCounter === 1) {
            const cap = rl.getServerCapacity();
            if (cap) {
              const remaining = rl.getActualRemaining();
              const recsPerSec = (cap.limit / cap.windowSeconds / POINTS_PER_RECORD) * 0.8;
              currentBatchSize = Math.min(MAX_PDS_BATCH_SIZE, Math.max(10, Math.floor(recsPerSec * 45)));
              currentDelay = Math.max(500, Math.floor((currentBatchSize / recsPerSec) * 1000));
              onLog('info', `📊 Server: ${cap.limit} pts/${cap.windowSeconds}s — optimised to ${currentBatchSize} records/batch`);
              onLog('info', `   Remaining quota: ${remaining.toLocaleString()}/${cap.limit.toLocaleString()}`);
            }
          }
        }

        const rps = (successCount / ((Date.now() - startTime) / 1000)).toFixed(1);
        onLog('progress', `✓ Batch ${batchCounter} — ${successCount}/${total} records (${rps} rec/s)`);
        i += batch.length;

      } catch (err: unknown) {
        const e = err as any;
        if (ac.signal.aborted || isCancelled()) {
          return { successCount, errorCount, cancelled: true };
        }
        if (isRateLimitError(e)) {
          onLog('warn', '⚠️  Rate limit hit — waiting for quota reset…');
          const rawErrHeaders: Record<string, string> =
            typeof e?.response?.headers?.forEach === 'function'
              ? (() => { const o: Record<string, string> = {}; e.response.headers.forEach((v: string, k: string) => { o[k] = v; }); return o; })()
              : (e?.response?.headers ?? e?.headers ?? {});
          rl.handleRateLimitHit(normalizeHeaders(rawErrHeaders));
          await rl.waitForPermit(batchPoints, isCancelled);
          continue; // retry same batch
        }
        errorCount += batch.length;
        onLog('error', `✗ Batch ${batchCounter} failed: ${e?.message ?? e}`);
        i += batch.length;
      }

      if (i < total) {
        await cancellableSleep(currentDelay, isCancelled);
      }
    }
  } finally {
    clearInterval(cancelPoll);
  }

  return { successCount, errorCount, cancelled: false };
}
