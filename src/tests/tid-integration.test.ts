/**
 * Integration test for TID generation in the importer
 * 
 * Tests the full flow with actual CSV data
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { initTidClock, generateTIDFromISO, resetTidClock, getTidClockState } from '../utils/tid.js';
import { validateTid, areMonotonic } from '../utils/tid-clock.js';
import { auditTids, formatAuditReport } from '../utils/tid-audit.js';

describe('TID Integration - Importer Flow', () => {
  it('should initialize TID clock for production', async () => {
    resetTidClock();
    initTidClock({ mode: 'production' });

    const tid = await generateTIDFromISO('2020-01-01T00:00:00Z', 'test');
    assert.strictEqual(validateTid(tid), true);

    const state = getTidClockState();
    assert.strictEqual(state.generatedCount, 1);
  });

  it('should initialize TID clock for dry-run with deterministic output', async () => {
    const seed = 1000000000000000;
    resetTidClock();
    initTidClock({ mode: 'dry-run', seed });

    const dates = [
      '2005-01-01T00:00:00Z',
      '2010-01-01T00:00:00Z',
      '2015-01-01T00:00:00Z',
    ];

    const tids1 = await Promise.all(dates.map(d => generateTIDFromISO(d, 'test')));

    // Reset and regenerate
    resetTidClock();
    initTidClock({ mode: 'dry-run', seed });

    const tids2 = await Promise.all(dates.map(d => generateTIDFromISO(d, 'test')));

    // Should be identical
    assert.deepStrictEqual(tids1, tids2);
  });

  it('should handle batch TID generation', async () => {
    resetTidClock();
    initTidClock({ mode: 'production' });

    // Simulate batch processing
    const batchSize = 200;
    const records = Array.from({ length: batchSize }, (_, i) => ({
      playedTime: new Date(2020, 0, 1, 0, 0, i).toISOString(),
    }));

    const tids = await Promise.all(
      records.map(r => generateTIDFromISO(r.playedTime, 'batch:test'))
    );

    // All should be valid
    for (const tid of tids) {
      assert.strictEqual(validateTid(tid), true);
    }

    // All should be unique
    const uniqueTids = new Set(tids);
    assert.strictEqual(uniqueTids.size, batchSize);

    // Should be monotonic
    assert.strictEqual(areMonotonic(tids), true);

    // State should track count
    const state = getTidClockState();
    assert.strictEqual(state.generatedCount, batchSize);
  });

  it('should handle historical dates from Last.fm (2005-present)', async () => {
    resetTidClock();
    initTidClock({ mode: 'production' });

    // Simulate Last.fm scrobbles from different eras
    const historicalDates = [
      '2005-03-15T14:30:00Z',  // Very old
      '2010-06-20T08:15:00Z',
      '2015-11-05T19:45:00Z',
      '2020-01-01T00:00:00Z',
      '2024-12-25T12:00:00Z',  // Recent
    ];

    const tids = await Promise.all(
      historicalDates.map(d => generateTIDFromISO(d, 'historical'))
    );

    // All valid
    for (const tid of tids) {
      assert.strictEqual(validateTid(tid), true);
    }

    // All unique
    const uniqueTids = new Set(tids);
    assert.strictEqual(uniqueTids.size, historicalDates.length);

    // Monotonic despite being historical
    assert.strictEqual(areMonotonic(tids), true);
  });

  it('should audit a batch of TIDs', async () => {
    resetTidClock();
    initTidClock({ mode: 'production' });

    const tids = await Promise.all(
      Array.from({ length: 100 }, (_, i) =>
        generateTIDFromISO(new Date(2020, 0, 1, 0, 0, i).toISOString(), 'audit-test')
      )
    );

    const report = auditTids(tids);

    assert.strictEqual(report.totalTids, 100);
    assert.strictEqual(report.validTids, 100);
    assert.strictEqual(report.invalidTids, 0);
    assert.strictEqual(report.duplicates, 0);
    assert.strictEqual(report.monotonic, true);

    // Generate human-readable report
    const textReport = formatAuditReport(report);
    assert.ok(textReport.includes('TID AUDIT REPORT'));
    assert.ok(textReport.includes('✓ YES')); // Monotonic check
  });

  it('should detect problems in TID audit', async () => {
    // Create a bad TID list
    const badTids = [
      '3jzfcijpj2z2a',  // Valid
      '3jzfcijpj2z2b',  // Valid
      '3jzfcijpj2z2a',  // Duplicate!
      '0000000000000',  // Invalid format
      '3jzfcijpj2z2c',  // Valid
      '3jzfcijpj2z2b',  // Out of order
    ];

    const report = auditTids(badTids);

    assert.strictEqual(report.totalTids, 6);
    assert.strictEqual(report.duplicates, 2);
    assert.strictEqual(report.monotonic, false);
    assert.ok(report.invalidTids > 0);
    assert.ok(report.errors.length > 0);

    const textReport = formatAuditReport(report);
    assert.ok(textReport.includes('✗ NO')); // Monotonic check
    assert.ok(textReport.includes('ERRORS'));
  });
});

describe('TID Integration - Concurrent Batches', () => {
  it('should handle multiple concurrent batches safely', async () => {
    resetTidClock();
    initTidClock({ mode: 'production' });

    // Simulate 5 concurrent batches of 50 records each
    const batches = 5;
    const batchSize = 50;

    const allTids = await Promise.all(
      Array.from({ length: batches }, async (_, batchIdx) => {
        return await Promise.all(
          Array.from({ length: batchSize }, (_, recordIdx) => {
            const date = new Date(2020, 0, 1, batchIdx, recordIdx).toISOString();
            return generateTIDFromISO(date, `batch-${batchIdx}`);
          })
        );
      })
    );

    const flatTids = allTids.flat();

    // All unique
    const uniqueTids = new Set(flatTids);
    assert.strictEqual(uniqueTids.size, batches * batchSize);

    // All valid
    for (const tid of flatTids) {
      assert.strictEqual(validateTid(tid), true);
    }

    // Total count correct
    const state = getTidClockState();
    assert.strictEqual(state.generatedCount, batches * batchSize);
  });
});

console.log('✓ All integration tests completed');
