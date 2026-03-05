/**
 * Integration tests for TID generation in the importer flow.
 * Backed by @ewanc26/tid via src/utils/tid.ts.
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert';
import {
  generateTIDFromISO,
  resetTidClock,
  seedTidClock,
  getTidClockState,
} from '../utils/tid.js';
import { validateTid, areMonotonic } from '@ewanc26/tid';
import { auditTids, formatAuditReport } from '../utils/tid-audit.js';

beforeEach(() => resetTidClock());

describe('TID Integration - Importer Flow', () => {
  it('should generate a valid TID from an ISO string', () => {
    const tid = generateTIDFromISO('2020-01-01T00:00:00Z', 'test');
    assert.strictEqual(validateTid(tid), true);

    const state = getTidClockState();
    assert.strictEqual(state.generatedCount, 1);
  });

  it('should be deterministic with seedTidClock', () => {
    const seed = 1_000_000_000_000_000;
    const dates = [
      '2005-01-01T00:00:00Z',
      '2010-01-01T00:00:00Z',
      '2015-01-01T00:00:00Z',
    ];

    seedTidClock(seed, 0);
    const tids1 = dates.map(d => generateTIDFromISO(d, 'test'));

    seedTidClock(seed, 0);
    const tids2 = dates.map(d => generateTIDFromISO(d, 'test'));

    assert.deepStrictEqual(tids1, tids2);
  });

  it('should handle batch TID generation', () => {
    const batchSize = 200;
    const records = Array.from({ length: batchSize }, (_, i) => ({
      playedTime: new Date(2020, 0, 1, 0, 0, i).toISOString(),
    }));

    const tids = records.map(r => generateTIDFromISO(r.playedTime, 'batch:test'));

    for (const tid of tids) assert.strictEqual(validateTid(tid), true);
    assert.strictEqual(new Set(tids).size, batchSize);
    assert.strictEqual(areMonotonic(tids), true);

    const state = getTidClockState();
    assert.strictEqual(state.generatedCount, batchSize);
  });

  it('should handle historical dates from Last.fm (2005–present)', () => {
    const historicalDates = [
      '2005-03-15T14:30:00Z',
      '2010-06-20T08:15:00Z',
      '2015-11-05T19:45:00Z',
      '2020-01-01T00:00:00Z',
      '2024-12-25T12:00:00Z',
    ];

    const tids = historicalDates.map(d => generateTIDFromISO(d, 'historical'));

    for (const tid of tids) assert.strictEqual(validateTid(tid), true);
    assert.strictEqual(new Set(tids).size, historicalDates.length);
    assert.strictEqual(areMonotonic(tids), true);
  });

  it('should audit a batch of TIDs', () => {
    const tids = Array.from({ length: 100 }, (_, i) =>
      generateTIDFromISO(new Date(2020, 0, 1, 0, 0, i).toISOString(), 'audit-test')
    );

    const report = auditTids(tids);

    assert.strictEqual(report.totalTids, 100);
    assert.strictEqual(report.validTids, 100);
    assert.strictEqual(report.invalidTids, 0);
    assert.strictEqual(report.duplicates, 0);
    assert.strictEqual(report.monotonic, true);

    const textReport = formatAuditReport(report);
    assert.ok(textReport.includes('TID AUDIT REPORT'));
    assert.ok(textReport.includes('✓ YES'));
  });

  it('should detect problems in TID audit', () => {
    const badTids = [
      '3jzfcijpj2z2a', // Valid
      '3jzfcijpj2z2b', // Valid
      '3jzfcijpj2z2a', // Duplicate
      '0000000000000', // Invalid format
      '3jzfcijpj2z2c', // Valid
      '3jzfcijpj2z2b', // Out of order
    ];

    const report = auditTids(badTids);

    assert.strictEqual(report.totalTids, 6);
    assert.strictEqual(report.duplicates, 2);
    assert.strictEqual(report.monotonic, false);
    assert.ok(report.invalidTids > 0);
    assert.ok(report.errors.length > 0);

    const textReport = formatAuditReport(report);
    assert.ok(textReport.includes('✗ NO'));
    assert.ok(textReport.includes('ERRORS'));
  });
});

describe('TID Integration - Concurrent Batches', () => {
  it('should produce unique TIDs across concurrent-style batch generation', () => {
    const batches = 5;
    const batchSize = 50;

    const allTids: string[] = [];
    for (let b = 0; b < batches; b++) {
      for (let r = 0; r < batchSize; r++) {
        allTids.push(generateTIDFromISO(new Date(2020, 0, 1, b, r).toISOString(), `batch-${b}`));
      }
    }

    assert.strictEqual(new Set(allTids).size, batches * batchSize);
    for (const tid of allTids) assert.strictEqual(validateTid(tid), true);

    const state = getTidClockState();
    assert.strictEqual(state.generatedCount, batches * batchSize);
  });
});

console.log('✓ All integration tests completed');
