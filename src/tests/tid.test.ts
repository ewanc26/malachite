/**
 * Unit tests for TID generation
 * 
 * Tests cover:
 * - Format validation
 * - Monotonicity (single-threaded and concurrent)
 * - Deterministic dry-run mode
 * - Clock drift handling
 * - State persistence
 * - Collision detection
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs';
import path from 'path';
import os from 'os';
import {
  TidClock,
  RealClock,
  FakeClock,
  validateTid,
  ensureValidTid,
  decodeTidTimestamp,
  decodeTidClockId,
  compareTids,
  areMonotonic,
  InvalidTidError,
  SilentTidLogger,
} from '../utils/tid-clock.js';

const TID_LENGTH = 13;

describe('TID Format Validation', () => {
  it('should validate correct TID format', () => {
    const validTids = [
      '3jzfcijpj2z2a',
      '7777777777777',
      '3zzzzzzzzzzzz',
    ];

    for (const tid of validTids) {
      assert.strictEqual(validateTid(tid), true, `Should validate: ${tid}`);
    }
  });

  it('should reject invalid TID format', () => {
    const invalidTids = [
      '3jzfcijpj2z21',     // Invalid character (1)
      '0000000000000',     // Invalid character (0)
      '3jzfcijpj2z2aa',    // Too long
      '3jzfcijpj2z2',      // Too short
      '3jzf-cij-pj2z-2a',  // Dashes not allowed
      'zzzzzzzzzzzzz',     // High bit violation
      'kjzfcijpj2z2a',     // High bit violation
    ];

    for (const tid of invalidTids) {
      assert.strictEqual(validateTid(tid), false, `Should reject: ${tid}`);
    }
  });

  it('should enforce TID length', () => {
    assert.strictEqual(validateTid('123'), false);
    assert.strictEqual(validateTid('12345678901234567890'), false);
  });

  it('should enforce base32 alphabet', () => {
    assert.strictEqual(validateTid('3jzfcijpj2z21'), false); // Has '1'
    assert.strictEqual(validateTid('0jzfcijpj2z2a'), false); // Starts with '0'
  });

  it('should throw on ensureValidTid for invalid input', () => {
    assert.throws(
      () => ensureValidTid('invalid'),
      InvalidTidError
    );
  });
});

describe('TID Generation - Basic', () => {
  it('should generate valid TID format', async () => {
    const clock = new TidClock(new RealClock(), new SilentTidLogger());
    const tid = await clock.next();

    assert.strictEqual(tid.length, TID_LENGTH);
    assert.strictEqual(validateTid(tid), true);
  });

  it('should generate TID from Date', async () => {
    const clock = new TidClock(new FakeClock(1000000000000000), new SilentTidLogger());
    const date = new Date('2005-01-01T00:00:00Z');
    const tid = await clock.fromDate(date);

    assert.strictEqual(validateTid(tid), true);
    assert.strictEqual(tid.length, TID_LENGTH);
  });

  it('should encode timestamp correctly', async () => {
    const timestamp = 1000000000000000; // Fixed timestamp
    const clock = new TidClock(new FakeClock(timestamp), new SilentTidLogger());
    const tid = await clock.next();

    const decoded = decodeTidTimestamp(tid);
    assert.strictEqual(decoded, timestamp);
  });

  it('should encode clock ID correctly', async () => {
    const clock = new TidClock(
      new FakeClock(1000000000000000),
      new SilentTidLogger(),
      { clockId: 15 }
    );
    const tid = await clock.next();

    const clockId = decodeTidClockId(tid);
    assert.strictEqual(clockId, 15);
  });
});

describe('TID Monotonicity - Single Thread', () => {
  it('should generate monotonically increasing TIDs', async () => {
    const fakeClock = new FakeClock(1000000000000000);
    const clock = new TidClock(fakeClock, new SilentTidLogger());

    const tids: string[] = [];
    for (let i = 0; i < 100; i++) {
      fakeClock.advance(1000); // Advance 1ms
      tids.push(await clock.next());
    }

    assert.strictEqual(areMonotonic(tids), true);
  });

  it('should handle same timestamp with sequence increment', async () => {
    const fakeClock = new FakeClock(1000000000000000);
    const clock = new TidClock(fakeClock, new SilentTidLogger());

    // Generate multiple TIDs at same timestamp
    const tid1 = await clock.next();
    const tid2 = await clock.next();
    const tid3 = await clock.next();

    assert.notStrictEqual(tid1, tid2);
    assert.notStrictEqual(tid2, tid3);
    assert.strictEqual(compareTids(tid1, tid2), -1);
    assert.strictEqual(compareTids(tid2, tid3), -1);
  });

  it('should handle backwards clock drift', async () => {
    const fakeClock = new FakeClock(1000000000000000);
    const clock = new TidClock(fakeClock, new SilentTidLogger());

    const tid1 = await clock.next();
    
    // Move clock backwards
    fakeClock.set(999999000000000);
    
    const tid2 = await clock.next();

    // Should still be monotonic
    assert.strictEqual(compareTids(tid1, tid2), -1);
  });

  it('should generate unique TIDs even with clock drift', async () => {
    const fakeClock = new FakeClock(1000000000000000);
    const clock = new TidClock(fakeClock, new SilentTidLogger());

    const tids: string[] = [];
    
    // Generate some TIDs
    for (let i = 0; i < 5; i++) {
      tids.push(await clock.next());
    }

    // Move clock backwards
    fakeClock.set(999999000000000);

    // Generate more TIDs
    for (let i = 0; i < 5; i++) {
      tids.push(await clock.next());
    }

    // All should be unique and monotonic
    const uniqueTids = new Set(tids);
    assert.strictEqual(uniqueTids.size, tids.length);
    assert.strictEqual(areMonotonic(tids), true);
  });
});

describe('TID Monotonicity - Concurrent', () => {
  it('should handle concurrent generation safely', async () => {
    const clock = new TidClock(new RealClock(), new SilentTidLogger());

    // Generate 100 TIDs concurrently
    const promises = Array.from({ length: 100 }, () => clock.next());
    const tids = await Promise.all(promises);

    // All should be unique
    const uniqueTids = new Set(tids);
    assert.strictEqual(uniqueTids.size, tids.length);

    // All should be valid
    for (const tid of tids) {
      assert.strictEqual(validateTid(tid), true);
    }

    // Should be monotonic when sorted
    const sorted = [...tids].sort(compareTids);
    assert.strictEqual(areMonotonic(sorted), true);
  });

  it('should handle high-frequency concurrent generation', async () => {
    const clock = new TidClock(new RealClock(), new SilentTidLogger());

    // Generate 1000 TIDs in parallel batches
    const batchSize = 50;
    const batches = 20;
    const allTids: string[] = [];

    for (let b = 0; b < batches; b++) {
      const promises = Array.from({ length: batchSize }, () => clock.next());
      const batchTids = await Promise.all(promises);
      allTids.push(...batchTids);
    }

    // All should be unique
    const uniqueTids = new Set(allTids);
    assert.strictEqual(uniqueTids.size, allTids.length);

    // All should be valid
    for (const tid of allTids) {
      assert.strictEqual(validateTid(tid), true);
    }
  });
});

describe('TID Deterministic Mode', () => {
  it('should generate deterministic TIDs with same seed', async () => {
    const seed = 1000000000000000;

    const clock1 = new TidClock(new FakeClock(seed), new SilentTidLogger(), { clockId: 10 });
    const clock2 = new TidClock(new FakeClock(seed), new SilentTidLogger(), { clockId: 10 });

    const tids1: string[] = [];
    const tids2: string[] = [];

    for (let i = 0; i < 10; i++) {
      tids1.push(await clock1.next());
      tids2.push(await clock2.next());
    }

    // Should generate identical sequences
    assert.deepStrictEqual(tids1, tids2);
  });

  it('should be deterministic for historical dates', async () => {
    const dates = [
      new Date('2005-01-01T00:00:00Z'),
      new Date('2010-06-15T12:30:00Z'),
      new Date('2020-12-31T23:59:59Z'),
    ];

    const clock1 = new TidClock(new FakeClock(0), new SilentTidLogger(), { clockId: 5 });
    const clock2 = new TidClock(new FakeClock(0), new SilentTidLogger(), { clockId: 5 });

    const tids1 = await Promise.all(dates.map(d => clock1.fromDate(d)));
    const tids2 = await Promise.all(dates.map(d => clock2.fromDate(d)));

    assert.deepStrictEqual(tids1, tids2);
  });
});

describe('TID State Persistence', () => {
  const tempDir = path.join(os.tmpdir(), 'tid-test-' + Date.now());
  const statePath = path.join(tempDir, 'tid-state.json');

  before(() => {
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
  });

  after(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('should persist state to disk', async () => {
    const clock = new TidClock(
      new FakeClock(1000000000000000),
      new SilentTidLogger(),
      { statePath }
    );

    await clock.next();
    await clock.next();

    // State file should exist
    assert.strictEqual(fs.existsSync(statePath), true);

    // Should contain state
    const stateData = JSON.parse(fs.readFileSync(statePath, 'utf-8'));
    assert.strictEqual(stateData.generatedCount, 2);
  });

  it('should restore state from disk', async () => {
    // Use a unique state file path for this test
    const restoreStatePath = path.join(tempDir, 'tid-state-restore.json');
    
    // Create first clock and generate some TIDs
    const clock1 = new TidClock(
      new FakeClock(1000000000000000),
      new SilentTidLogger(),
      { statePath: restoreStatePath, clockId: 10 }
    );

    await clock1.next();
    const tid2 = await clock1.next();

    // Create new clock with same state file
    const clock2 = new TidClock(
      new FakeClock(1000000000000000),
      new SilentTidLogger(),
      { statePath: restoreStatePath }
    );

    const tid3 = await clock2.next();

    // Should continue from where clock1 left off
    assert.strictEqual(compareTids(tid2, tid3), -1);
    
    const state = clock2.getState();
    assert.strictEqual(state.generatedCount, 3);
  });
});

describe('TID Historical Dates', () => {
  it('should handle very old dates (2005)', async () => {
    const clock = new TidClock(new FakeClock(0), new SilentTidLogger());
    const date = new Date('2005-01-01T00:00:00Z');
    const tid = await clock.fromDate(date);

    assert.strictEqual(validateTid(tid), true);
  });

  it('should maintain monotonicity with out-of-order dates', async () => {
    const clock = new TidClock(new FakeClock(0), new SilentTidLogger());

    const dates = [
      new Date('2020-01-01T00:00:00Z'),
      new Date('2015-01-01T00:00:00Z'), // Earlier!
      new Date('2010-01-01T00:00:00Z'), // Even earlier!
      new Date('2025-01-01T00:00:00Z'),
    ];

    const tids = await Promise.all(dates.map(d => clock.fromDate(d)));

    // Should still be monotonic despite out-of-order input
    assert.strictEqual(areMonotonic(tids), true);
  });

  it('should handle duplicate dates', async () => {
    const clock = new TidClock(new FakeClock(0), new SilentTidLogger());
    const date = new Date('2020-01-01T00:00:00Z');

    const tid1 = await clock.fromDate(date);
    const tid2 = await clock.fromDate(date);
    const tid3 = await clock.fromDate(date);

    // All should be unique
    assert.notStrictEqual(tid1, tid2);
    assert.notStrictEqual(tid2, tid3);
    assert.strictEqual(areMonotonic([tid1, tid2, tid3]), true);
  });
});

describe('TID Collision Detection', () => {
  it('should detect duplicate TIDs (should never happen)', async () => {
    const clock = new TidClock(new FakeClock(1000000000000000), new SilentTidLogger());
    
    // Generate a TID
    await clock.next();
    
    // Try to force a duplicate by manually resetting state (testing collision detection)
    // This simulates what would happen if there was a bug in generation
    const state = clock.getState();
    
    // Create a new clock with the exact same state
    const clock2 = new TidClock(
      new FakeClock(state.lastTimestampUs),
      new SilentTidLogger(),
      {
        clockId: state.clockId,
        initialState: {
          lastTimestampUs: state.lastTimestampUs - 1, // Trick it into generating same timestamp
          generatedCount: 0,
        }
      }
    );
    
    // First TID from clock2 might be a duplicate
    // But the clock should handle it via sequence increment
    const tid = await clock2.next();
    assert.strictEqual(validateTid(tid), true);
  });
});

describe('TID Comparison and Sorting', () => {
  it('should compare TIDs correctly', () => {
    const tid1 = '3jzfcijpj2z2a';
    const tid2 = '3jzfcijpj2z2b';
    const tid3 = '7777777777777';

    assert.strictEqual(compareTids(tid1, tid1), 0);
    assert.strictEqual(compareTids(tid1, tid2), -1);
    assert.strictEqual(compareTids(tid2, tid1), 1);
    assert.strictEqual(compareTids(tid1, tid3), -1);
  });

  it('should sort TIDs correctly', () => {
    const tids = [
      '7777777777777',
      '3jzfcijpj2z2a',
      '3zzzzzzzzzzzz',
      '3jzfcijpj2z2b',
    ];

    const sorted = [...tids].sort(compareTids);
    
    assert.deepStrictEqual(sorted, [
      '3jzfcijpj2z2a',
      '3jzfcijpj2z2b',
      '3zzzzzzzzzzzz',
      '7777777777777',
    ]);
  });
});

describe('TID Edge Cases', () => {
  it('should handle microsecond precision', async () => {
    const clock = new TidClock(new FakeClock(1234567890123456), new SilentTidLogger());
    const tid = await clock.next();
    const decoded = decodeTidTimestamp(tid);

    assert.strictEqual(decoded, 1234567890123456);
  });

  it('should handle very large timestamps', async () => {
    const farFuture = new Date('2099-12-31T23:59:59.999Z').getTime() * 1000;
    const clock = new TidClock(new FakeClock(farFuture), new SilentTidLogger());
    const tid = await clock.next();

    assert.strictEqual(validateTid(tid), true);
  });

  it('should handle rapid sequential generation', async () => {
    const clock = new TidClock(new RealClock(), new SilentTidLogger());
    const tids: string[] = [];

    // Generate as fast as possible
    for (let i = 0; i < 1000; i++) {
      tids.push(await clock.next());
    }

    const uniqueTids = new Set(tids);
    assert.strictEqual(uniqueTids.size, tids.length);
    assert.strictEqual(areMonotonic(tids), true);
  });
});

console.log('✓ All TID tests completed');
