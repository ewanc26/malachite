/**
 * Unit tests for TID generation — backed by @ewanc26/tid.
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert';
import {
  generateTID,
  generateNextTID,
  validateTid,
  ensureValidTid,
  decodeTid,
  decodeTidTimestamp,
  decodeTidClockId,
  compareTids,
  areMonotonic,
  InvalidTidError,
  resetTidClock,
  seedTidClock,
} from '@ewanc26/tid';

const TID_LENGTH = 13;

beforeEach(() => resetTidClock());

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
      '3jzfcijpj2z2aa',   // Too long
      '3jzfcijpj2z2',     // Too short
      '3jzf-cij-pj2z-2a', // Dashes not allowed
      'zzzzzzzzzzzzz',    // High bit violation
      'kjzfcijpj2z2a',    // High bit violation
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
    assert.strictEqual(validateTid('3jzfcijpj2z21'), false);
    assert.strictEqual(validateTid('0jzfcijpj2z2a'), false);
  });

  it('should throw on ensureValidTid for invalid input', () => {
    assert.throws(() => ensureValidTid('invalid'), InvalidTidError);
  });
});

describe('TID Generation - Basic', () => {
  it('should generate valid TID format', () => {
    const tid = generateNextTID();
    assert.strictEqual(tid.length, TID_LENGTH);
    assert.strictEqual(validateTid(tid), true);
  });

  it('should generate TID from ISO string', () => {
    const tid = generateTID('2005-01-01T00:00:00Z');
    assert.strictEqual(validateTid(tid), true);
    assert.strictEqual(tid.length, TID_LENGTH);
  });

  it('should generate TID from Date', () => {
    const tid = generateTID(new Date('2005-01-01T00:00:00Z'));
    assert.strictEqual(validateTid(tid), true);
    assert.strictEqual(tid.length, TID_LENGTH);
  });

  it('should encode timestamp correctly', () => {
    const seed = 1_000_000_000_000_000;
    seedTidClock(seed, 0);
    const tid = generateNextTID();
    assert.strictEqual(decodeTidTimestamp(tid), seed);
  });

  it('should encode clock ID correctly', () => {
    seedTidClock(1_000_000_000_000_000, 15);
    const tid = generateNextTID();
    assert.strictEqual(decodeTidClockId(tid), 15);
  });

  it('should decode timestamp and date correctly', () => {
    const source = new Date('2020-06-15T12:00:00Z');
    const tid = generateTID(source);
    const { timestampUs, date } = decodeTid(tid);
    assert.strictEqual(Math.floor(timestampUs / 1000), source.getTime());
    assert.strictEqual(date.getTime(), source.getTime());
  });

  it('should decode clock ID in valid range', () => {
    const { clockId } = decodeTid(generateNextTID());
    assert.ok(clockId >= 0 && clockId <= 31, `clockId ${clockId} out of range`);
  });
});

describe('TID Monotonicity - Single Thread', () => {
  it('should generate monotonically increasing TIDs from advancing timestamps', () => {
    const tids: string[] = [];
    for (let i = 0; i < 100; i++) {
      tids.push(generateTID(new Date(1_000_000_000_000 + i)));
    }
    assert.strictEqual(areMonotonic(tids), true);
  });

  it('should handle same timestamp via sequence increment', () => {
    const now = new Date('2020-01-01T00:00:00Z');
    const tid1 = generateTID(now);
    const tid2 = generateTID(now);
    const tid3 = generateTID(now);
    assert.notStrictEqual(tid1, tid2);
    assert.notStrictEqual(tid2, tid3);
    assert.strictEqual(compareTids(tid1, tid2), -1);
    assert.strictEqual(compareTids(tid2, tid3), -1);
  });

  it('should handle backwards clock drift', () => {
    const tid1 = generateTID(new Date('2020-01-01T00:00:00Z'));
    const tid2 = generateTID(new Date('2015-01-01T00:00:00Z')); // earlier
    assert.strictEqual(compareTids(tid1, tid2), -1);
  });

  it('should generate unique TIDs even with clock drift', () => {
    const tids: string[] = [];
    for (let i = 0; i < 5; i++) tids.push(generateTID(new Date('2020-01-01T00:00:00Z')));
    for (let i = 0; i < 5; i++) tids.push(generateTID(new Date('2015-01-01T00:00:00Z')));
    assert.strictEqual(new Set(tids).size, tids.length);
    assert.strictEqual(areMonotonic(tids), true);
  });
});

describe('TID Monotonicity - Concurrent', () => {
  it('should produce unique and valid TIDs across rapid sequential generation', () => {
    const tids = Array.from({ length: 100 }, () => generateNextTID());
    assert.strictEqual(new Set(tids).size, tids.length);
    for (const tid of tids) assert.strictEqual(validateTid(tid), true);
    const sorted = [...tids].sort(compareTids);
    assert.strictEqual(areMonotonic(sorted), true);
  });

  it('should handle high-frequency generation', () => {
    const allTids: string[] = [];
    for (let b = 0; b < 20; b++) {
      for (let r = 0; r < 50; r++) allTids.push(generateNextTID());
    }
    assert.strictEqual(new Set(allTids).size, allTids.length);
    for (const tid of allTids) assert.strictEqual(validateTid(tid), true);
  });
});

describe('TID Deterministic Mode', () => {
  it('should generate deterministic TIDs with same seed', () => {
    const seed = 1_000_000_000_000_000;
    seedTidClock(seed, 10);
    const tids1 = Array.from({ length: 10 }, () => generateNextTID());
    seedTidClock(seed, 10);
    const tids2 = Array.from({ length: 10 }, () => generateNextTID());
    assert.deepStrictEqual(tids1, tids2);
  });

  it('should be deterministic for historical dates', () => {
    const dates = [
      new Date('2005-01-01T00:00:00Z'),
      new Date('2010-06-15T12:30:00Z'),
      new Date('2020-12-31T23:59:59Z'),
    ];
    seedTidClock(0, 5);
    const tids1 = dates.map(d => generateTID(d));
    seedTidClock(0, 5);
    const tids2 = dates.map(d => generateTID(d));
    assert.deepStrictEqual(tids1, tids2);
  });
});

describe('TID Historical Dates', () => {
  it('should handle very old dates (2005)', () => {
    assert.strictEqual(validateTid(generateTID(new Date('2005-01-01T00:00:00Z'))), true);
  });

  it('should maintain monotonicity with out-of-order dates', () => {
    const tids = [
      new Date('2020-01-01T00:00:00Z'),
      new Date('2015-01-01T00:00:00Z'),
      new Date('2010-01-01T00:00:00Z'),
      new Date('2025-01-01T00:00:00Z'),
    ].map(d => generateTID(d));
    assert.strictEqual(areMonotonic(tids), true);
  });

  it('should handle duplicate dates', () => {
    const date = new Date('2020-01-01T00:00:00Z');
    const tid1 = generateTID(date);
    const tid2 = generateTID(date);
    const tid3 = generateTID(date);
    assert.notStrictEqual(tid1, tid2);
    assert.notStrictEqual(tid2, tid3);
    assert.strictEqual(areMonotonic([tid1, tid2, tid3]), true);
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
    const tids = ['7777777777777', '3jzfcijpj2z2a', '3zzzzzzzzzzzz', '3jzfcijpj2z2b'];
    assert.deepStrictEqual([...tids].sort(compareTids), [
      '3jzfcijpj2z2a',
      '3jzfcijpj2z2b',
      '3zzzzzzzzzzzz',
      '7777777777777',
    ]);
  });
});

describe('TID Edge Cases', () => {
  it('should handle microsecond precision', () => {
    const seed = 1_234_567_890_123_456;
    seedTidClock(seed, 0);
    const tid = generateNextTID();
    assert.strictEqual(decodeTidTimestamp(tid), seed);
  });

  it('should handle far-future dates (2099)', () => {
    assert.strictEqual(validateTid(generateTID('2099-12-31T23:59:59.999Z')), true);
  });

  it('should handle rapid sequential generation', () => {
    const tids = Array.from({ length: 1000 }, () => generateNextTID());
    assert.strictEqual(new Set(tids).size, tids.length);
    assert.strictEqual(areMonotonic(tids), true);
  });
});

console.log('✓ All TID tests completed');
