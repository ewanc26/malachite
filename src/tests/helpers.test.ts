/**
 * Unit tests for helper utilities
 * 
 * Tests cover:
 * - Date formatting and localization
 * - String normalization
 * - Duration formatting
 * - Batch size calculation
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  formatDate,
  formatDateRange,
  formatDuration,
  sortRecords,
  normalizeString,
  calculateOptimalBatchSize,
} from '../utils/helpers.js';
import type { PlayRecord, Config } from '../types.js';

describe('Date Formatting', () => {
  it('should format date without time', () => {
    const date = new Date('2025-06-15T10:30:00Z');
    const formatted = formatDate(date, false);
    // Should contain month abbreviation, day, and year
    assert.ok(formatted.includes('Jun') || formatted.includes('2025'));
    assert.ok(formatted.includes('15'));
  });

  it('should format date with time', () => {
    const date = new Date('2025-06-15T10:30:00Z');
    const formatted = formatDate(date, true);
    // Should contain time information
    assert.ok(formatted.includes(':'));
    assert.ok(formatted.includes('15'));
  });

  it('should handle string date input', () => {
    const formatted = formatDate('2025-06-15T10:30:00Z', false);
    assert.ok(formatted);
    assert.ok(formatted.includes('15'));
  });

  it('should format date ranges', () => {
    const start = new Date('2025-06-01T00:00:00Z');
    const end = new Date('2025-06-30T23:59:59Z');
    const range = formatDateRange(start, end);
    assert.match(range, /to/);
    assert.ok(range.length > 0);
  });
});

describe('String Normalization', () => {
  it('should normalize strings to lowercase', () => {
    const result = normalizeString('Hello World');
    assert.strictEqual(result, 'hello world');
  });

  it('should remove punctuation', () => {
    const result = normalizeString("it's a test!");
    assert.strictEqual(result, 'its a test');
  });

  it('should normalize whitespace', () => {
    const result = normalizeString('hello    world');
    assert.strictEqual(result, 'hello world');
  });

  it('should handle mixed case and punctuation', () => {
    const result = normalizeString("Böhse-Blöffen's Test!!!");
    assert.ok(!result.includes('!'));
    assert.strictEqual(result, result.toLowerCase());
  });

  it('should trim whitespace', () => {
    const result = normalizeString('  hello world  ');
    assert.strictEqual(result, 'hello world');
  });
});

describe('Duration Formatting', () => {
  it('should format seconds', () => {
    const result = formatDuration(5000);
    assert.match(result, /\d+s/);
  });

  it('should format minutes and seconds', () => {
    const result = formatDuration(90000); // 1m 30s
    assert.match(result, /\d+m\s\d+s/);
  });

  it('should format hours, minutes and seconds', () => {
    const result = formatDuration(3665000); // 1h 1m 5s
    assert.match(result, /\d+h\s\d+m/);
  });

  it('should handle zero duration', () => {
    const result = formatDuration(0);
    assert.strictEqual(result, '0s');
  });

  it('should handle large durations', () => {
    const result = formatDuration(86400000); // 24 hours
    assert.match(result, /\d+h/);
  });
});

describe('Record Sorting', () => {
  it('should sort records oldest first by default', () => {
    const records: PlayRecord[] = [
      {
        $type: 'test',
        trackName: 'Track 1',
        artists: [{ artistName: 'Artist 1' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'Track 2',
        artists: [{ artistName: 'Artist 2' }],
        playedTime: '2025-06-10T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
    ];

    const sorted = sortRecords([...records], false);
    assert.strictEqual(
      sorted[0].playedTime,
      '2025-06-10T10:00:00Z',
      'Should sort oldest first'
    );
    assert.strictEqual(
      sorted[1].playedTime,
      '2025-06-15T10:00:00Z',
      'Should sort newest last'
    );
  });

  it('should sort records newest first when reverse is true', () => {
    const records: PlayRecord[] = [
      {
        $type: 'test',
        trackName: 'Track 1',
        artists: [{ artistName: 'Artist 1' }],
        playedTime: '2025-06-10T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'Track 2',
        artists: [{ artistName: 'Artist 2' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
    ];

    const sorted = sortRecords([...records], true);
    assert.strictEqual(
      sorted[0].playedTime,
      '2025-06-15T10:00:00Z',
      'Should sort newest first'
    );
    assert.strictEqual(
      sorted[1].playedTime,
      '2025-06-10T10:00:00Z',
      'Should sort oldest last'
    );
  });

  it('should handle empty record array', () => {
    const sorted = sortRecords([], false);
    assert.deepStrictEqual(sorted, []);
  });

  it('should handle single record', () => {
    const record: PlayRecord = {
      $type: 'test',
      trackName: 'Track 1',
      artists: [{ artistName: 'Artist 1' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const sorted = sortRecords([record], false);
    assert.strictEqual(sorted.length, 1);
    assert.strictEqual(sorted[0].trackName, 'Track 1');
  });
});

describe('Batch Size Calculation', () => {
  const mockConfig: Config = {
    RECORD_TYPE: 'test',
    MIN_RECORDS_FOR_SCALING: 20,
    BASE_BATCH_SIZE: 200,
    MAX_BATCH_SIZE: 200,
    SCALING_FACTOR: 1.5,
    DEFAULT_BATCH_SIZE: 100,
    DEFAULT_BATCH_DELAY: 2000,
    MIN_BATCH_DELAY: 1000,
    RECORDS_PER_DAY_LIMIT: 10000,
    SAFETY_MARGIN: 0.75,
    AGGRESSIVE_SAFETY_MARGIN: 0.85,
    SLINGSHOT_RESOLVER: 'https://slingshot.microcosm.blue',
  };

  it('should scale up for larger datasets', () => {
    const size = calculateOptimalBatchSize(1000, 2000, mockConfig);
    assert.ok(size > 3);
  });

  it('should respect maximum batch size', () => {
    const size = calculateOptimalBatchSize(1000000, 2000, mockConfig);
    assert.ok(size <= mockConfig.MAX_BATCH_SIZE);
  });

  it('should ensure minimum batch size', () => {
    const size = calculateOptimalBatchSize(1, 2000, mockConfig);
    assert.ok(size >= 3);
  });

  it('should adjust for low batch delay', () => {
    const lowDelay = calculateOptimalBatchSize(500, 1000, mockConfig);
    // Low delay might not affect much, but should be valid
    assert.ok(lowDelay >= 3);
  });
});
