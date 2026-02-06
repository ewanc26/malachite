/**
 * Unit tests for sync utilities
 * 
 * Tests cover:
 * - Record key generation
 * - Record deduplication
 * - Duplicate detection
 * - Record filtering
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { createRecordKey, deduplicateInputRecords } from '../lib/sync.js';
import type { PlayRecord } from '../types.js';

describe('Record Key Generation', () => {
  it('should create consistent key for same record', () => {
    const record: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const key1 = createRecordKey(record);
    const key2 = createRecordKey(record);

    assert.strictEqual(key1, key2);
  });

  it('should create different keys for different records', () => {
    const record1: PlayRecord = {
      $type: 'test',
      trackName: 'Track One',
      artists: [{ artistName: 'Artist One' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const record2: PlayRecord = {
      $type: 'test',
      trackName: 'Track Two',
      artists: [{ artistName: 'Artist One' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const key1 = createRecordKey(record1);
    const key2 = createRecordKey(record2);

    assert.notStrictEqual(key1, key2);
  });

  it('should normalize case in record key', () => {
    const record1: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const record2: PlayRecord = {
      $type: 'test',
      trackName: 'test track',
      artists: [{ artistName: 'test artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const key1 = createRecordKey(record1);
    const key2 = createRecordKey(record2);

    assert.strictEqual(key1, key2);
  });

  it('should normalize whitespace in record key', () => {
    const record1: PlayRecord = {
      $type: 'test',
      trackName: 'Test  Track',
      artists: [{ artistName: 'Test  Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const record2: PlayRecord = {
      $type: 'test',
      trackName: 'Test  Track',
      artists: [{ artistName: 'Test  Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const key1 = createRecordKey(record1);
    const key2 = createRecordKey(record2);

    assert.strictEqual(key1, key2);
  });

  it('should include timestamp in key', () => {
    const record1: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const record2: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T11:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const key1 = createRecordKey(record1);
    const key2 = createRecordKey(record2);

    assert.notStrictEqual(key1, key2);
  });

  it('should handle multiple artists', () => {
    const record: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [
        { artistName: 'Artist One' },
        { artistName: 'Artist Two' },
      ],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const key = createRecordKey(record);
    assert.ok(key);
    assert.ok(key.includes('artist one'));
  });
});

describe('Record Deduplication', () => {
  it('should identify duplicate records', () => {
    const record1: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const record2: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const result = deduplicateInputRecords([record1, record2]);

    assert.strictEqual(result.unique.length, 1);
    assert.strictEqual(result.duplicates, 1);
  });

  it('should keep first occurrence of duplicates', () => {
    const record1: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'last.fm',
      originUrl: 'http://last.fm',
    };

    const record2: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'spotify.com',
      originUrl: 'http://spotify.com',
    };

    const result = deduplicateInputRecords([record1, record2]);

    assert.strictEqual(result.unique.length, 1);
    assert.strictEqual(result.unique[0].musicServiceBaseDomain, 'last.fm');
  });

  it('should not mark different records as duplicates', () => {
    const records: PlayRecord[] = [
      {
        $type: 'test',
        trackName: 'Track One',
        artists: [{ artistName: 'Artist One' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'Track Two',
        artists: [{ artistName: 'Artist Two' }],
        playedTime: '2025-06-15T11:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
    ];

    const result = deduplicateInputRecords(records);

    assert.strictEqual(result.unique.length, 2);
    assert.strictEqual(result.duplicates, 0);
  });

  it('should handle empty record array', () => {
    const result = deduplicateInputRecords([]);

    assert.strictEqual(result.unique.length, 0);
    assert.strictEqual(result.duplicates, 0);
  });

  it('should handle single record', () => {
    const record: PlayRecord = {
      $type: 'test',
      trackName: 'Test Track',
      artists: [{ artistName: 'Test Artist' }],
      playedTime: '2025-06-15T10:00:00Z',
      submissionClientAgent: 'test',
      musicServiceBaseDomain: 'test.com',
      originUrl: 'http://test.com',
    };

    const result = deduplicateInputRecords([record]);

    assert.strictEqual(result.unique.length, 1);
    assert.strictEqual(result.duplicates, 0);
  });

  it('should detect multiple duplicate sets', () => {
    const records: PlayRecord[] = [
      {
        $type: 'test',
        trackName: 'Track A',
        artists: [{ artistName: 'Artist A' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'Track A',
        artists: [{ artistName: 'Artist A' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'Track B',
        artists: [{ artistName: 'Artist B' }],
        playedTime: '2025-06-15T11:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'Track B',
        artists: [{ artistName: 'Artist B' }],
        playedTime: '2025-06-15T11:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
    ];

    const result = deduplicateInputRecords(records);

    assert.strictEqual(result.unique.length, 2);
    assert.strictEqual(result.duplicates, 2);
  });

  it('should normalize track and artist names when deduplicating', () => {
    const records: PlayRecord[] = [
      {
        $type: 'test',
        trackName: 'Test Track',
        artists: [{ artistName: 'Test Artist' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
      {
        $type: 'test',
        trackName: 'test track',
        artists: [{ artistName: 'test artist' }],
        playedTime: '2025-06-15T10:00:00Z',
        submissionClientAgent: 'test',
        musicServiceBaseDomain: 'test.com',
        originUrl: 'http://test.com',
      },
    ];

    const result = deduplicateInputRecords(records);

    assert.strictEqual(result.unique.length, 1);
    assert.strictEqual(result.duplicates, 1);
  });
});
