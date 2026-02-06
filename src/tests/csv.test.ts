/**
 * Unit tests for CSV parsing and Last.fm record conversion
 * 
 * Tests cover:
 * - CSV delimiter detection
 * - Column normalization
 * - Record parsing and validation
 * - Timestamp handling
 * - Last.fm record conversion
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs';
import path from 'path';
import os from 'os';
import {
  parseLastFmCsv,
  convertToPlayRecord,
} from '../lib/csv.js';
import type { Config, LastFmCsvRecord } from '../types.js';

const mockConfig: Config = {
  RECORD_TYPE: 'fm.teal.alpha.feed.play',
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

describe('CSV Parsing', () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'csv-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('should parse valid CSV with comma delimiter', () => {
    const csvContent = `uts,utc_time,artist,album,track
1623801600,2021-06-15T20:00:00Z,Test Artist,Test Album,Test Track`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].artist, 'Test Artist');
    assert.strictEqual(records[0].track, 'Test Track');
    assert.strictEqual(records[0].album, 'Test Album');
  });

  it('should detect semicolon delimiter', () => {
    const csvContent = `uts;utc_time;artist;album;track
1623801600;2021-06-15T20:00:00Z;Artist A;Album A;Track A`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].artist, 'Artist A');
  });

  it('should handle tab delimiter', () => {
    const csvContent = `uts\tutc_time\tartist\talbum\ttrack
1623801600\t2021-06-15T20:00:00Z\tArtist B\tAlbum B\tTrack B`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].artist, 'Artist B');
  });

  it('should remove BOM if present', () => {
    const csvContent = `uts,utc_time,artist,album,track
1623801600,2021-06-15T20:00:00Z,Artist C,Album C,Track C`;

    const filePath = path.join(tempDir, 'test.csv');
    // Write with BOM
    fs.writeFileSync(filePath, '\ufeff' + csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].artist, 'Artist C');
  });

  it('should convert millisecond timestamps to seconds', () => {
    const csvContent = `uts,utc_time,artist,album,track
1623801600000,2021-06-15T20:00:00Z,Artist D,Album D,Track D`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 1);
    assert.ok(records[0].uts);
    // Should be converted from milliseconds to seconds
    const timestamp = parseInt(records[0].uts);
    assert.ok(timestamp < 2000000000); // Should be in seconds, not milliseconds
  });

  it('should validate required fields', () => {
    const csvContent = `uts,utc_time,artist,album,track
1623801600,2021-06-15T20:00:00Z,Artist E,,Track E
,2021-06-15T20:00:00Z,,Album F,Track F`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    // Should only have records with all required fields
    assert.strictEqual(
      records.length,
      1,
      'Should filter out records with missing required fields'
    );
  });

  it('should handle alternative column names', () => {
    const csvContent = `timestamp,artist_name,song,album_name
1623801600,Artist G,Track G,Album G`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].artist, 'Artist G');
    assert.strictEqual(records[0].track, 'Track G');
  });

  it('should handle multiple records', () => {
    const csvContent = `uts,utc_time,artist,album,track
1623801600,2021-06-15T20:00:00Z,Artist H,Album H,Track H
1623801700,2021-06-15T20:01:40Z,Artist I,Album I,Track I
1623801800,2021-06-15T20:03:20Z,Artist J,Album J,Track J`;

    const filePath = path.join(tempDir, 'test.csv');
    fs.writeFileSync(filePath, csvContent);

    const records = parseLastFmCsv(filePath);
    assert.strictEqual(records.length, 3);
    assert.strictEqual(records[0].artist, 'Artist H');
    assert.strictEqual(records[1].artist, 'Artist I');
    assert.strictEqual(records[2].artist, 'Artist J');
  });
});

describe('Last.fm Record Conversion', () => {
  it('should convert CSV record to PlayRecord', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'Test Artist',
      artist_mbid: 'artist-id-123',
      album: 'Test Album',
      album_mbid: 'album-id-456',
      track: 'Test Track',
      track_mbid: 'track-id-789',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Test Track');
    assert.strictEqual(playRecord.artists[0].artistName, 'Test Artist');
    assert.strictEqual(playRecord.artists[0].artistMbId, 'artist-id-123');
    assert.strictEqual(playRecord.releaseName, 'Test Album');
    assert.strictEqual(playRecord.releaseMbId, 'album-id-456');
    assert.strictEqual(playRecord.recordingMbId, 'track-id-789');
    assert.strictEqual(playRecord.musicServiceBaseDomain, 'last.fm');
    assert.match(playRecord.originUrl, /last\.fm/);
  });

  it('should handle records without optional MBIDs', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'Test Artist',
      album: 'Test Album',
      track: 'Test Track',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Test Track');
    assert.strictEqual(playRecord.artists[0].artistName, 'Test Artist');
    assert.ok(!playRecord.artists[0].artistMbId);
    assert.strictEqual(playRecord.releaseName, 'Test Album');
    assert.ok(!playRecord.releaseMbId);
    assert.ok(!playRecord.recordingMbId);
  });

  it('should generate Last.fm origin URL', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'The Beatles',
      album: 'Abbey Road',
      track: 'Come Together',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);

    assert.match(playRecord.originUrl, /https:\/\/www\.last\.fm/);
    assert.match(playRecord.originUrl, /The%20Beatles/);
    assert.match(playRecord.originUrl, /Come%20Together/);
  });

  it('should set correct record type', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'Artist',
      album: 'Album',
      track: 'Track',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);

    assert.strictEqual(playRecord.$type, mockConfig.RECORD_TYPE);
  });

  it('should set submission client agent', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'Artist',
      album: 'Album',
      track: 'Track',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);

    assert.ok(playRecord.submissionClientAgent);
    assert.match(playRecord.submissionClientAgent, /malachite/i);
  });

  it('should parse timestamp correctly', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'Artist',
      album: 'Album',
      track: 'Track',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);
    const playedDate = new Date(playRecord.playedTime);

    assert.strictEqual(playedDate.getTime(), 1623801600000);
  });

  it('should handle empty strings for optional fields', () => {
    const csvRecord: LastFmCsvRecord = {
      uts: '1623801600',
      utc_time: '2021-06-15T20:00:00Z',
      artist: 'Artist',
      artist_mbid: '',
      album: '   ',
      album_mbid: '',
      track: 'Track',
      track_mbid: '  ',
    };

    const playRecord = convertToPlayRecord(csvRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Track');
    assert.ok(!playRecord.releaseName || playRecord.releaseName.trim() === '');
    assert.ok(!playRecord.releaseMbId);
  });
});
