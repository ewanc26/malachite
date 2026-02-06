/**
 * Unit tests for Spotify parsing and record conversion
 * 
 * Tests cover:
 * - JSON parsing (single file and directories)
 * - Record filtering
 * - Spotify record conversion
 * - Track metadata extraction
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs';
import path from 'path';
import os from 'os';
import {
  parseSpotifyJson,
  convertSpotifyToPlayRecord,
} from '../lib/spotify.js';
import type { Config } from '../types.js';

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

// Helper function to create a valid Spotify record
function createSpotifyRecord(overrides: any = {}) {
  return {
    ts: '2021-06-15T20:00:00Z',
    platform: 'web',
    ms_played: 180000,
    conn_country: 'US',
    master_metadata_track_name: 'Track',
    master_metadata_album_artist_name: 'Artist',
    master_metadata_album_album_name: null,
    spotify_track_uri: null,
    episode_name: null,
    episode_show_name: null,
    spotify_episode_uri: null,
    reason_start: 'uriopen' as const,
    reason_end: 'endplay' as const,
    shuffle: false,
    skipped: false,
    offline: false,
    offline_timestamp: null,
    incognito_mode: false,
    ...overrides,
  };
}

describe('Spotify JSON Parsing', () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spotify-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('should parse single Spotify JSON file', () => {
    const spotifyData = [
      createSpotifyRecord({
        master_metadata_track_name: 'Track One',
        master_metadata_album_artist_name: 'Artist One',
        master_metadata_album_album_name: 'Album One',
        spotify_track_uri: 'spotify:track:123',
      }),
    ];

    const filePath = path.join(tempDir, 'Streaming_History_Audio_0.json');
    fs.writeFileSync(filePath, JSON.stringify(spotifyData));

    const records = parseSpotifyJson(filePath);
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].master_metadata_track_name, 'Track One');
    assert.strictEqual(
      records[0].master_metadata_album_artist_name,
      'Artist One'
    );
  });

  it('should parse directory with multiple Spotify JSON files', () => {
    const spotifyData1 = [
      createSpotifyRecord({
        master_metadata_track_name: 'Track One',
        master_metadata_album_artist_name: 'Artist One',
        master_metadata_album_album_name: 'Album One',
        spotify_track_uri: 'spotify:track:123',
      }),
    ];

    const spotifyData2 = [
      createSpotifyRecord({
        ts: '2021-06-14T20:00:00Z',
        platform: 'mobile',
        ms_played: 200000,
        master_metadata_track_name: 'Track Two',
        master_metadata_album_artist_name: 'Artist Two',
        master_metadata_album_album_name: 'Album Two',
        spotify_track_uri: 'spotify:track:456',
      }),
    ];

    fs.writeFileSync(
      path.join(tempDir, 'Streaming_History_Audio_0.json'),
      JSON.stringify(spotifyData1)
    );
    fs.writeFileSync(
      path.join(tempDir, 'Streaming_History_Audio_1.json'),
      JSON.stringify(spotifyData2)
    );

    const records = parseSpotifyJson(tempDir);
    assert.strictEqual(records.length, 2);
    assert.strictEqual(records[0].master_metadata_track_name, 'Track One');
    assert.strictEqual(records[1].master_metadata_track_name, 'Track Two');
  });

  it('should filter out non-music records (podcasts, audiobooks)', () => {
    const spotifyData = [
      createSpotifyRecord({
        master_metadata_track_name: 'Music Track',
        master_metadata_album_artist_name: 'Artist',
        master_metadata_album_album_name: 'Album',
        spotify_track_uri: 'spotify:track:123',
      }),
      createSpotifyRecord({
        ts: '2021-06-15T19:00:00Z',
        master_metadata_track_name: null,
        master_metadata_album_artist_name: null,
        episode_name: 'Podcast Episode',
        episode_show_name: 'Podcast Show',
        spotify_episode_uri: 'spotify:episode:123',
      }),
    ];

    const filePath = path.join(tempDir, 'Streaming_History_Audio_0.json');
    fs.writeFileSync(filePath, JSON.stringify(spotifyData));

    const records = parseSpotifyJson(filePath);
    assert.strictEqual(
      records.length,
      1,
      'Should filter out podcast/audiobook records'
    );
    assert.strictEqual(records[0].master_metadata_track_name, 'Music Track');
  });

  it('should handle empty files', () => {
    const filePath = path.join(tempDir, 'Streaming_History_Audio_0.json');
    fs.writeFileSync(filePath, JSON.stringify([]));

    const records = parseSpotifyJson(filePath);
    assert.strictEqual(records.length, 0);
  });

  it('should ignore non-streaming history files', () => {
    const filePath1 = path.join(tempDir, 'Streaming_History_Audio_0.json');
    const filePath2 = path.join(tempDir, 'other_file.json');

    fs.writeFileSync(
      filePath1,
      JSON.stringify([
        createSpotifyRecord({
          master_metadata_track_name: 'Track',
          master_metadata_album_artist_name: 'Artist',
          spotify_track_uri: 'spotify:track:123',
        }),
      ])
    );
    fs.writeFileSync(filePath2, JSON.stringify([]));

    const records = parseSpotifyJson(tempDir);
    assert.strictEqual(records.length, 1);
  });
});

describe('Spotify Record Conversion', () => {
  it('should convert Spotify record to PlayRecord', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: 'Test Track',
      master_metadata_album_artist_name: 'Test Artist',
      master_metadata_album_album_name: 'Test Album',
      spotify_track_uri: 'spotify:track:7qiZfU4dY1lsylvNFutmtK',
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Test Track');
    assert.strictEqual(playRecord.artists[0].artistName, 'Test Artist');
    assert.strictEqual(playRecord.releaseName, 'Test Album');
    assert.strictEqual(playRecord.playedTime, '2021-06-15T20:00:00Z');
    assert.strictEqual(playRecord.musicServiceBaseDomain, 'spotify.com');
    assert.match(playRecord.originUrl, /spotify.com/);
    assert.match(playRecord.originUrl, /7qiZfU4dY1lsylvNFutmtK/);
  });

  it('should generate Spotify origin URL from track URI', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: 'Track',
      master_metadata_album_artist_name: 'Artist',
      spotify_track_uri: 'spotify:track:abc123xyz',
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.strictEqual(
      playRecord.originUrl,
      'https://open.spotify.com/track/abc123xyz'
    );
  });

  it('should handle records without track URI', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: 'Track',
      master_metadata_album_artist_name: 'Artist',
      spotify_track_uri: null,
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Track');
    assert.strictEqual(playRecord.originUrl, '');
  });

  it('should handle missing album name', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: 'Track',
      master_metadata_album_artist_name: 'Artist',
      master_metadata_album_album_name: null,
      spotify_track_uri: 'spotify:track:123',
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Track');
    assert.ok(!playRecord.releaseName);
  });

  it('should handle null track name', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: null,
      master_metadata_album_artist_name: 'Artist',
      spotify_track_uri: 'spotify:track:123',
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.strictEqual(playRecord.trackName, 'Unknown Track');
  });

  it('should set correct record type', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: 'Track',
      master_metadata_album_artist_name: 'Artist',
      spotify_track_uri: 'spotify:track:123',
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.strictEqual(playRecord.$type, mockConfig.RECORD_TYPE);
  });

  it('should set submission client agent', () => {
    const spotifyRecord = createSpotifyRecord({
      master_metadata_track_name: 'Track',
      master_metadata_album_artist_name: 'Artist',
      spotify_track_uri: 'spotify:track:123',
    });

    const playRecord = convertSpotifyToPlayRecord(spotifyRecord, mockConfig);

    assert.ok(playRecord.submissionClientAgent);
    assert.match(playRecord.submissionClientAgent, /malachite/i);
  });
});
