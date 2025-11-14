import { AtpAgent as Agent } from '@atproto/api';

/**
 * Type alias for the ATProto Agent, used for clarity in the project.
 */
export type AtpAgent = Agent;

export interface LastFmCsvRecord {
  artist: string;
  track: string;
  album: string;
  uts: string;
  artist_mbid?: string;
  album_mbid?: string;
  track_mbid?: string;
}

export interface PlayRecordArtist {
  artistName: string;
  artistMbId?: string;
}

export interface PlayRecord {
  $type: string;
  trackName: string;
  artists: PlayRecordArtist[];
  playedTime: string;
  submissionClientAgent: string;
  musicServiceBaseDomain: string;
  releaseName?: string;
  releaseMbId?: string;
  recordingMbId?: string;
  originUrl: string;
}

export interface CommandLineArgs {
  help?: boolean;
  file?: string;
  identifier?: string;
  password?: string;
  'batch-size'?: string;
  'batch-delay'?: string;
  yes?: boolean;
  'dry-run'?: boolean;
  'reverse-chronological'?: boolean;
}

export interface PublishResult {
  successCount: number;
  errorCount: number;
  cancelled: boolean;
}

export interface Config {
  MIN_RECORDS_FOR_SCALING: number;
  BASE_BATCH_SIZE: number;
  MAX_BATCH_SIZE: number;
  SCALING_FACTOR: number;
  DEFAULT_BATCH_DELAY: number;

  CLIENT_AGENT: string;

  DEFAULT_BATCH_SIZE: number;   // from rate limiter
  MIN_BATCH_DELAY: number;      // from rate limiter
  RECORDS_PER_DAY_LIMIT: number;
  SAFETY_MARGIN: number;

  SLINGSHOT_RESOLVER: string;

  RECORD_TYPE: string;
}