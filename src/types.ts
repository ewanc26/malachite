import { AtpAgent as Agent } from '@atproto/api';

/**
 * Type alias for the ATProto Agent, used for clarity in the project.
 */
export type AtpAgent = Agent;

export interface LastFmCsvRecord {
  uts: string;
  utc_time: string;
  artist: string;
  artist_mbid?: string;
  album: string;
  album_mbid?: string;
  track: string;
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
  // Help
  help?: boolean;
  
  // Authentication
  handle?: string;
  password?: string;
  
  // Input
  input?: string;
  'spotify-input'?: string;
  
  // Mode
  mode?: string;
  
  // Batch configuration
  'batch-size'?: string;
  'batch-delay'?: string;
  
  // Import options
  reverse?: boolean;
  yes?: boolean;
  'dry-run'?: boolean;
  aggressive?: boolean;
  fresh?: boolean;
  'clear-cache'?: boolean;
  'clear-all-caches'?: boolean;
  'clear-credentials'?: boolean;
  
  // Output
  verbose?: boolean;
  quiet?: boolean;
  dev?: boolean;
  // Optional PDS override URL to skip identity resolution
  pds?: string;
  
  // Legacy flags (for backwards compatibility)
  file?: string;
  'spotify-file'?: string;
  identifier?: string;
  'reverse-chronological'?: boolean;
  sync?: boolean;
  spotify?: boolean;
  combined?: boolean;
  'remove-duplicates'?: boolean;
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

  DEFAULT_BATCH_SIZE: number;   // from rate limiter
  MIN_BATCH_DELAY: number;      // from rate limiter
  RECORDS_PER_DAY_LIMIT: number;
  SAFETY_MARGIN: number;
  AGGRESSIVE_SAFETY_MARGIN: number;

  SLINGSHOT_RESOLVER: string;

  RECORD_TYPE: string;
}
