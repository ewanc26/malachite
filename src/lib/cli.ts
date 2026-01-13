#!/usr/bin/env node

import { parseArgs } from 'node:util';
import { AtpAgent } from '@atproto/api';
import type { PlayRecord, Config, CommandLineArgs, PublishResult } from '../types.js';
import { login } from './auth.js';
import { parseLastFmCsv, convertToPlayRecord, sortRecords } from '../lib/csv.js';
import { parseSpotifyJson, convertSpotifyToPlayRecord, sortSpotifyRecords } from '../lib/spotify.js';
import { parseCombinedExports } from '../lib/merge.js';
import { publishRecordsWithApplyWrites } from './publisher.js';
import { prompt } from '../utils/input.js';
import config from '../config.js';
import { calculateOptimalBatchSize } from '../utils/helpers.js';
import { fetchExistingRecords, filterNewRecords, displaySyncStats, removeDuplicates } from './sync.js';
import { Logger, LogLevel, setGlobalLogger, log } from '../utils/logger.js';
import {
  loadImportState,
  createImportState,
  displayResumeInfo,
  clearImportState,
  ImportState,
} from '../utils/import-state.js';

/**
 * Show help message
 */
export function showHelp(): void {
  console.log(`
${'\x1b[1m'}Last.fm to ATProto Importer v0.6.1${'\x1b[0m'}

${'\x1b[1m'}USAGE:${'\x1b[0m'}
  npm start [options]
  lastfm-import [options]

${'\x1b[1m'}AUTHENTICATION:${'\x1b[0m'}
  -h, --handle <handle>          ATProto handle or DID (e.g., user.bsky.social)
  -p, --password <password>      ATProto app password

${'\x1b[1m'}INPUT:${'\x1b[0m'}
  -i, --input <path>             Path to Last.fm CSV or Spotify JSON export
  --spotify-input <path>         Path to Spotify export (for combined mode)

${'\x1b[1m'}MODE:${'\x1b[0m'}
  -m, --mode <mode>              Import mode (default: lastfm)
                                 lastfm          Import Last.fm export only
                                 spotify         Import Spotify export only
                                 combined        Merge Last.fm + Spotify exports
                                 sync            Skip existing records (sync mode)
                                 deduplicate     Remove duplicate records

${'\x1b[1m'}BATCH CONFIGURATION:${'\x1b[0m'}
  -b, --batch-size <number>      Records per batch (default: 100)
  -d, --batch-delay <ms>         Delay between batches in ms (default: 2000ms, min: 1000ms)

${'\x1b[1m'}IMPORT OPTIONS:${'\x1b[0m'}
  -r, --reverse                  Process newest records first (default: oldest first)
  -y, --yes                      Skip confirmation prompts
  --dry-run                      Preview without importing
  --aggressive                   Faster imports (8,500/day vs 7,500/day default)
  --fresh                        Start fresh (ignore previous import state)

${'\x1b[1m'}OUTPUT:${'\x1b[0m'}
  -v, --verbose                  Enable verbose logging (debug level)
  -q, --quiet                    Suppress non-essential output
  --help                         Show this help message

${'\x1b[1m'}EXAMPLES:${'\x1b[0m'}

  ${'\x1b[2m'}# Import Last.fm export${'\x1b[0m'}
  npm start -- -i lastfm-export.csv -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Import Spotify export${'\x1b[0m'}
  npm start -- -i spotify-export/ -m spotify -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Combined import (merge both sources)${'\x1b[0m'}
  npm start -- -i lastfm.csv --spotify-input spotify/ -m combined -h user.bsky.social -p pass

  ${'\x1b[2m'}# Sync mode (only import new records)${'\x1b[0m'}
  npm start -- -i lastfm.csv -m sync -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Dry run with verbose logging${'\x1b[0m'}
  npm start -- -i lastfm.csv --dry-run -v

  ${'\x1b[2m'}# Remove duplicate records${'\x1b[0m'}
  npm start -- -m deduplicate -h user.bsky.social -p app-password

${'\x1b[1m'}NOTES:${'\x1b[0m'}
  • Rate limits: Max 10,000 records/day to avoid PDS rate limiting
  • Import will auto-pause between days for large datasets
  • Press Ctrl+C during import to stop gracefully after current batch
  • Sync mode requires authentication even with --dry-run

${'\x1b[1m'}MORE INFO:${'\x1b[0m'}
  Repository: https://github.com/ewanc26/atproto-lastfm-importer
  Issues: https://github.com/ewanc26/atproto-lastfm-importer/issues
`);
}

/**
 * Parse command line arguments
 */
export function parseCommandLineArgs(): CommandLineArgs {
  const options = {
    // Help
    help: { type: 'boolean', default: false },
    
    // Authentication
    handle: { type: 'string', short: 'h' },
    password: { type: 'string', short: 'p' },
    
    // Input
    input: { type: 'string', short: 'i' },
    'spotify-input': { type: 'string' },
    
    // Mode
    mode: { type: 'string', short: 'm' },
    
    // Batch configuration
    'batch-size': { type: 'string', short: 'b' },
    'batch-delay': { type: 'string', short: 'd' },
    
    // Import options
    reverse: { type: 'boolean', short: 'r', default: false },
    yes: { type: 'boolean', short: 'y', default: false },
    'dry-run': { type: 'boolean', default: false },
    aggressive: { type: 'boolean', default: false },
    fresh: { type: 'boolean', default: false },
    
    // Output
    verbose: { type: 'boolean', short: 'v', default: false },
    quiet: { type: 'boolean', short: 'q', default: false },
    
    // Legacy flags for backwards compatibility (hidden from help)
    file: { type: 'string', short: 'f' },  // Maps to --input
    'spotify-file': { type: 'string' },    // Maps to --spotify-input
    identifier: { type: 'string' },        // Maps to --handle
    'reverse-chronological': { type: 'boolean' },  // Maps to --reverse
    sync: { type: 'boolean', short: 's' },  // Maps to --mode sync
    spotify: { type: 'boolean' },           // Maps to --mode spotify
    combined: { type: 'boolean' },          // Maps to --mode combined
    'remove-duplicates': { type: 'boolean' }, // Maps to --mode deduplicate
  } as const;

  try {
    const { values } = parseArgs({ options, allowPositionals: false });
    
    // Handle legacy flag mappings
    const normalizedArgs: CommandLineArgs = {
      help: values.help,
      handle: values.handle || values.identifier,
      password: values.password,
      input: values.input || values.file,
      'spotify-input': values['spotify-input'] || values['spotify-file'],
      'batch-size': values['batch-size'],
      'batch-delay': values['batch-delay'],
      reverse: values.reverse || values['reverse-chronological'],
      yes: values.yes,
      'dry-run': values['dry-run'],
      aggressive: values.aggressive,
      fresh: values.fresh,
      verbose: values.verbose,
      quiet: values.quiet,
    };
    
    // Determine mode from new --mode flag or legacy flags
    if (values.mode) {
      normalizedArgs.mode = values.mode;
    } else if (values['remove-duplicates']) {
      normalizedArgs.mode = 'deduplicate';
    } else if (values.combined) {
      normalizedArgs.mode = 'combined';
    } else if (values.sync) {
      normalizedArgs.mode = 'sync';
    } else if (values.spotify) {
      normalizedArgs.mode = 'spotify';
    } else {
      normalizedArgs.mode = 'lastfm'; // default
    }
    
    return normalizedArgs;
  } catch (error) {
    const err = error as Error;
    console.error('Error parsing arguments:', err.message);
    showHelp();
    process.exit(1);
  }
}

/**
 * Validate and normalize mode
 */
function validateMode(mode: string): 'lastfm' | 'spotify' | 'combined' | 'sync' | 'deduplicate' {
  const validModes = ['lastfm', 'spotify', 'combined', 'sync', 'deduplicate'];
  const normalized = mode.toLowerCase();
  
  if (!validModes.includes(normalized)) {
    throw new Error(
      `Invalid mode: ${mode}. Must be one of: ${validModes.join(', ')}`
    );
  }
  
  return normalized as 'lastfm' | 'spotify' | 'combined' | 'sync' | 'deduplicate';
}

/**
 * The full, real implementation of the CLI
 */
export async function runCLI(): Promise<void> {
  try {
    const args = parseCommandLineArgs();
    const cfg = config as Config;

    // Setup logging
    const logger = new Logger(
      args.quiet ? LogLevel.WARN :
      args.verbose ? LogLevel.DEBUG :
      LogLevel.INFO
    );
    setGlobalLogger(logger);

    if (args.help) {
      showHelp();
      return;
    }

    // Validate and normalize mode
    const mode = validateMode(args.mode || 'lastfm');
    const dryRun = args['dry-run'] ?? false;
    let agent: AtpAgent | null = null;

    log.debug(`Mode: ${mode}`);
    log.debug(`Dry run: ${dryRun}`);
    log.debug(`Log level: ${args.verbose ? 'DEBUG' : args.quiet ? 'WARN' : 'INFO'}`);

    // Validate mode-specific requirements
    if (mode === 'combined') {
      if (!args.input || !args['spotify-input']) {
        throw new Error('Combined mode requires both --input (Last.fm) and --spotify-input (Spotify)');
      }
    } else if (mode !== 'deduplicate' && !args.input) {
      throw new Error('Missing required argument: --input <path>');
    }

    // Deduplicate mode
    if (mode === 'deduplicate') {
      if (!args.handle || !args.password) {
        throw new Error('Deduplicate mode requires --handle and --password');
      }

      log.section('Remove Duplicate Records');
      agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent;

      const result = await removeDuplicates(agent, cfg, true);

      if (result.totalDuplicates === 0) {
        return;
      }

      if (!dryRun && !args.yes) {
        log.warn(`This will permanently delete ${result.totalDuplicates} duplicate records from Teal.`);
        log.info('The first occurrence of each duplicate will be kept.');
        log.blank();
        const answer = await prompt('Are you sure you want to continue? (y/N) ');
        if (answer.toLowerCase() !== 'y') {
          log.info('Duplicate removal cancelled by user.');
          process.exit(0);
        }

        await removeDuplicates(agent, cfg, false);
        log.success('Duplicate removal complete!');
      } else if (dryRun) {
        log.info('DRY RUN: No records were actually removed.');
        log.info('Remove --dry-run flag to actually delete duplicates.');
      }

      return;
    }

    // Authentication (required for sync mode, even in dry-run)
    if (!dryRun || mode === 'sync') {
      if (!args.handle || !args.password) {
        throw new Error('Missing required arguments: --handle and --password');
      }
      log.debug('Authenticating...');
      agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent;
      log.debug('Authentication successful');
    }

    // Parse and prepare records
    log.section('Loading Records');
    let records: PlayRecord[];
    let rawRecordCount: number;

    const isDebug = args.verbose ?? false;

    if (mode === 'combined') {
      log.info('Merging Last.fm and Spotify exports...');
      records = parseCombinedExports(args.input!, args['spotify-input']!, cfg, isDebug);
      rawRecordCount = records.length;
    } else if (mode === 'spotify') {
      log.info('Importing from Spotify export...');
      const spotifyRecords = parseSpotifyJson(args.input!);
      rawRecordCount = spotifyRecords.length;
      records = spotifyRecords.map(record => convertSpotifyToPlayRecord(record, cfg, isDebug));
    } else {
      log.info('Importing from Last.fm CSV export...');
      const csvRecords = parseLastFmCsv(args.input!);
      rawRecordCount = csvRecords.length;
      records = csvRecords.map(record => convertToPlayRecord(record, cfg, isDebug));
    }

    log.success(`Loaded ${rawRecordCount.toLocaleString()} records`);

    // Sync mode: filter existing records
    if (mode === 'sync' && agent) {
      log.section('Sync Mode');
      log.info('Checking for existing records...');
      const originalRecords = [...records];
      const existingRecords = await fetchExistingRecords(agent, cfg);
      records = filterNewRecords(records, existingRecords);

      if (records.length === 0) {
        log.success('All records already exist in Teal. Nothing to import!');
        process.exit(0);
      }

      displaySyncStats(originalRecords, existingRecords, records);
    }

    const totalRecords = records.length;

    // Sort records (skip for combined mode as it already sorts)
    if (mode !== 'combined') {
      log.debug(`Sorting records (reverse: ${args.reverse})...`);
      records = mode === 'spotify'
        ? sortSpotifyRecords(records, args.reverse ?? false)
        : sortRecords(records, args.reverse ?? false);
    }

    // Determine batch parameters
    log.section('Batch Configuration');
    
    let batchDelay = cfg.DEFAULT_BATCH_DELAY;
    if (args['batch-delay']) {
      const delay = parseInt(args['batch-delay'], 10);
      if (isNaN(delay)) {
        throw new Error(`Invalid batch delay: ${args['batch-delay']}`);
      }
      batchDelay = Math.max(delay, cfg.MIN_BATCH_DELAY);
      if (delay < cfg.MIN_BATCH_DELAY) {
        log.warn(`Batch delay increased to minimum: ${cfg.MIN_BATCH_DELAY}ms`);
      }
    }

    let batchSize: number;
    if (args['batch-size']) {
      batchSize = parseInt(args['batch-size'], 10);
      if (isNaN(batchSize) || batchSize <= 0) {
        throw new Error(`Invalid batch size: ${args['batch-size']}`);
      }
      log.info(`Using manual batch size: ${batchSize} records`);
    } else {
      batchSize = calculateOptimalBatchSize(totalRecords, batchDelay, cfg);
      log.info(`Using auto-calculated batch size: ${batchSize} records`);
    }

    log.info(`Batch delay: ${batchDelay}ms`);

    // Apply aggressive mode if enabled
    const safetyMargin = args.aggressive ? cfg.AGGRESSIVE_SAFETY_MARGIN : cfg.SAFETY_MARGIN;
    if (args.aggressive) {
      log.warn('⚡ Aggressive mode enabled: Using 85% of daily limit (8,500 records/day)');
    }

    // Show rate limiting information
    log.section('Import Configuration');
    log.info(`Total records: ${totalRecords.toLocaleString()}`);
    log.info(`Batch size: ${batchSize} records`);
    log.info(`Batch delay: ${batchDelay}ms`);
    
    const recordsPerDay = cfg.RECORDS_PER_DAY_LIMIT * safetyMargin;
    const estimatedDays = Math.ceil(totalRecords / recordsPerDay);
    
    if (estimatedDays > 1) {
      log.info(`Duration: ${estimatedDays} days (${recordsPerDay.toLocaleString()} records/day limit)`);
      log.warn('Large import will span multiple days with automatic pauses');
    }
    
    log.blank();

    // Check for existing import state (resume functionality)
    let importState: ImportState | null = null;
    if (!dryRun && args.input) {
      // Clear state if --fresh flag is used
      if (args.fresh) {
        clearImportState(args.input, mode);
        log.info('Starting fresh import (previous state cleared)');
      } else {
        // Try to load existing state
        importState = loadImportState(args.input, mode);
        
        if (importState && !importState.completed) {
          displayResumeInfo(importState);
          
          if (!args.yes) {
            const answer = await prompt('Resume from previous import? (Y/n) ');
            if (answer.toLowerCase() === 'n') {
              importState = null;
              clearImportState(args.input, mode);
              log.info('Starting fresh import');
              log.blank();
            }
          } else {
            log.info('Auto-resuming previous import (--yes flag)');
            log.blank();
          }
        } else if (importState?.completed) {
          log.info('Previous import was completed - starting fresh');
          importState = null;
          clearImportState(args.input, mode);
        }
      }
      
      // Create new state if not resuming
      if (!importState) {
        importState = createImportState(args.input, mode, totalRecords);
        log.debug('Created new import state');
      }
    }

    // Confirmation prompt
    if (!dryRun && !args.yes) {
      const modeLabel = mode === 'combined' ? 'merged' : mode === 'sync' ? 'new' : '';
      const skippedInfo = mode === 'sync' ? ` (${rawRecordCount - totalRecords} skipped)` : '';
      log.raw(`Ready to publish ${totalRecords.toLocaleString()} ${modeLabel} records${skippedInfo}`);
      const answer = await prompt('Continue? (y/N) ');
      if (answer.toLowerCase() !== 'y') {
        log.info('Cancelled by user.');
        process.exit(0);
      }
      log.blank();
    }

    // Publish records
    log.section('Publishing Records');
    const result: PublishResult = await publishRecordsWithApplyWrites(
      agent,
      records,
      batchSize,
      batchDelay,
      cfg,
      dryRun,
      mode === 'sync' || mode === 'combined',
      importState
    );

    // Final output
    log.blank();
    if (result.cancelled) {
      log.warn(`Stopped: ${result.successCount.toLocaleString()} processed`);
    } else if (dryRun) {
      const modeLabel = mode === 'combined' ? 'COMBINED' : mode === 'sync' ? 'SYNC' : '';
      log.success(`Dry run complete${modeLabel ? ` (${modeLabel})` : ''}`);
    } else {
      const modeLabel = mode === 'combined' ? 'Combined' : mode === 'sync' ? 'Sync' : 'Import';
      log.success(`${modeLabel} complete!`);
      log.info(`Processed: ${result.successCount.toLocaleString()} (${result.errorCount.toLocaleString()} failed)`);
      if (mode === 'sync' || mode === 'combined') {
        const skipped = rawRecordCount - totalRecords;
        if (skipped > 0) {
          log.info(`Skipped: ${skipped.toLocaleString()} duplicates`);
        }
      }
    }

  } catch (error) {
    const err = error as Error;
    log.blank();
    log.fatal('A fatal error occurred:');
    log.error(err.message);
    if (log.getLevel() <= LogLevel.DEBUG) {
      console.error(err.stack);
    }
    process.exit(1);
  }
}
