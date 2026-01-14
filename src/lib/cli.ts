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
import { fetchExistingRecords, filterNewRecords, displaySyncStats, removeDuplicates, deduplicateInputRecords } from './sync.js';
import { Logger, LogLevel, setGlobalLogger, log } from '../utils/logger.js';
import { registerKillswitch } from '../utils/killswitch.js';
import { clearCache, clearAllCaches } from '../utils/teal-cache.js';
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
${'\x1b[1m'}Last.fm to ATProto Importer v0.6.2${'\x1b[0m'}

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
  --fresh                        Start fresh (ignore cache & previous import state)
  --clear-cache                  Clear cached records for current user
  --clear-all-caches             Clear all cached records

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

  ${'\x1b[2m'}# Clear cache for current user${'\x1b[0m'}
  npm start -- --clear-cache -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Clear all caches${'\x1b[0m'}
  npm start -- --clear-all-caches

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
    help: { type: 'boolean', default: false },
    handle: { type: 'string', short: 'h' },
    password: { type: 'string', short: 'p' },
    input: { type: 'string', short: 'i' },
    'spotify-input': { type: 'string' },
    mode: { type: 'string', short: 'm' },
    'batch-size': { type: 'string', short: 'b' },
    'batch-delay': { type: 'string', short: 'd' },
    reverse: { type: 'boolean', short: 'r', default: false },
    yes: { type: 'boolean', short: 'y', default: false },
    'dry-run': { type: 'boolean', default: false },
    aggressive: { type: 'boolean', default: false },
    fresh: { type: 'boolean', default: false },
    'clear-cache': { type: 'boolean', default: false },
    'clear-all-caches': { type: 'boolean', default: false },
    verbose: { type: 'boolean', short: 'v', default: false },
    quiet: { type: 'boolean', short: 'q', default: false },
    file: { type: 'string', short: 'f' },
    'spotify-file': { type: 'string' },
    identifier: { type: 'string' },
    'reverse-chronological': { type: 'boolean' },
    sync: { type: 'boolean', short: 's' },
    spotify: { type: 'boolean' },
    combined: { type: 'boolean' },
    'remove-duplicates': { type: 'boolean' },
  } as const;

  try {
    const { values } = parseArgs({ options, allowPositionals: false });
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
      'clear-cache': values['clear-cache'],
      'clear-all-caches': values['clear-all-caches'],
      verbose: values.verbose,
      quiet: values.quiet,
    };

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
      normalizedArgs.mode = 'lastfm';
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
    throw new Error(`Invalid mode: ${mode}. Must be one of: ${validModes.join(', ')}`);
  }
  return normalized as 'lastfm' | 'spotify' | 'combined' | 'sync' | 'deduplicate';
}

/**
 * The full, real implementation of the CLI
 */
export async function runCLI(): Promise<void> {
  try {
    registerKillswitch();
    const args = parseCommandLineArgs();
    const cfg = config as Config;
    let agent: AtpAgent | null = null;

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

    if (args['clear-all-caches']) {
      log.section('Clear All Caches');
      clearAllCaches();
      log.success('All caches cleared successfully');
      return;
    }

    if (args['clear-cache']) {
      if (!args.handle || !args.password) {
        throw new Error('--clear-cache requires --handle and --password to identify the cache');
      }
      log.section('Clear Cache');
      log.info('Authenticating to identify cache...');
      agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent;
      const did = agent.session?.did;
      if (!did) {
        throw new Error('Failed to get DID from session');
      }
      clearCache(did);
      log.success(`Cache cleared for ${args.handle} (${did})`);
      return;
    }

    const mode = validateMode(args.mode || 'lastfm');
    const dryRun = args['dry-run'] ?? false;

    log.debug(`Mode: ${mode}`);
    log.debug(`Dry run: ${dryRun}`);
    log.debug(`Log level: ${args.verbose ? 'DEBUG' : args.quiet ? 'WARN' : 'INFO'}`);

    if (mode === 'combined') {
      if (!args.input || !args['spotify-input']) {
        throw new Error('Combined mode requires both --input (Last.fm) and --spotify-input (Spotify)');
      }
    } else if (mode !== 'deduplicate' && !args.input) {
      throw new Error('Missing required argument: --input <path>');
    }

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

    if (!args.handle || !args.password) {
      throw new Error('Missing required arguments: --handle and --password');
    }
    log.debug('Authenticating...');
    agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent;
    log.debug('Authentication successful');

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

    const dedupResult = deduplicateInputRecords(records);
    records = dedupResult.unique;
    if (dedupResult.duplicates > 0) {
      log.warn(`Removed ${dedupResult.duplicates.toLocaleString()} duplicate(s) from input data`);
      log.info(`Unique records: ${records.length.toLocaleString()}`);
    } else {
      log.info(`No duplicates found in input data`);
    }
    log.blank();

    if (agent) {
      const originalRecords = [...records];
      const existingRecords = await fetchExistingRecords(agent, cfg, args.fresh ?? false);
      records = filterNewRecords(records, existingRecords);
      if (records.length === 0) {
        log.success('All records already exist in Teal. Nothing to import!');
        process.exit(0);
      }
      if (mode === 'sync' || mode === 'combined') {
        displaySyncStats(originalRecords, existingRecords, records);
      } else {
        const skipped = originalRecords.length - records.length;
        if (skipped > 0) {
          log.info(`Found ${skipped.toLocaleString()} record(s) already in Teal (skipping)`);
          log.info(`New records to import: ${records.length.toLocaleString()}`);
        } else {
          log.info(`All ${records.length.toLocaleString()} records are new`);
        }
        log.blank();
      }
    }

    const totalRecords = records.length;

    if (mode !== 'combined') {
      log.debug(`Sorting records (reverse: ${args.reverse})...`);
      records = mode === 'spotify'
        ? sortSpotifyRecords(records, args.reverse ?? false)
        : sortRecords(records, args.reverse ?? false);
    }

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

    const safetyMargin = args.aggressive ? cfg.AGGRESSIVE_SAFETY_MARGIN : cfg.SAFETY_MARGIN;
    if (args.aggressive) {
      log.warn('⚡ Aggressive mode enabled: Using 85% of daily limit (8,500 records/day)');
    }

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

    let importState: ImportState | null = null;
    if (!dryRun && args.input) {
      if (args.fresh) {
        clearImportState(args.input, mode);
        log.info('Starting fresh import (previous state cleared)');
      } else {
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
      if (!importState) {
        importState = createImportState(args.input, mode, totalRecords);
        log.debug('Created new import state');
      }
    }

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
    if (err.message === 'Operation cancelled by user') {
      log.blank();
      log.warn('Operation cancelled by user');
      process.exit(0);
    }
    log.blank();
    log.fatal('A fatal error occurred:');
    log.error(err.message);
    if (log.getLevel() <= LogLevel.DEBUG) {
      console.error(err.stack);
    }
    process.exit(1);
  }
}
