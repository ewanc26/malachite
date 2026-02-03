#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { SafeAgent } from './safe-agent.js';
import type { PlayRecord, Config, CommandLineArgs, PublishResult } from '../types.js';
import { login } from './auth.js';
import { nukeWithConfirmation } from './nuke.js';
import { parseLastFmCsv, convertToPlayRecord, sortRecords } from '../lib/csv.js';
import { parseSpotifyJson, convertSpotifyToPlayRecord, sortSpotifyRecords } from '../lib/spotify.js';
import { parseCombinedExports } from '../lib/merge.js';
import { publishRecordsWithApplyWrites } from './publisher.js';
import { prompt, confirm } from '../utils/input.js';
import config from '../config.js';
import { calculateOptimalBatchSize } from '../utils/helpers.js';
import { fetchExistingRecords, filterNewRecords, displaySyncStats, removeDuplicates, deduplicateInputRecords } from './sync.js';
import { Logger, LogLevel, setGlobalLogger, log } from '../utils/logger.js';
import { registerKillswitch } from '../utils/killswitch.js';
import { clearCache, clearAllCaches } from '../utils/teal-cache.js';
import { 
  saveCredentials, 
  loadCredentials, 
  hasStoredCredentials, 
  clearCredentials,
  getStoredHandle,
  getCredentialsInfo 
} from '../utils/credentials.js';
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
${'\x1b[1m'}Malachite v0.8.0${'\x1b[0m'}

${'\x1b[1m'}USAGE:${'\x1b[0m'}
  pnpm start                     Run in interactive mode (prompts for all inputs)
  pnpm start [options]           Run with command-line arguments
  malachite [options]            Same as above when installed globally

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
                                 nuke            Delete ALL records (DESTRUCTIVE)

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
  --clear-credentials            Clear saved credentials

${'\x1b[1m'}OUTPUT:${'\x1b[0m'}
  -v, --verbose                  Enable verbose logging (debug level)
  -q, --quiet                    Suppress non-essential output
  --dev                          Development mode (verbose + file logging + smaller batches)
  --help                         Show this help message

${'\x1b[1m'}EXAMPLES:${'\x1b[0m'}
  ${'\x1b[2m'}# Import Last.fm export${'\x1b[0m'}
  pnpm start -i lastfm-export.csv -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Import Spotify export${'\x1b[0m'}
  pnpm start -i spotify-export/ -m spotify -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Combined import (merge both sources)${'\x1b[0m'}
  pnpm start -i lastfm.csv --spotify-input spotify/ -m combined -h user.bsky.social -p pass

  ${'\x1b[2m'}# Sync mode (only import new records)${'\x1b[0m'}
  pnpm start -i lastfm.csv -m sync -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Dry run with verbose logging${'\x1b[0m'}
  pnpm start -i lastfm.csv --dry-run -v

  ${'\x1b[2m'}# Development mode (verbose + file logging + debug batches)${'\x1b[0m'}
  pnpm start -i lastfm.csv --dev --dry-run

  ${'\x1b[2m'}# Remove duplicate records${'\x1b[0m'}
  pnpm start -m deduplicate -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Clear cache for current user${'\x1b[0m'}
  pnpm start --clear-cache -h user.bsky.social -p app-password

  ${'\x1b[2m'}# Clear all caches${'\x1b[0m'}
  pnpm start --clear-all-caches

  ${'\x1b[2m'}# Clear saved credentials${'\x1b[0m'}
  pnpm start --clear-credentials

${'\x1b[1m'}NOTES:${'\x1b[0m'}
  • Rate limits: Max 10,000 records/day to avoid PDS rate limiting
  • Import will auto-pause between days for large datasets
  • Press Ctrl+C during import to stop gracefully after current batch
  • Sync mode requires authentication even with --dry-run

${'\x1b[1m'}MORE INFO:${'\x1b[0m'}
  Repository: https://github.com/ewanc26/malachite
  Issues: https://github.com/ewanc26/malachite/issues
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
    'clear-credentials': { type: 'boolean', default: false },
    verbose: { type: 'boolean', short: 'v', default: false },
    quiet: { type: 'boolean', short: 'q', default: false },
    dev: { type: 'boolean', default: false },
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
      'clear-credentials': values['clear-credentials'],
      verbose: values.verbose,
      quiet: values.quiet,
      dev: values.dev,
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
function validateMode(mode: string): 'lastfm' | 'spotify' | 'combined' | 'sync' | 'deduplicate' | 'nuke' {
  const validModes = ['lastfm', 'spotify', 'combined', 'sync', 'deduplicate', 'nuke'];
  const normalized = mode.toLowerCase();
  if (!validModes.includes(normalized)) {
    throw new Error(`Invalid mode: ${mode}. Must be one of: ${validModes.join(', ')}`);
  }
  return normalized as 'lastfm' | 'spotify' | 'combined' | 'sync' | 'deduplicate' | 'nuke';
}

/**
 * Interactive mode - prompts user for all required inputs
 */
async function runInteractiveMode(): Promise<CommandLineArgs> {
  console.log('\n' + '┏' + '━'.repeat(58) + '┓');
  console.log('┃' + ' '.repeat(58) + '┃');
  console.log('┃' + '  Welcome to Malachite - Interactive Mode'.padEnd(58) + '┃');
  console.log('┃' + ' '.repeat(58) + '┃');
  console.log('┗' + '━'.repeat(58) + '┛\n');
  
  console.log('\x1b[1mWhat would you like to do?\x1b[0m\n');
  console.log('\x1b[36m╭─ IMPORT OPERATIONS ─────────────────────────────╮\x1b[0m');
  console.log('\x1b[36m│\x1b[0m                                                 \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m  \x1b[1m1\x1b[0m │ Import Last.fm scrobbles                   \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m    │ \x1b[2mFrom Last.fm CSV export\x1b[0m                   \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m                                                 \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m  \x1b[1m2\x1b[0m │ Import Spotify history                     \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m    │ \x1b[2mFrom Spotify JSON export\x1b[0m                  \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m                                                 \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m  \x1b[1m3\x1b[0m │ Combine Last.fm + Spotify                  \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m    │ \x1b[2mMerge both sources with deduplication\x1b[0m     \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m                                                 \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m  \x1b[1m4\x1b[0m │ Sync new records only                      \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m    │ \x1b[2mSkip records already in Teal\x1b[0m              \x1b[36m│\x1b[0m');
  console.log('\x1b[36m│\x1b[0m                                                 \x1b[36m│\x1b[0m');
  console.log('\x1b[36m╰─────────────────────────────────────────────────╯\x1b[0m\n');
  
  console.log('\x1b[33m╭─ MAINTENANCE ───────────────────────────────────╮\x1b[0m');
  console.log('\x1b[33m│\x1b[0m                                                 \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m  \x1b[1m5\x1b[0m │ Remove duplicate records                   \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m    │ \x1b[2mClean up duplicates in Teal\x1b[0m               \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m                                                 \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m  \x1b[1m6\x1b[0m │ Clear cache                                \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m    │ \x1b[2mRemove cached Teal records\x1b[0m                \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m                                                 \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m  \x1b[1m7\x1b[0m │ Clear saved credentials                    \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m    │ \x1b[2mRemove stored login info\x1b[0m                  \x1b[33m│\x1b[0m');
  console.log('\x1b[33m│\x1b[0m                                                 \x1b[33m│\x1b[0m');
  console.log('\x1b[33m╰─────────────────────────────────────────────────╯\x1b[0m\n');
  
  console.log('\x1b[31m╭─ DESTRUCTIVE ───────────────────────────────────╮\x1b[0m');
  console.log('\x1b[31m│\x1b[0m                                                 \x1b[31m│\x1b[0m');
  console.log('\x1b[31m│\x1b[0m  \x1b[1m8\x1b[0m │ ⚠️  Delete ALL records                     \x1b[31m│\x1b[0m');
  console.log('\x1b[31m│\x1b[0m    │ \x1b[2mPermanently remove all records (DANGER!)\x1b[0m  \x1b[31m│\x1b[0m');
  console.log('\x1b[31m│\x1b[0m                                                 \x1b[31m│\x1b[0m');
  console.log('\x1b[31m╰─────────────────────────────────────────────────╯\x1b[0m\n');
  
  console.log('\x1b[90m  0 │ Exit\x1b[0m\n');
  
  const mode = await prompt('\x1b[1mEnter your choice [0-8]:\x1b[0m ');
  
  if (mode === '0' || !mode) {
    console.log('\nGoodbye!');
    process.exit(0);
  }
  
  // Validate input
  if (!['1', '2', '3', '4', '5', '6', '7', '8'].includes(mode)) {
    console.log('\nInvalid choice. Please run again and select a valid option (0-8).');
    process.exit(1);
  }
  
  const args: CommandLineArgs = {};
  
  // Map selection to mode
  if (mode === '1') args.mode = 'lastfm';
  else if (mode === '2') args.mode = 'spotify';
  else if (mode === '3') args.mode = 'combined';
  else if (mode === '4') args.mode = 'sync';
  else if (mode === '5') args.mode = 'deduplicate';
  else if (mode === '6') {
    args['clear-cache'] = true;
    return args;
  }
  else if (mode === '7') {
    args['clear-credentials'] = true;
    return args;
  }
  else if (mode === '8') args.mode = 'nuke';
  
  console.log('');
  
  // Get authentication (not needed for clear cache)
  if (args.mode === 'deduplicate' || args.mode === 'sync' || args.mode === 'combined' || args.mode === 'lastfm' || args.mode === 'spotify' || args.mode === 'nuke') {
    // Check for saved credentials
    const savedCreds = hasStoredCredentials();
    let useSavedCreds = false;
    
    if (savedCreds) {
      const storedHandle = getStoredHandle();
      console.log(`\n🔑 Found saved credentials for: ${storedHandle}`);
      useSavedCreds = await confirm('Use saved credentials?', true);
    }
    
    if (useSavedCreds) {
      const creds = loadCredentials();
      if (creds) {
        args.handle = creds.handle;
        args.password = creds.password;
        console.log('✓ Loaded saved credentials');
      } else {
        console.log('⚠️  Failed to load saved credentials. Please enter manually:');
        useSavedCreds = false;
      }
    }
    
    if (!useSavedCreds) {
      let handle = '';
      while (!handle) {
        handle = await prompt('ATProto handle (e.g., alice.bsky.social): ');
        if (!handle) {
          console.log('⚠️  Handle is required. Please try again.');
        }
      }
      args.handle = handle;
      
      let password = '';
      while (!password) {
        password = await prompt('App password: ', true);
        if (!password) {
          console.log('⚠️  Password is required. Please try again.');
        }
      }
      args.password = password;
      
      // Offer to save credentials
      const saveCredsAnswer = await confirm('\nSave credentials for future use? (encrypted, machine-specific)', false);
      if (saveCredsAnswer) {
        saveCredentials(args.handle, args.password);
        console.log('✓ Credentials saved securely to ~/.malachite/credentials.json');
      }
    }
    
    console.log('');
  }
  
  // Get input files
  if (args.mode !== 'deduplicate' && args.mode !== 'nuke') {
    if (args.mode === 'combined') {
      let input = '';
      while (!input) {
        input = await prompt('Path to Last.fm CSV file: ');
        if (!input) {
          console.log('⚠️  Path is required. Please try again.');
        }
      }
      args.input = input;
      
      let spotifyInput = '';
      while (!spotifyInput) {
        spotifyInput = await prompt('Path to Spotify export (file or directory): ');
        if (!spotifyInput) {
          console.log('⚠️  Path is required. Please try again.');
        }
      }
      args['spotify-input'] = spotifyInput;
    } else if (args.mode === 'spotify') {
      let input = '';
      while (!input) {
        input = await prompt('Path to Spotify export (file or directory): ');
        if (!input) {
          console.log('⚠️  Path is required. Please try again.');
        }
      }
      args.input = input;
    } else {
      let input = '';
      while (!input) {
        input = await prompt('Path to Last.fm CSV file: ');
        if (!input) {
          console.log('⚠️  Path is required. Please try again.');
        }
      }
      args.input = input;
    }
    console.log('');
  }
  
  // Additional options
  console.log('\nAdditional Options (press Enter to skip):');
  console.log('─'.repeat(50));
  
  if (args.mode !== 'deduplicate' && args.mode !== 'nuke') {
    const dryRun = await confirm('Preview without importing (dry run)?', false);
    if (dryRun) args['dry-run'] = true;
    
    const verbose = await confirm('Enable verbose logging?', false);
    if (verbose) args.verbose = true;
    
    const reverse = await confirm('Process newest records first?', false);
    if (reverse) args.reverse = true;
  }
  
  args.yes = true; // Auto-confirm in interactive mode since user already confirmed via prompts
  
  return args;
}

/**
 * The full, real implementation of the CLI
 */
export async function runCLI(): Promise<void> {
  try {
    registerKillswitch();
    let args = parseCommandLineArgs();
    
    // Check if running with no arguments (interactive mode)
    // Modifier flags like --dry-run, --verbose, --yes, etc. don't count as "real" arguments
    const modifierFlags = ['dry-run', 'verbose', 'quiet', 'yes', 'reverse', 'aggressive', 'fresh', 'dev'];
    const hasSubstantiveArgs = Object.keys(args).some(key => {
      const value = args[key as keyof CommandLineArgs];
      // Skip undefined, false values, and default mode
      if (value === undefined || value === false || (key === 'mode' && value === 'lastfm')) {
        return false;
      }
      // Skip modifier flags
      if (modifierFlags.includes(key)) {
        return false;
      }
      return true;
    });
    
    if (!hasSubstantiveArgs) {
      // No substantive arguments provided - run interactive mode
      args = await runInteractiveMode();
    }
    
    const cfg = config as Config;
    let agent: SafeAgent | null = null;

    // Development mode enables verbose logging and file logging
    const isDev = args.dev ?? false;
    const isVerbose = args.verbose || isDev;
    const isQuiet = args.quiet && !isDev; // dev overrides quiet

    const logger = new Logger(
      isQuiet ? LogLevel.WARN :
      isVerbose ? LogLevel.DEBUG :
      LogLevel.INFO
    );
    setGlobalLogger(logger);

    // Enable file logging in development mode
    if (isDev) {
      logger.enableFileLogging();
      log.info('🔧 Development mode enabled');
      log.info(`   → Verbose logging: ON`);
      log.info(`   → File logging: ${log.getLogFile()}`);
      log.info(`   → Smaller batch sizes for easier debugging`);
      log.blank();
    }

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

    if (args['clear-credentials']) {
      log.section('Clear Saved Credentials');
      if (hasStoredCredentials()) {
        const info = getCredentialsInfo();
        if (info) {
          log.info(`Saved credentials for: ${info.handle}`);
          log.info(`Created: ${new Date(info.createdAt).toLocaleString()}`);
          log.info(`Last used: ${new Date(info.lastUsedAt).toLocaleString()}`);
        }
        clearCredentials();
        log.success('Saved credentials cleared successfully');
      } else {
        log.info('No saved credentials found');
      }
      return;
    }

    if (args['clear-cache']) {
      if (!args.handle || !args.password) {
        throw new Error('--clear-cache requires --handle and --password to identify the cache');
      }
      log.section('Clear Cache');
      log.info('Authenticating to identify cache...');
      agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER);
      const did = agent?.session?.did;
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
    } else if (mode !== 'deduplicate' && mode !== 'nuke' && !args.input) {
      throw new Error('Missing required argument: --input <path>');
    }

    if (mode === 'deduplicate') {
      // Try to load saved credentials if not provided
      if (!args.handle || !args.password) {
        const creds = loadCredentials();
        if (creds) {
          args.handle = creds.handle;
          args.password = creds.password;
          log.info(`Using saved credentials for: ${creds.handle}`);
        } else {
          throw new Error('Deduplicate mode requires --handle and --password (or saved credentials)');
        }
      }
      log.section('Remove Duplicate Records');
      agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER);
      const result = await removeDuplicates(agent, cfg, dryRun);
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

    if (mode === 'nuke') {
      // Try to load saved credentials if not provided
      if (!args.handle || !args.password) {
        const creds = loadCredentials();
        if (creds) {
          args.handle = creds.handle;
          args.password = creds.password;
          log.info(`Using saved credentials for: ${creds.handle}`);
        } else {
          throw new Error('Nuke mode requires --handle and --password (or saved credentials)');
        }
      }
      agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER);
      const result = await nukeWithConfirmation(agent, cfg, args.yes ?? false);
      if (result.cancelled) {
        log.info('Nuke operation cancelled.');
        process.exit(0);
      }
      return;
    }

    // Try to load saved credentials if not provided
    if (!args.handle || !args.password) {
      const creds = loadCredentials();
      if (creds) {
        args.handle = creds.handle;
        args.password = creds.password;
        log.info(`Using saved credentials for: ${creds.handle}`);
      } else {
        throw new Error('Missing required arguments: --handle and --password (or saved credentials)');
      }
    }
    log.debug('Authenticating...');
    agent = await login(args.handle, args.password, cfg.SLINGSHOT_RESOLVER);
    log.debug('Authentication successful');

    log.section('Loading Records');
    let records: PlayRecord[];
    let rawRecordCount: number;
    const isDebug = isVerbose;

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
      
      // In dev mode, use smaller batches for easier debugging
      if (isDev && batchSize > 20) {
        batchSize = Math.min(20, batchSize);
        log.info(`Using dev batch size: ${batchSize} records (capped for debugging)`);
      } else {
        log.info(`Using auto-calculated batch size: ${batchSize} records`);
      }
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
  } finally {
    // Close log file if it was opened
    log.closeLogFile();
  }
}
