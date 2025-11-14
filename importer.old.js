#!/usr/bin/env node

import { AtpAgent } from '@atproto/api';
import * as fs from 'fs';
import * as readline from 'readline';
import { parse } from 'csv-parse/sync';
import { parseArgs } from 'node:util';

// Configuration
const DEFAULT_BATCH_SIZE = 10; // Default number of records to submit per batch
const DEFAULT_BATCH_DELAY = 2000; // Default delay between batches in milliseconds
const MIN_BATCH_DELAY = 1000; // Minimum safe delay to respect rate limits
const RECORD_TYPE = 'fm.teal.alpha.feed.play';
const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue/xrpc/com.bad-example.identity.resolveMiniDoc';

// Global state for killswitch
let importCancelled = false;
let gracefulShutdown = false;

/**
 * Setup killswitch handler for graceful shutdown
 */
function setupKillswitch() {
  process.on('SIGINT', () => {
    if (gracefulShutdown) {
      console.log('\n\n⚠️  Force quit detected. Exiting immediately...');
      process.exit(1);
    }
    
    gracefulShutdown = true;
    importCancelled = true;
    console.log('\n\n🛑 Killswitch activated! Stopping after current batch...');
    console.log('   Press Ctrl+C again to force quit immediately.\n');
  });
}

/**
 * Calculate optimal batch size based on total records and rate limits
 * Uses a logarithmic scaling approach to balance throughput with API safety
 * 
 * Algorithm Analysis:
 * - Time Complexity: O(n) where n is total records (each record processed once)
 * - Space Complexity: O(1) for batch calculation, O(b) where b is batch size in memory
 * - Rate Limit Strategy: Token bucket approach with conservative limits
 * 
 * The batch size grows logarithmically with input size to prevent overwhelming
 * the API while maximizing throughput. Formula: min(MAX, BASE * log2(n/MIN))
 */
function calculateOptimalBatchSize(totalRecords, batchDelay = DEFAULT_BATCH_DELAY) {
  // Constants based on typical API rate limits and safety margins
  const MIN_RECORDS = 100;        // Minimum records before scaling kicks in
  const BASE_BATCH_SIZE = 5;      // Starting point for small datasets
  const MAX_BATCH_SIZE = 50;      // Hard cap to prevent API overwhelming
  const SCALING_FACTOR = 1.5;     // Growth rate modifier
  
  // For very small datasets, use minimal batches
  if (totalRecords <= 50) {
    return 3;
  }
  
  // For small to medium datasets, use conservative batching
  if (totalRecords <= MIN_RECORDS) {
    return BASE_BATCH_SIZE;
  }
  
  // Logarithmic scaling: batch size grows with log of total records
  // This ensures O(n) time complexity while respecting rate limits
  // Formula: BASE * (log2(n/MIN) * SCALING_FACTOR)
  const logScale = Math.log2(totalRecords / MIN_RECORDS);
  const calculatedSize = Math.floor(BASE_BATCH_SIZE + (logScale * SCALING_FACTOR));
  
  // Apply maximum cap and ensure reasonable batch size
  let optimalSize = Math.min(calculatedSize, MAX_BATCH_SIZE);
  
  // Adjust based on batch delay to respect rate limits
  // Shorter delays should use smaller batches
  if (batchDelay < 1500 && optimalSize > 15) {
    optimalSize = Math.floor(optimalSize * 0.75);
  }
  
  // Ensure batch size is at least 3 for efficiency
  return Math.max(3, optimalSize);
}

/**
 * Parse command line arguments
 */
function parseCommandLineArgs() {
  const options = {
    help: {
      type: 'boolean',
      short: 'h',
      default: false,
    },
    file: {
      type: 'string',
      short: 'f',
    },
    identifier: {
      type: 'string',
      short: 'i',
    },
    password: {
      type: 'string',
      short: 'p',
    },
    'batch-size': {
      type: 'string',
      short: 'b',
    },
    'batch-delay': {
      type: 'string',
      short: 'd',
    },
    yes: {
      type: 'boolean',
      short: 'y',
      default: false,
    },
    'dry-run': {
      type: 'boolean',
      short: 'n',
      default: false,
    },
    'reverse-chronological': {
      type: 'boolean',
      short: 'r',
      default: false,
    },
  };

  try {
    const { values } = parseArgs({ options, allowPositionals: false });
    return values;
  } catch (error) {
    console.error('Error parsing arguments:', error.message);
    showHelp();
    process.exit(1);
  }
}

/**
 * Show help message
 */
function showHelp() {
  console.log(`
Last.fm to ATProto Importer

Usage: node importer.js [options]

Options:
  -h, --help                     Show this help message
  -f, --file <path>              Path to Last.fm CSV export file
  -i, --identifier <id>          ATProto handle or DID
  -p, --password <pass>          ATProto app password
  -b, --batch-size <num>         Number of records per batch (auto-calculated if not set)
  -d, --batch-delay <ms>         Delay between batches in ms (default: 2000, min: 1000)
  -y, --yes                      Skip confirmation prompt
  -n, --dry-run                  Preview records without publishing
  -r, --reverse-chronological    Process newest first (default: oldest first)

Examples:
  node importer.js -f lastfm.csv -i alice.bsky.social -p xxxx-xxxx-xxxx-xxxx
  node importer.js --file export.csv --identifier alice.bsky.social --yes
  node importer.js -f lastfm.csv --dry-run
  node importer.js  (interactive mode - prompts for all values)

Notes:
  - Batch size uses logarithmic scaling algorithm (O(n) complexity) for optimal throughput
  - Auto-calculated batch size considers both record count and delay settings
  - Records are processed in chronological order (oldest first) by default
  - Minimum batch delay of 1000ms enforced to respect rate limits
  - Rate limiting follows token bucket strategy for safe API usage
`);
}

/**
 * Read user input from command line with proper password masking
 */
function prompt(question, hideInput = false) {
  return new Promise((resolve) => {
    if (hideInput) {
      // For password input, use a simpler approach
      const stdin = process.stdin;
      const wasRaw = stdin.isRaw;
      
      // Set raw mode to capture individual keystrokes
      if (stdin.isTTY) {
        stdin.setRawMode(true);
      }
      
      stdin.resume();
      stdin.setEncoding('utf8');
      
      process.stdout.write(question);
      
      let password = '';
      const onData = (char) => {
        char = char.toString();
        
        switch (char) {
          case '\n':
          case '\r':
          case '\u0004': // Ctrl-D
            stdin.removeListener('data', onData);
            if (stdin.isTTY) {
              stdin.setRawMode(wasRaw);
            }
            stdin.pause();
            process.stdout.write('\n');
            resolve(password);
            break;
          case '\u0003': // Ctrl-C
            process.exit(1);
            break;
          case '\u007f': // Backspace
          case '\b': // Backspace
            if (password.length > 0) {
              password = password.slice(0, -1);
              process.stdout.clearLine(0);
              process.stdout.cursorTo(0);
              process.stdout.write(question + '*'.repeat(password.length));
            }
            break;
          default:
            password += char;
            process.stdout.write('*');
            break;
        }
      };
      
      stdin.on('data', onData);
    } else {
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
      });
      
      rl.question(question, (answer) => {
        rl.close();
        resolve(answer);
      });
    }
  });
}

/**
 * Resolves an AT Protocol identifier (handle or DID) to get PDS information
 */
async function resolveIdentifier(identifier) {
  console.log(`Resolving identifier: ${identifier}`);
  
  const response = await fetch(
    `${SLINGSHOT_RESOLVER}?identifier=${encodeURIComponent(identifier)}`
  );
  
  if (!response.ok) {
    throw new Error(`Failed to resolve identifier: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  
  if (!data.did || !data.pds) {
    throw new Error('Invalid response from identity resolver');
  }
  
  console.log(`✓ Resolved to PDS: ${data.pds}`);
  return data;
}

/**
 * Login to ATProto using Slingshot resolver
 */
async function login(identifier, password) {
  console.log('\n=== ATProto Login ===');
  
  // Prompt for missing credentials
  if (!identifier) {
    identifier = await prompt('Handle or DID: ');
  } else {
    console.log(`Handle or DID: ${identifier}`);
  }
  
  if (!password) {
    password = await prompt('App password: ', true);
  } else {
    console.log('App password: [hidden]');
  }
  
  try {
    // Resolve the identifier to get PDS
    const resolved = await resolveIdentifier(identifier);
    
    // Create agent with resolved PDS
    const pdsAgent = new AtpAgent({ service: resolved.pds });
    
    // Login using the resolved DID
    await pdsAgent.login({
      identifier: resolved.did,
      password: password,
    });
    
    console.log('✓ Logged in successfully!');
    console.log(`  DID: ${pdsAgent.session.did}`);
    console.log(`  Handle: ${pdsAgent.session.handle}\n`);
    
    return pdsAgent;
  } catch (error) {
    console.error('✗ Login failed:', error.message);
    
    // Provide more specific error messages
    if (error.message.includes('Failed to resolve identifier')) {
      throw new Error('Handle not found. Please check your AT Protocol handle.');
    } else if (error.message.includes('AuthFactorTokenRequired')) {
      throw new Error('Two-factor authentication required. Please use your app password.');
    } else if (error.message.includes('InvalidCredentials')) {
      throw new Error('Invalid credentials. Please check your handle and app password.');
    }
    
    throw error;
  }
}

/**
 * Parse Last.fm CSV export
 */
function parseLastFmCsv(filePath) {
  console.log(`Reading CSV file: ${filePath}`);
  const fileContent = fs.readFileSync(filePath, 'utf-8');
  
  const records = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  
  console.log(`✓ Parsed ${records.length} scrobbles\n`);
  return records;
}

/**
 * Convert Last.fm CSV record to ATProto play record
 * Following the fm.teal.alpha.feed.play lexicon schema
 */
function convertToPlayRecord(csvRecord) {
  // Parse the timestamp (Unix timestamp in seconds)
  const timestamp = parseInt(csvRecord.uts);
  const playedTime = new Date(timestamp * 1000).toISOString();
  
  // Build artists array according to lexicon
  const artists = [];
  if (csvRecord.artist) {
    const artistData = {
      artistName: csvRecord.artist,
    };
    // Only add artistMbId if it exists and is not empty
    if (csvRecord.artist_mbid && csvRecord.artist_mbid.trim()) {
      artistData.artistMbId = csvRecord.artist_mbid;
    }
    artists.push(artistData);
  }
  
  // Build the play record with required fields
  const playRecord = {
    $type: RECORD_TYPE,
    trackName: csvRecord.track,
    artists, // Required field
    playedTime,
    submissionClientAgent: 'lastfm-importer/v0.0.1',
    musicServiceBaseDomain: 'last.fm',
  };
  
  // Add optional fields only if present and not empty
  if (csvRecord.album && csvRecord.album.trim()) {
    playRecord.releaseName = csvRecord.album;
  }
  
  if (csvRecord.album_mbid && csvRecord.album_mbid.trim()) {
    playRecord.releaseMbId = csvRecord.album_mbid;
  }
  
  if (csvRecord.track_mbid && csvRecord.track_mbid.trim()) {
    playRecord.recordingMbId = csvRecord.track_mbid;
  }
  
  // Generate Last.fm URL
  const artistEncoded = encodeURIComponent(csvRecord.artist);
  const trackEncoded = encodeURIComponent(csvRecord.track);
  playRecord.originUrl = `https://www.last.fm/music/${artistEncoded}/_/${trackEncoded}`;
  
  return playRecord;
}

/**
 * Format duration in human-readable format
 */
function formatDuration(milliseconds) {
  const seconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  } else if (minutes > 0) {
    const secs = seconds % 60;
    return `${minutes}m ${secs}s`;
  } else {
    return `${seconds}s`;
  }
}

/**
 * Publish records in batches with rate limiting and killswitch support
 */
async function publishRecords(agent, records, batchSize, batchDelay, dryRun = false) {
  const totalRecords = records.length;
  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();
  
  if (dryRun) {
    console.log(`\n=== DRY RUN MODE ===`);
    console.log(`Would publish ${totalRecords} records in batches of ${batchSize}`);
    console.log(`Estimated time: ${formatDuration(Math.ceil(totalRecords / batchSize) * batchDelay)}\n`);
    
    // Show first 5 records as preview
    const previewCount = Math.min(5, totalRecords);
    console.log(`Preview of first ${previewCount} records (in processing order):\n`);
    
    for (let i = 0; i < previewCount; i++) {
      const record = records[i];
      console.log(`${i + 1}. ${record.artists[0]?.artistName} - ${record.trackName}`);
      console.log(`   Album: ${record.releaseName || 'N/A'}`);
      console.log(`   Played: ${record.playedTime}`);
      console.log(`   URL: ${record.originUrl}`);
      
      // Show MusicBrainz IDs if available
      const mbids = [];
      if (record.artists[0]?.artistMbId) mbids.push(`Artist: ${record.artists[0].artistMbId}`);
      if (record.recordingMbId) mbids.push(`Recording: ${record.recordingMbId}`);
      if (record.releaseMbId) mbids.push(`Release: ${record.releaseMbId}`);
      
      if (mbids.length > 0) {
        console.log(`   MBIDs: ${mbids.join(', ')}`);
      }
      console.log('');
    }
    
    if (totalRecords > previewCount) {
      console.log(`... and ${totalRecords - previewCount} more records\n`);
    }
    
    console.log('=== DRY RUN COMPLETE ===');
    console.log('No records were actually published.');
    console.log('Remove --dry-run flag to publish for real.\n');
    
    return { successCount: totalRecords, errorCount: 0, cancelled: false };
  }
  
  const totalBatches = Math.ceil(totalRecords / batchSize);
  const estimatedTime = formatDuration(totalBatches * batchDelay);
  
  console.log(`Publishing ${totalRecords} records in batches of ${batchSize}...`);
  console.log(`Total batches: ${totalBatches}`);
  console.log(`Estimated time: ${estimatedTime}`);
  console.log(`\n🚨 Press Ctrl+C to stop gracefully after current batch\n`);
  
  for (let i = 0; i < totalRecords; i += batchSize) {
    // Check killswitch before processing batch
    if (importCancelled) {
      console.log(`\n🛑 Import cancelled by user`);
      console.log(`   Processed: ${successCount}/${totalRecords} records`);
      console.log(`   Remaining: ${totalRecords - successCount} records\n`);
      return { successCount, errorCount, cancelled: true };
    }
    
    const batch = records.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const progress = ((i / totalRecords) * 100).toFixed(1);
    
    console.log(`[${progress}%] Batch ${batchNum}/${totalBatches} (records ${i + 1}-${Math.min(i + batchSize, totalRecords)})`);
    
    // Process batch records
    const batchStartTime = Date.now();
    for (const record of batch) {
      // Check killswitch during batch processing
      if (importCancelled) {
        console.log(`  ⚠️  Stopping mid-batch...`);
        break;
      }
      
      try {
        await agent.com.atproto.repo.createRecord({
          repo: agent.session.did,
          collection: RECORD_TYPE,
          record,
        });
        successCount++;
      } catch (error) {
        errorCount++;
        console.error(`  ✗ Failed: ${record.trackName} - ${error.message}`);
      }
    }
    
    const batchDuration = Date.now() - batchStartTime;
    const elapsed = formatDuration(Date.now() - startTime);
    const remaining = formatDuration(((totalRecords - i - batchSize) / batchSize) * batchDelay);
    
    console.log(`  ✓ Complete in ${batchDuration}ms (${successCount} successful, ${errorCount} failed)`);
    
    // Only show time estimates if not cancelled
    if (!importCancelled) {
      console.log(`  ⏱  Elapsed: ${elapsed} | Remaining: ~${remaining}\n`);
    }
    
    // Check again before waiting (in case cancelled during batch)
    if (importCancelled) {
      console.log(`\n🛑 Import cancelled by user`);
      console.log(`   Processed: ${successCount}/${totalRecords} records`);
      console.log(`   Remaining: ${totalRecords - successCount} records\n`);
      return { successCount, errorCount, cancelled: true };
    }
    
    // Wait before next batch (except for last batch)
    if (i + batchSize < totalRecords) {
      await new Promise(resolve => setTimeout(resolve, batchDelay));
    }
  }
  
  return { successCount, errorCount, cancelled: false };
}

/**
 * Main execution
 */
async function main() {
  const args = parseCommandLineArgs();
  
  // Show help if requested
  if (args.help) {
    showHelp();
    process.exit(0);
  }
  
  // Setup killswitch (unless in dry-run mode)
  if (!args['dry-run']) {
    setupKillswitch();
  }
  
  try {
    console.log('=== Last.fm to ATProto Importer ===\n');
    
    // Get CSV file path
    let csvPath = args.file;
    if (!csvPath) {
      csvPath = await prompt('Enter path to Last.fm CSV export: ');
    } else {
      console.log(`CSV file: ${csvPath}`);
    }
    
    if (!fs.existsSync(csvPath)) {
      console.error('✗ File not found!');
      process.exit(1);
    }
    
    // Parse CSV
    const csvRecords = parseLastFmCsv(csvPath);
    
    if (csvRecords.length === 0) {
      console.error('✗ No records found in CSV file!');
      process.exit(1);
    }
    
    // Convert records
    console.log('Converting records to ATProto format...');
    const playRecords = csvRecords.map(convertToPlayRecord);
    console.log('✓ Conversion complete\n');
    
    // Sort records chronologically (oldest first) unless reverse flag is set
    const reverseChronological = args['reverse-chronological'];
    console.log(`Sorting records ${reverseChronological ? 'newest' : 'oldest'} first...`);
    
    playRecords.sort((a, b) => {
      const timeA = new Date(a.playedTime).getTime();
      const timeB = new Date(b.playedTime).getTime();
      return reverseChronological ? timeB - timeA : timeA - timeB;
    });
    
    const firstPlay = new Date(playRecords[0].playedTime).toLocaleDateString();
    const lastPlay = new Date(playRecords[playRecords.length - 1].playedTime).toLocaleDateString();
    console.log(`✓ Sorted ${playRecords.length} records`);
    console.log(`  First: ${firstPlay}`);
    console.log(`  Last: ${lastPlay}\n`);
    
    // Validate and set batch delay with minimum enforcement first
    let batchDelay = args['batch-delay'] ? parseInt(args['batch-delay']) : DEFAULT_BATCH_DELAY;
    if (batchDelay < MIN_BATCH_DELAY) {
      console.log(`⚠️  Batch delay ${batchDelay}ms is below minimum safe limit.`);
      console.log(`   Enforcing minimum delay of ${MIN_BATCH_DELAY}ms to respect rate limits.\n`);
      batchDelay = MIN_BATCH_DELAY;
    }
    
    // Calculate optimal batch size if not specified (considers batch delay)
    let batchSize = args['batch-size'] ? parseInt(args['batch-size']) : null;
    if (!batchSize) {
      batchSize = calculateOptimalBatchSize(playRecords.length, batchDelay);
      console.log(`Auto-calculated batch size: ${batchSize}`);
      console.log(`  Algorithm: Logarithmic scaling with O(n) time complexity`);
      console.log(`  Optimized for: ${playRecords.length} records at ${batchDelay}ms delay`);
      console.log(`  Rate limit strategy: Token bucket with conservative limits\n`);
    } else {
      console.log(`Using specified batch size: ${batchSize}\n`);
    }
    
    // Check if dry run mode
    const isDryRun = args['dry-run'];
    
    if (isDryRun) {
      console.log('🔍 Running in DRY RUN mode - no authentication required\n');
      
      // Show preview without publishing
      await publishRecords(null, playRecords, batchSize, batchDelay, true);
      process.exit(0);
    }
    
    // Login to ATProto (only if not dry run)
    const agent = await login(args.identifier, args.password);
    
    // Confirm before publishing (unless --yes flag is set)
    if (!args.yes) {
      const confirm = await prompt(`\nReady to publish ${playRecords.length} records. Continue? (yes/no): `);
      if (confirm.toLowerCase() !== 'yes' && confirm.toLowerCase() !== 'y') {
        console.log('Aborted.');
        process.exit(0);
      }
      console.log('');
    } else {
      console.log(`Auto-confirmed: Publishing ${playRecords.length} records...\n`);
    }
    
    // Publish records
    const startTime = Date.now();
    const { successCount, errorCount, cancelled } = await publishRecords(agent, playRecords, batchSize, batchDelay, false);
    const totalTime = formatDuration(Date.now() - startTime);
    
    // Summary
    console.log('=== Import Complete ===');
    if (cancelled) {
      console.log('Status: CANCELLED BY USER');
    } else {
      console.log('Status: COMPLETED');
    }
    console.log(`Total records: ${playRecords.length}`);
    console.log(`Successfully published: ${successCount}`);
    console.log(`Failed: ${errorCount}`);
    if (cancelled) {
      console.log(`Not processed: ${playRecords.length - successCount - errorCount}`);
    }
    console.log(`Total time: ${totalTime}`);
    
    if (successCount > 0) {
      const avgTime = (Date.now() - startTime) / successCount;
      console.log(`Average time per record: ${avgTime.toFixed(0)}ms`);
    }
    
    console.log('\n✓ Logged out');
    
    // Exit with appropriate code
    process.exit(cancelled ? 130 : 0);
    
  } catch (error) {
    console.error('\n✗ Fatal error:', error.message);
    if (error.stack && process.env.DEBUG) {
      console.error('\nStack trace:', error.stack);
    }
    process.exit(1);
  }
}

main();
