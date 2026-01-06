import { parseArgs } from 'node:util';
import { AtpAgent } from '@atproto/api'; // Use AtpAgent for consistency
import type { PlayRecord, Config, CommandLineArgs, PublishResult } from '../types.js'; 
import { login } from './auth.js'; 
import { parseLastFmCsv, convertToPlayRecord, sortRecords } from '../lib/csv.js';
import { parseSpotifyJson, convertSpotifyToPlayRecord, sortSpotifyRecords } from '../lib/spotify.js'; 
import { publishRecordsWithApplyWrites } from './publisher.js'; 
import { prompt } from '../utils/input.js'; 
import config from '../config.js'; 
import { calculateOptimalBatchSize, showRateLimitInfo } from '../utils/helpers.js';
import { fetchExistingRecords, filterNewRecords, displaySyncStats, removeDuplicates } from './sync.js'; 

/**
 * Show help message
 */
export function showHelp(): void {
    console.log(`
Last.fm to ATProto Importer v0.3.0

Usage: npm start [options]

Options:
  -h, --help                     Show this help message
  -f, --file <path>              Path to Last.fm CSV or Spotify JSON export file/directory
  -i, --identifier <id>          ATProto handle or DID
  -p, --password <pass>          ATProto app password
  -b, --batch-size <num>         Number of records per batch (auto-calculated if not set)
  -d, --batch-delay <ms>         Delay between batches in ms (default: 500, min: 500)
  -y, --yes                      Skip confirmation prompt
  -n, --dry-run                  Preview records without publishing
  -r, --reverse-chronological    Process newest first (default: oldest first)
  -s, --sync                     Re-sync mode: check existing Teal records and only import new ones
  --spotify                      Import from Spotify JSON export instead of Last.fm CSV
  --remove-duplicates            Remove duplicate records from Teal (keeps first occurrence)
`);
}

/**
 * Parse command line arguments
 */
export function parseCommandLineArgs(): CommandLineArgs {
    // The options definition is identical to the CommandLineArgs keys
    const options = {
        help: { type: 'boolean', short: 'h', default: false },
        file: { type: 'string', short: 'f' },
        identifier: { type: 'string', short: 'i' },
        password: { type: 'string', short: 'p' },
        'batch-size': { type: 'string', short: 'b' },
        'batch-delay': { type: 'string', short: 'd' },
        yes: { type: 'boolean', short: 'y', default: false },
        'dry-run': { type: 'boolean', short: 'n', default: false },
        'reverse-chronological': { type: 'boolean', short: 'r', default: false },
        sync: { type: 'boolean', short: 's', default: false },
        spotify: { type: 'boolean', default: false },
        'remove-duplicates': { type: 'boolean', default: false },
    } as const; 
    
    try {
        const { values } = parseArgs({ options, allowPositionals: false });
        return values as CommandLineArgs; 
    } catch (error) {
        const err = error as Error;
        console.error('Error parsing arguments:', err.message);
        showHelp();
        process.exit(1);
    }
}

/**
 * The full, real implementation of the CLI
 */
export async function runCLI(): Promise<void> {
    try {
        const args = parseCommandLineArgs();
        const cfg = config as Config; // Use a constant for the typed config

        if (args.help) {
            showHelp();
            return;
        }

        if (!args.file) {
            throw new Error('Missing required argument: -f, --file <path>');
        }

        const dryRun = args['dry-run'] ?? false;
        const syncMode = args.sync ?? false;
        const removeDuplicatesMode = args['remove-duplicates'] ?? false;
        let agent: AtpAgent | null = null;

        // Remove duplicates mode - requires authentication but not file
        if (removeDuplicatesMode) {
            if (!args.identifier || !args.password) {
                throw new Error('Missing required arguments for login: -i (identifier) and -p (password)');
            }
            
            agent = await login(args.identifier, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent;
            
            // Check for duplicates first
            const result = await removeDuplicates(agent, cfg, true); // Always dry-run first to show info
            
            if (result.totalDuplicates === 0) {
                return; // No duplicates, exit early
            }
            
            // Ask for confirmation if not in dry-run mode
            if (!dryRun && !(args.yes ?? false)) {
                console.log(`⚠️  WARNING: This will permanently delete ${result.totalDuplicates} duplicate records from Teal.`);
                console.log('   The first occurrence of each duplicate will be kept.\n');
                const answer = await prompt('Are you sure you want to continue? (y/N) ');
                if (answer.toLowerCase() !== 'y') {
                    console.log('Duplicate removal cancelled by user.');
                    process.exit(0);
                }
                
                // Actually remove duplicates
                await removeDuplicates(agent, cfg, false);
                console.log('🎉 Duplicate removal complete!\n');
            } else if (dryRun) {
                console.log('DRY RUN: No records were actually removed.\n');
                console.log('Remove --dry-run flag to actually delete duplicates.\n');
            }
            
            return;
        }

        // 1. Get Authentication (required for sync mode, even in dry-run)
        if (!dryRun || syncMode) {
            if (!args.identifier || !args.password) {
                throw new Error('Missing required arguments for login: -i (identifier) and -p (password)');
            }
            // Assume login returns AtpAgent, as per the type fix
            agent = await login(args.identifier, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent; 
        }

        // 2. Parse and Prepare Records
        const useSpotify = args.spotify ?? false;
        let records: PlayRecord[];
        let rawRecordCount: number;
        
        if (useSpotify) {
            console.log('📀 Importing from Spotify export...\n');
            const spotifyRecords = parseSpotifyJson(args.file);
            rawRecordCount = spotifyRecords.length;
            records = spotifyRecords.map(record => convertSpotifyToPlayRecord(record, cfg));
        } else {
            console.log('📀 Importing from Last.fm CSV export...\n');
            const csvRecords = parseLastFmCsv(args.file);
            rawRecordCount = csvRecords.length;
            records = csvRecords.map(record => convertToPlayRecord(record, cfg));
        }
        
        // 2.5. Sync Mode: Fetch existing records and filter duplicates
        if (syncMode && agent) {
            const originalRecords = [...records]; // Save before filtering
            const existingRecords = await fetchExistingRecords(agent, cfg);
            records = filterNewRecords(records, existingRecords);
            
            if (records.length === 0) {
                console.log('✓ All records already exist in Teal. Nothing to import!');
                process.exit(0);
            }
            
            displaySyncStats(originalRecords, existingRecords, records);
        }
        
        const totalRecords = records.length;
        
        const reverseChronological = args['reverse-chronological'] ?? false;
        const sortedRecords = useSpotify 
            ? sortSpotifyRecords(records, reverseChronological)
            : sortRecords(records, reverseChronological); 
        
        // 3. Determine Batching parameters
        let batchDelay = cfg.DEFAULT_BATCH_DELAY;
        if (args['batch-delay']) {
            const delay = parseInt(args['batch-delay'], 10);
            if (isNaN(delay)) {
                 throw new Error(`Invalid batch delay value: ${args['batch-delay']}`);
            }
            // Enforce minimum delay
            batchDelay = Math.max(delay, cfg.MIN_BATCH_DELAY); 
        }
        
        let batchSize: number;
        if (args['batch-size']) {
            batchSize = parseInt(args['batch-size'], 10);
            if (isNaN(batchSize) || batchSize <= 0) {
                 throw new Error(`Invalid batch size value: ${args['batch-size']}`);
            }
        } else {
            // Calculate optimal batch size if not provided
            batchSize = calculateOptimalBatchSize(totalRecords, batchDelay, cfg);
        }

        // 4. Show Rate Limiting Information
        const recordsPerDay = cfg.RECORDS_PER_DAY_LIMIT * cfg.SAFETY_MARGIN;
        const estimatedDays = Math.ceil(totalRecords / recordsPerDay);

        // Updated call to match the expected signature in showRateLimitInfo (from previous response)
        showRateLimitInfo(
            totalRecords, 
            batchSize, 
            batchDelay, 
            estimatedDays, 
            cfg.RECORDS_PER_DAY_LIMIT,
        );

        // 5. Confirmation Prompt
        if (!dryRun && !(args.yes ?? false)) {
            if (syncMode) {
                console.log(`\nReady to publish ${totalRecords.toLocaleString()} NEW records (${rawRecordCount - totalRecords} duplicates skipped).`);
            } else {
                console.log(`\nReady to publish ${totalRecords.toLocaleString()} records.`);
            }
            const answer = await prompt('Do you want to continue? (y/N) ');
            if (answer.toLowerCase() !== 'y') {
                console.log('Import cancelled by user.');
                process.exit(0);
            }
        }
        
        // 6. Publish Records
        const result: PublishResult = await publishRecordsWithApplyWrites(
            agent, 
            sortedRecords,
            batchSize,
            batchDelay,
            cfg,
            dryRun,
            syncMode
        );

        // 7. Final Output
        if (result.cancelled) {
            console.log(`\nImport stopped gracefully. ${result.successCount} records processed.`);
        } else if (dryRun) {
            console.log(`\nDRY RUN COMPLETE${syncMode ? ' (SYNC MODE)' : ''}. No records were published.`);
        } else {
            console.log(`\n🎉 ${syncMode ? 'Sync' : 'Import'} Complete!`);
            console.log(`Total records processed: ${result.successCount.toLocaleString()} (${result.errorCount.toLocaleString()} failed)`);
            if (syncMode) {
                console.log(`Duplicates skipped: ${rawRecordCount - totalRecords}`);
            }
        }

    } catch (error) {
        // Handle fatal errors
        const err = error as Error;
        console.error('\n🛑 A fatal error occurred:');
        console.error(err.message);
        process.exit(1);
    }
}