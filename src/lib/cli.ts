import { parseArgs } from 'node:util';
import { AtpAgent } from '@atproto/api'; // Use AtpAgent for consistency
import type { PlayRecord, Config, CommandLineArgs, PublishResult } from '../types.js'; 
import { login } from './auth.js'; 
import { parseLastFmCsv, convertToPlayRecord, sortRecords } from '../lib/csv.js'; 
import { publishRecords } from './publisher.js'; 
import { prompt } from '../utils/input.js'; 
import config from '../config.js'; 
import { calculateOptimalBatchSize, showRateLimitInfo } from '../utils/helpers.js'; 

/**
 * Show help message
 */
export function showHelp(): void {
    console.log(`
Last.fm to ATProto Importer v0.0.2

Usage: npm start [options]

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
        let agent: AtpAgent | null = null;

        // 1. Get Authentication (skips login if dry-run)
        if (!dryRun) {
            if (!args.identifier || !args.password) {
                throw new Error('Missing required arguments for login: -i (identifier) and -p (password)');
            }
            // Assume login returns AtpAgent, as per the type fix
            agent = await login(args.identifier, args.password, cfg.SLINGSHOT_RESOLVER) as AtpAgent; 
        }

        // 2. Parse and Prepare Records
        // This function is assumed to read the file path in args.file
        const csvRecords = parseLastFmCsv(args.file); 
        
        // This function maps the raw CSV records to the standardized PlayRecord structure
        const records: PlayRecord[] = csvRecords.map(record => convertToPlayRecord(record, cfg));
        const totalRecords = records.length;
        
        const reverseChronological = args['reverse-chronological'] ?? false;
        const sortedRecords = sortRecords(records, reverseChronological); 
        
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
            console.log(`\nReady to publish ${totalRecords.toLocaleString()} records.`);
            const answer = await prompt('Do you want to continue? (y/N) ');
            if (answer.toLowerCase() !== 'y') {
                console.log('Import cancelled by user.');
                process.exit(0);
            }
        }
        
        // 6. Publish Records
        const result: PublishResult = await publishRecords(
            agent, 
            sortedRecords,
            batchSize,
            batchDelay,
            cfg,
            dryRun
        );

        // 7. Final Output
        if (result.cancelled) {
            console.log(`\nImport stopped gracefully. ${result.successCount} records processed.`);
        } else if (dryRun) {
            console.log('\nDRY RUN COMPLETE. No records were published.');
        } else {
            console.log(`\n🎉 Import Complete!`);
            console.log(`Total records processed: ${result.successCount.toLocaleString()} (${result.errorCount.toLocaleString()} failed)`);
        }

    } catch (error) {
        // Handle fatal errors
        const err = error as Error;
        console.error('\n🛑 A fatal error occurred:');
        console.error(err.message);
        process.exit(1);
    }
}