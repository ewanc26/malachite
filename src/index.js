#!/usr/bin/env node

import * as fs from 'fs';
import * as config from './config.js';
import { parseCommandLineArgs, showHelp } from './lib/cli.js';
import { login } from './lib/auth.js';
import { parseLastFmCsv, convertToPlayRecord, sortRecords } from './lib/csv.js';
import { publishRecords } from './lib/publisher.js';
import { prompt } from './utils/input.js';
import { formatDuration, calculateOptimalBatchSize } from './utils/helpers.js';
import { setupKillswitch } from './utils/killswitch.js';

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
    const playRecords = csvRecords.map(record => convertToPlayRecord(record, config));
    console.log('✓ Conversion complete\n');
    
    // Sort records chronologically
    const reverseChronological = args['reverse-chronological'];
    sortRecords(playRecords, reverseChronological);
    
    // Validate and set batch delay
    let batchDelay = args['batch-delay'] ? parseInt(args['batch-delay']) : config.DEFAULT_BATCH_DELAY;
    if (batchDelay < config.MIN_BATCH_DELAY) {
      console.log(`⚠️  Batch delay ${batchDelay}ms is below minimum safe limit.`);
      console.log(`   Enforcing minimum delay of ${config.MIN_BATCH_DELAY}ms to respect rate limits.\n`);
      batchDelay = config.MIN_BATCH_DELAY;
    }
    
    // Calculate optimal batch size
    let batchSize = args['batch-size'] ? parseInt(args['batch-size']) : null;
    if (!batchSize) {
      batchSize = calculateOptimalBatchSize(playRecords.length, batchDelay, config);
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
      await publishRecords(null, playRecords, batchSize, batchDelay, config, true);
      process.exit(0);
    }
    
    // Login to ATProto (only if not dry run)
    const agent = await login(args.identifier, args.password, config.SLINGSHOT_RESOLVER);
    
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
    const { successCount, errorCount, cancelled } = await publishRecords(
      agent,
      playRecords,
      batchSize,
      batchDelay,
      config,
      false
    );
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
