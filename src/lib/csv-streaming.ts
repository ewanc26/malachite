import * as fs from 'fs';
import { parse } from 'csv-parse';
import type { LastFmCsvRecord, PlayRecord, Config } from '../types.js';
import { StreamDB } from '../utils/stream-db.js';
import { log } from '../utils/logger.js';
import { normalizeColumns, convertToPlayRecord } from '../utils/csv-helpers.js';

/**
 * Streaming CSV parser that processes records without loading everything into memory
 */
export async function parseLastFmCsvStreaming(
  filePath: string,
  db: StreamDB,
  config: Config,
  debug = false
): Promise<{ total: number; inserted: number; duplicates: number }> {
  log.info(`Reading CSV file (streaming): ${filePath}`);
  
  return new Promise((resolve, reject) => {
    let total = 0;
    let inserted = 0;
    let duplicates = 0;
    let batch: PlayRecord[] = [];
    const BATCH_SIZE = 1000; // Process in batches of 1000
    
    const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
    
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_quotes: true,
      relax_column_count: true,
      cast: false,
    });
    
    parser.on('readable', async function() {
      let record;
      while ((record = parser.read()) !== null) {
        try {
          // Normalize and validate
          const normalized = normalizeColumns(record);
          if (!normalized.artist || !normalized.track || !normalized.uts) {
            continue; // Skip invalid records
          }
          
          // Convert to PlayRecord
          const playRecord = convertToPlayRecord(normalized, config, debug);
          batch.push(playRecord);
          total++;
          
          // Process batch when full
          if (batch.length >= BATCH_SIZE) {
            const batchInserted = db.insertMany(batch);
            inserted += batchInserted;
            duplicates += batch.length - batchInserted;
            
            // Log progress
            if (total % 10000 === 0) {
              log.progress(`Processed ${total.toLocaleString()} records...`);
            }
            
            batch = [];
          }
        } catch (error) {
          log.warn(`Skipping invalid record: ${error}`);
        }
      }
    });
    
    parser.on('error', (error) => {
      reject(error);
    });
    
    parser.on('end', () => {
      // Process remaining batch
      if (batch.length > 0) {
        const batchInserted = db.insertMany(batch);
        inserted += batchInserted;
        duplicates += batch.length - batchInserted;
      }
      
      log.success(`✓ Parsed ${total.toLocaleString()} records (${inserted.toLocaleString()} unique, ${duplicates.toLocaleString()} duplicates)`);
      resolve({ total, inserted, duplicates });
    });
    
    fileStream.pipe(parser);
  });
}
