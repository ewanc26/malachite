import Database from 'better-sqlite3';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import type { PlayRecord } from '../types.js';
import { log } from './logger.js';

/**
 * Streaming database for low-memory record processing
 * Uses SQLite as temporary storage instead of keeping everything in memory
 */
export class StreamDB {
  private db: Database.Database;
  private dbPath: string;
  private insertStmt: Database.Statement;
  private selectStmt: Database.Statement;
  private checkExistsStmt: Database.Statement;
  private countStmt: Database.Statement;
  
  constructor(sessionId: string) {
    // Create temp database file
    const tempDir = path.join(os.tmpdir(), 'malachite');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    this.dbPath = path.join(tempDir, `${sessionId}.db`);
    
    // Remove old database if exists
    if (fs.existsSync(this.dbPath)) {
      fs.unlinkSync(this.dbPath);
    }
    
    log.debug(`Creating temporary database: ${this.dbPath}`);
    this.db = new Database(this.dbPath);
    
    // Enable WAL mode for better performance
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('cache_size = -64000'); // 64MB cache
    
    // Create tables
    this.createTables();
    
    // Prepare statements
    this.insertStmt = this.db.prepare(`
      INSERT INTO records (key, artist, track, timestamp, data)
      VALUES (?, ?, ?, ?, ?)
    `);
    
    this.selectStmt = this.db.prepare(`
      SELECT data FROM records ORDER BY timestamp ASC LIMIT ? OFFSET ?
    `);
    
    this.checkExistsStmt = this.db.prepare(`
      SELECT 1 FROM records WHERE key = ? LIMIT 1
    `);
    
    this.countStmt = this.db.prepare(`
      SELECT COUNT(*) as count FROM records
    `);
  }
  
  private createTables(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL UNIQUE,
        artist TEXT NOT NULL,
        track TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        data TEXT NOT NULL
      );
      
      CREATE INDEX IF NOT EXISTS idx_key ON records(key);
      CREATE INDEX IF NOT EXISTS idx_timestamp ON records(timestamp);
    `);
  }
  
  /**
   * Create a unique key for a play record
   */
  private createRecordKey(record: PlayRecord): string {
    const artist = record.artists[0]?.artistName || '';
    const track = record.trackName;
    const timestamp = record.playedTime;
    
    const normalizedArtist = artist.toLowerCase().trim();
    const normalizedTrack = track.toLowerCase().trim();
    
    return `${normalizedArtist}|||${normalizedTrack}|||${timestamp}`;
  }
  
  /**
   * Insert a record into the database
   * Returns true if inserted, false if duplicate
   */
  insert(record: PlayRecord): boolean {
    const key = this.createRecordKey(record);
    const artist = record.artists[0]?.artistName || '';
    const track = record.trackName;
    const timestamp = record.playedTime;
    const data = JSON.stringify(record);
    
    try {
      this.insertStmt.run(key, artist, track, timestamp, data);
      return true;
    } catch (error: any) {
      // Duplicate key error
      if (error.code === 'SQLITE_CONSTRAINT') {
        return false;
      }
      throw error;
    }
  }
  
  /**
   * Insert multiple records in a transaction
   * Returns count of inserted records (skips duplicates)
   */
  insertMany(records: PlayRecord[]): number {
    let inserted = 0;
    
    const insertMany = this.db.transaction((records: PlayRecord[]) => {
      for (const record of records) {
        if (this.insert(record)) {
          inserted++;
        }
      }
    });
    
    insertMany(records);
    return inserted;
  }
  
  /**
   * Check if a record exists
   */
  exists(record: PlayRecord): boolean {
    const key = this.createRecordKey(record);
    const result = this.checkExistsStmt.get(key);
    return result !== undefined;
  }
  
  /**
   * Get total record count
   */
  count(): number {
    const result = this.countStmt.get() as { count: number };
    return result.count;
  }
  
  /**
   * Get records in batches (streaming)
   * Returns a generator that yields batches of records
   */
  *getBatches(batchSize: number): Generator<PlayRecord[], void, unknown> {
    const totalRecords = this.count();
    let offset = 0;
    
    while (offset < totalRecords) {
      const rows = this.selectStmt.all(batchSize, offset) as Array<{ data: string }>;
      const records = rows.map(row => JSON.parse(row.data) as PlayRecord);
      
      yield records;
      offset += batchSize;
    }
  }
  
  /**
   * Get all records as an array (use sparingly - defeats the purpose of streaming)
   */
  getAll(): PlayRecord[] {
    const rows = this.db.prepare('SELECT data FROM records ORDER BY timestamp ASC')
      .all() as Array<{ data: string }>;
    
    return rows.map(row => JSON.parse(row.data) as PlayRecord);
  }
  
  /**
   * Close the database and clean up
   */
  close(): void {
    this.db.close();
    
    // Remove temp database file
    if (fs.existsSync(this.dbPath)) {
      try {
        fs.unlinkSync(this.dbPath);
        log.debug('Temporary database cleaned up');
      } catch (error) {
        log.warn(`Failed to remove temporary database: ${this.dbPath}`);
      }
    }
  }
  
  /**
   * Optimize database (run periodically for large imports)
   */
  optimize(): void {
    this.db.exec('VACUUM');
    this.db.exec('ANALYZE');
  }
}

/**
 * Create a unique session ID for temporary database
 */
export function createSessionId(): string {
  return `malachite-${Date.now()}-${Math.random().toString(36).substring(7)}`;
}
