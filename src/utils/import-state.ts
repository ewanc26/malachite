import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import type { PlayRecord } from '../types.js';
import { log } from './logger.js';
import { getMalachiteStateDir } from './platform.js';

/**
 * Import state for resume functionality
 */
export interface ImportState {
  version: string;
  startedAt: string;
  lastUpdatedAt: string;
  inputFile: string;
  inputFileHash: string;
  totalRecords: number;
  processedRecords: number;
  successfulRecords: number;
  failedRecords: number;
  lastSuccessfulIndex: number;
  mode: 'lastfm' | 'spotify' | 'combined' | 'sync';
  completed: boolean;
}

/**
 * Get the state file path for an import
 */
export function getStateFilePath(inputFile: string, mode: string): string {
  const stateDir = path.join(getMalachiteStateDir(), 'state');
  
  // Create state directory if it doesn't exist
  if (!fs.existsSync(stateDir)) {
    fs.mkdirSync(stateDir, { recursive: true });
  }
  
  // Create a unique filename based on input file and mode
  const hash = crypto
    .createHash('md5')
    .update(inputFile + mode)
    .digest('hex')
    .substring(0, 8);
  
  return path.join(stateDir, `import-${hash}.json`);
}

/**
 * Calculate hash of input file for change detection
 */
export function calculateFileHash(filePath: string): string {
  if (!fs.existsSync(filePath)) {
    return '';
  }
  
  const stats = fs.statSync(filePath);
  if (stats.isDirectory()) {
    // For directories (like Spotify exports), hash the directory name and modification time
    return crypto
      .createHash('md5')
      .update(filePath + stats.mtime.toISOString())
      .digest('hex');
  }
  
  // For files, use file size and modification time for quick comparison
  return crypto
    .createHash('md5')
    .update(`${stats.size}-${stats.mtime.toISOString()}`)
    .digest('hex');
}

/**
 * Load import state from disk
 */
export function loadImportState(inputFile: string, mode: string): ImportState | null {
  const stateFile = getStateFilePath(inputFile, mode);
  
  if (!fs.existsSync(stateFile)) {
    return null;
  }
  
  try {
    const data = fs.readFileSync(stateFile, 'utf-8');
    const state = JSON.parse(data) as ImportState;
    
    // Check if input file has changed
    const currentHash = calculateFileHash(inputFile);
    if (state.inputFileHash !== currentHash) {
      log.warn('Input file has changed since last import - starting fresh');
      return null;
    }
    
    return state;
  } catch (error) {
    log.warn('Failed to load state file - starting fresh');
    return null;
  }
}

/**
 * Save import state to disk
 */
export function saveImportState(state: ImportState): void {
  const stateFile = getStateFilePath(state.inputFile, state.mode);
  
  try {
    state.lastUpdatedAt = new Date().toISOString();
    fs.writeFileSync(stateFile, JSON.stringify(state, null, 2), 'utf-8');
  } catch (error) {
    log.error('Failed to save state file - progress may be lost on restart');
  }
}

/**
 * Create initial import state
 */
export function createImportState(
  inputFile: string,
  mode: 'lastfm' | 'spotify' | 'combined' | 'sync',
  totalRecords: number
): ImportState {
  return {
    version: '1.0',
    startedAt: new Date().toISOString(),
    lastUpdatedAt: new Date().toISOString(),
    inputFile,
    inputFileHash: calculateFileHash(inputFile),
    totalRecords,
    processedRecords: 0,
    successfulRecords: 0,
    failedRecords: 0,
    lastSuccessfulIndex: -1,
    mode,
    completed: false,
  };
}

/**
 * Update import state after batch
 */
export function updateImportState(
  state: ImportState,
  batchIndex: number,
  successCount: number,
  errorCount: number
): void {
  state.processedRecords += successCount + errorCount;
  state.successfulRecords += successCount;
  state.failedRecords += errorCount;
  
  if (successCount > 0) {
    state.lastSuccessfulIndex = batchIndex;
  }
  
  saveImportState(state);
}

/**
 * Mark import as completed
 */
export function completeImport(state: ImportState): void {
  state.completed = true;
  saveImportState(state);
}

/**
 * Clear import state (for fresh start)
 */
export function clearImportState(inputFile: string, mode: string): void {
  const stateFile = getStateFilePath(inputFile, mode);
  
  if (fs.existsSync(stateFile)) {
    fs.unlinkSync(stateFile);
  }
}

/**
 * Display resume information
 */
export function displayResumeInfo(state: ImportState): void {
  const elapsed = Date.now() - new Date(state.startedAt).getTime();
  const elapsedHours = Math.floor(elapsed / (1000 * 60 * 60));
  const elapsedMinutes = Math.floor((elapsed % (1000 * 60 * 60)) / (1000 * 60));
  
  const remaining = state.totalRecords - state.processedRecords;
  const progress = ((state.processedRecords / state.totalRecords) * 100).toFixed(1);
  
  log.section('Resuming Previous Import');
  log.info(`Started: ${new Date(state.startedAt).toLocaleString()}`);
  log.info(`Progress: ${state.processedRecords.toLocaleString()}/${state.totalRecords.toLocaleString()} (${progress}%)`);
  log.info(`Successful: ${state.successfulRecords.toLocaleString()}`);
  
  if (state.failedRecords > 0) {
    log.warn(`Failed: ${state.failedRecords.toLocaleString()}`);
  }
  
  log.info(`Remaining: ${remaining.toLocaleString()} records`);
  
  if (elapsedHours > 0) {
    log.info(`Time elapsed: ${elapsedHours}h ${elapsedMinutes}m`);
  } else if (elapsedMinutes > 0) {
    log.info(`Time elapsed: ${elapsedMinutes}m`);
  }
  
  log.blank();
}

/**
 * Filter records to skip already processed ones
 */
export function filterUnprocessedRecords(
  records: PlayRecord[],
  state: ImportState
): PlayRecord[] {
  if (state.lastSuccessfulIndex < 0) {
    return records;
  }
  
  // Skip records up to and including the last successful index
  const startIndex = state.lastSuccessfulIndex + 1;
  
  if (startIndex >= records.length) {
    return [];
  }
  
  return records.slice(startIndex);
}

/**
 * Get the starting index for resume
 */
export function getResumeStartIndex(state: ImportState): number {
  return state.lastSuccessfulIndex + 1;
}
