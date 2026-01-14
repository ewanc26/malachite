import { existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';

/**
 * Cache configuration
 */
const CACHE_VERSION = 1;
const CACHE_TTL_HOURS = 24; // Cache validity period
const CACHE_DIR = join(homedir(), '.malachite', 'cache');

/**
 * Cache file structure
 */
interface CacheFile {
  version: number;
  did: string;
  timestamp: number;
  records: Array<[string, { uri: string; cid: string; value: any }]>;
}

/**
 * Ensure cache directory exists
 */
function ensureCacheDir(): void {
  if (!existsSync(CACHE_DIR)) {
    mkdirSync(CACHE_DIR, { recursive: true });
  }
}

/**
 * Get cache file path for a DID
 */
function getCachePath(did: string): string {
  // Sanitize DID for use in filename
  const sanitized = did.replace(/[^a-zA-Z0-9.-]/g, '_');
  return join(CACHE_DIR, `${sanitized}.json`);
}

/**
 * Check if cache exists and is valid for a given DID
 */
export function isCacheValid(did: string): boolean {
  const cachePath = getCachePath(did);
  
  if (!existsSync(cachePath)) {
    return false;
  }
  
  try {
    const data = readFileSync(cachePath, 'utf-8');
    const cache: CacheFile = JSON.parse(data);
    
    // Check version
    if (cache.version !== CACHE_VERSION) {
      return false;
    }
    
    // Check DID match
    if (cache.did !== did) {
      return false;
    }
    
    // Check age
    const ageHours = (Date.now() - cache.timestamp) / (1000 * 60 * 60);
    if (ageHours > CACHE_TTL_HOURS) {
      return false;
    }
    
    return true;
  } catch {
    // Invalid cache file
    return false;
  }
}

/**
 * Load cached records for a given DID
 * Returns null if cache doesn't exist or is invalid
 */
export function loadCache(did: string): Map<string, { uri: string; cid: string; value: any }> | null {
  const cachePath = getCachePath(did);
  
  if (!isCacheValid(did)) {
    return null;
  }
  
  try {
    const data = readFileSync(cachePath, 'utf-8');
    const cache: CacheFile = JSON.parse(data);
    
    // Convert array back to Map
    return new Map(cache.records);
  } catch {
    return null;
  }
}

/**
 * Save records to cache for a given DID
 */
export function saveCache(did: string, records: Map<string, { uri: string; cid: string; value: any }>): void {
  ensureCacheDir();
  
  const cache: CacheFile = {
    version: CACHE_VERSION,
    did,
    timestamp: Date.now(),
    records: Array.from(records.entries()),
  };
  
  const cachePath = getCachePath(did);
  writeFileSync(cachePath, JSON.stringify(cache), 'utf-8');
}

/**
 * Get cache information (age and record count)
 */
export function getCacheInfo(did: string): { age?: number; records?: number } {
  const cachePath = getCachePath(did);
  
  if (!existsSync(cachePath)) {
    return {};
  }
  
  try {
    const data = readFileSync(cachePath, 'utf-8');
    const cache: CacheFile = JSON.parse(data);
    
    const ageHours = (Date.now() - cache.timestamp) / (1000 * 60 * 60);
    
    return {
      age: ageHours,
      records: cache.records.length,
    };
  } catch {
    return {};
  }
}

/**
 * Clear cache for a specific DID
 */
export function clearCache(did: string): void {
  const cachePath = getCachePath(did);
  
  if (existsSync(cachePath)) {
    unlinkSync(cachePath);
  }
}

/**
 * Clear all caches
 */
export function clearAllCaches(): void {
  if (!existsSync(CACHE_DIR)) {
    return;
  }
  
  const files = readdirSync(CACHE_DIR);
  for (const file of files) {
    if (file.endsWith('.json')) {
      unlinkSync(join(CACHE_DIR, file));
    }
  }
}
