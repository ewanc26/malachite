import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { getMalachiteStateDir } from './platform.js';

/**
 * Stored credentials structure
 */
interface StoredCredentials {
  version: number;
  handle: string;
  encryptedPassword: string;
  iv: string;
  salt: string;
  machineFingerprint: string;
  createdAt: string;
  lastUsedAt: string;
}

/**
 * Get credentials file path
 */
function getCredentialsPath(): string {
  const credentialsDir = getMalachiteStateDir();
  if (!fs.existsSync(credentialsDir)) {
    fs.mkdirSync(credentialsDir, { recursive: true });
  }
  return path.join(credentialsDir, 'credentials.json');
}

/**
 * Generate machine-specific fingerprint
 * This makes credentials machine-specific and non-transferable
 */
function getMachineFingerprint(): string {
  // Collect machine-specific data
  const userInfo = os.userInfo();
  const machineData = [
    os.hostname(),
    userInfo.username,
    os.platform(),
    os.arch(),
    os.homedir(),
  ].join('|');
  
  // Hash with SHA-512 for fingerprint
  return crypto.createHash('sha512').update(machineData).digest('hex');
}

/**
 * Derive encryption key from machine-specific data using SHA-512
 * This makes credentials machine-specific and non-transferable
 */
function deriveKey(salt: Buffer, machineFingerprint: string): Buffer {
  // Use PBKDF2 with SHA-512 to derive a strong key
  // Combining the machine fingerprint with the random salt
  return crypto.pbkdf2Sync(
    machineFingerprint,
    salt,
    150000, // iterations (increased for SHA-512)
    64,     // key length (64 bytes for SHA-512)
    'sha512'
  ).slice(0, 32); // Take first 32 bytes for AES-256
}

/**
 * Encrypt password using AES-256-GCM with SHA-512 derived key
 */
function encryptPassword(password: string, salt: Buffer, machineFingerprint: string): { encrypted: string; iv: string } {
  const key = deriveKey(salt, machineFingerprint);
  const iv = crypto.randomBytes(16);
  
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  
  let encrypted = cipher.update(password, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  
  // Append auth tag
  const authTag = cipher.getAuthTag();
  encrypted += authTag.toString('hex');
  
  return {
    encrypted,
    iv: iv.toString('hex'),
  };
}

/**
 * Decrypt password using AES-256-GCM with SHA-512 derived key
 */
function decryptPassword(encryptedData: string, iv: string, salt: Buffer, machineFingerprint: string): string {
  const key = deriveKey(salt, machineFingerprint);
  
  // Extract auth tag (last 32 hex characters = 16 bytes)
  const authTag = Buffer.from(encryptedData.slice(-32), 'hex');
  const encrypted = encryptedData.slice(0, -32);
  
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(iv, 'hex'));
  decipher.setAuthTag(authTag);
  
  let decrypted = decipher.update(encrypted, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  
  return decrypted;
}

/**
 * Save credentials to disk (encrypted with SHA-512 and machine-specific salting)
 * This function is now called automatically on successful login
 */
export function saveCredentials(handle: string, password: string): void {
  const credentialsPath = getCredentialsPath();
  
  // Generate machine fingerprint
  const machineFingerprint = getMachineFingerprint();
  
  // Generate random salt for this credential
  const salt = crypto.randomBytes(64); // 64 bytes for SHA-512
  
  // Encrypt password with SHA-512 derived key
  const { encrypted, iv } = encryptPassword(password, salt, machineFingerprint);
  
  const credentials: StoredCredentials = {
    version: 2, // Version 2 for SHA-512
    handle,
    encryptedPassword: encrypted,
    iv,
    salt: salt.toString('hex'),
    machineFingerprint,
    createdAt: new Date().toISOString(),
    lastUsedAt: new Date().toISOString(),
  };
  
  fs.writeFileSync(credentialsPath, JSON.stringify(credentials, null, 2), 'utf-8');
  
  // Set restrictive permissions (readable only by owner)
  if (process.platform !== 'win32') {
    fs.chmodSync(credentialsPath, 0o600);
  }
}

/**
 * Load credentials from disk (decrypted)
 * Verifies machine fingerprint to prevent credential theft
 */
export function loadCredentials(): { handle: string; password: string } | null {
  const credentialsPath = getCredentialsPath();
  
  if (!fs.existsSync(credentialsPath)) {
    return null;
  }
  
  try {
    const data = fs.readFileSync(credentialsPath, 'utf-8');
    const credentials: StoredCredentials = JSON.parse(data);
    
    // Verify machine fingerprint
    const currentFingerprint = getMachineFingerprint();
    if (credentials.machineFingerprint !== currentFingerprint) {
      // Credentials were created on a different machine - cannot decrypt
      return null;
    }
    
    // Decrypt password using SHA-512 derived key
    const salt = Buffer.from(credentials.salt, 'hex');
    const password = decryptPassword(
      credentials.encryptedPassword,
      credentials.iv,
      salt,
      credentials.machineFingerprint
    );
    
    // Update last used timestamp
    credentials.lastUsedAt = new Date().toISOString();
    fs.writeFileSync(credentialsPath, JSON.stringify(credentials, null, 2), 'utf-8');
    
    return {
      handle: credentials.handle,
      password,
    };
  } catch (error) {
    // If decryption fails or file is corrupted, return null
    return null;
  }
}

/**
 * Check if credentials are saved
 */
export function hasStoredCredentials(): boolean {
  const credentialsPath = getCredentialsPath();
  return fs.existsSync(credentialsPath);
}

/**
 * Get stored handle (without decrypting password)
 */
export function getStoredHandle(): string | null {
  const credentialsPath = getCredentialsPath();
  
  if (!fs.existsSync(credentialsPath)) {
    return null;
  }
  
  try {
    const data = fs.readFileSync(credentialsPath, 'utf-8');
    const credentials: StoredCredentials = JSON.parse(data);
    return credentials.handle;
  } catch {
    return null;
  }
}

/**
 * Clear saved credentials
 */
export function clearCredentials(): void {
  const credentialsPath = getCredentialsPath();
  
  if (fs.existsSync(credentialsPath)) {
    fs.unlinkSync(credentialsPath);
  }
}

/**
 * Get credentials info (for display purposes)
 */
export function getCredentialsInfo(): { handle: string; createdAt: string; lastUsedAt: string } | null {
  const credentialsPath = getCredentialsPath();
  
  if (!fs.existsSync(credentialsPath)) {
    return null;
  }
  
  try {
    const data = fs.readFileSync(credentialsPath, 'utf-8');
    const credentials: StoredCredentials = JSON.parse(data);
    
    return {
      handle: credentials.handle,
      createdAt: credentials.createdAt,
      lastUsedAt: credentials.lastUsedAt,
    };
  } catch {
    return null;
  }
}
