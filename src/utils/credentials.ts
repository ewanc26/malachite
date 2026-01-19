import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import os from 'os';

/**
 * Stored credentials structure
 */
interface StoredCredentials {
  version: number;
  handle: string;
  encryptedPassword: string;
  iv: string;
  salt: string;
  createdAt: string;
  lastUsedAt: string;
}

/**
 * Get credentials file path
 */
function getCredentialsPath(): string {
  const credentialsDir = path.join(os.homedir(), '.malachite');
  if (!fs.existsSync(credentialsDir)) {
    fs.mkdirSync(credentialsDir, { recursive: true });
  }
  return path.join(credentialsDir, 'credentials.json');
}

/**
 * Derive encryption key from machine-specific data
 * This makes credentials machine-specific and non-transferable
 */
function deriveKey(salt: Buffer): Buffer {
  // Use hostname and username to create a machine-specific key
  const machineId = `${os.hostname()}-${os.userInfo().username}`;
  
  // Use PBKDF2 to derive a strong key
  return crypto.pbkdf2Sync(
    machineId,
    salt,
    100000, // iterations
    32,     // key length
    'sha256'
  );
}

/**
 * Encrypt password
 */
function encryptPassword(password: string, salt: Buffer): { encrypted: string; iv: string } {
  const key = deriveKey(salt);
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
 * Decrypt password
 */
function decryptPassword(encryptedData: string, iv: string, salt: Buffer): string {
  const key = deriveKey(salt);
  
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
 * Save credentials to disk (encrypted)
 */
export function saveCredentials(handle: string, password: string): void {
  const credentialsPath = getCredentialsPath();
  
  // Generate random salt for this credential
  const salt = crypto.randomBytes(32);
  
  // Encrypt password
  const { encrypted, iv } = encryptPassword(password, salt);
  
  const credentials: StoredCredentials = {
    version: 1,
    handle,
    encryptedPassword: encrypted,
    iv,
    salt: salt.toString('hex'),
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
 */
export function loadCredentials(): { handle: string; password: string } | null {
  const credentialsPath = getCredentialsPath();
  
  if (!fs.existsSync(credentialsPath)) {
    return null;
  }
  
  try {
    const data = fs.readFileSync(credentialsPath, 'utf-8');
    const credentials: StoredCredentials = JSON.parse(data);
    
    // Decrypt password
    const salt = Buffer.from(credentials.salt, 'hex');
    const password = decryptPassword(
      credentials.encryptedPassword,
      credentials.iv,
      salt
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
