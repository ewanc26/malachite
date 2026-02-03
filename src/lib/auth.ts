import { prompt } from '../utils/input.js';
import * as ui from '../utils/ui.js';
import { SafeAgent } from './safe-agent.js';

interface ResolvedIdentity {
  did: string;
  handle: string;
  pds: string;
  signing_key: string;
}

/**
 * Resolves an AT Protocol identifier (handle or DID) to get PDS information
 */
async function resolveIdentifier(identifier: string): Promise<ResolvedIdentity> {
  ui.startSpinner(`Resolving identifier: ${identifier}`);
  
  try {
    const response = await fetch(
      `https://slingshot.microcosm.blue/xrpc/com.bad-example.identity.resolveMiniDoc?identifier=${encodeURIComponent(identifier)}`
    );
    
    if (!response.ok) {
      throw new Error(`Failed to resolve identifier: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json() as ResolvedIdentity;
    
    if (!data.did || !data.pds) {
      throw new Error('Invalid response from identity resolver');
    }
    
    ui.succeedSpinner(`Resolved to PDS: ${data.pds}`);
    return data;
  } catch (error) {
    ui.failSpinner('Failed to resolve identifier');
    throw error;
  }
}

/**
 * Login to ATProto using Slingshot resolver
 */
export async function login(
  identifier: string | undefined,
  password: string | undefined,
  _resolverUrl?: string // Keep parameter for backwards compatibility but don't use it
): Promise<SafeAgent> {
  ui.header('ATProto Login');
  
  // Prompt for missing credentials
  if (!identifier) {
    identifier = await prompt('Handle or DID: ');
  } else {
    ui.keyValue('Handle or DID', identifier);
  }
  
  if (!password) {
    password = await prompt('App password: ', true);
  } else {
    ui.keyValue('App password', '[hidden]');
  }
  
  console.log('');
  
  try {
    // Resolve the identifier to get PDS and other info
    const resolved = await resolveIdentifier(identifier);
    
    // Initialize the agent with the resolved PDS URL
    ui.startSpinner('Logging in...');
    const agent = new SafeAgent({
      service: resolved.pds,
    });

    // Attempt to login using the resolved DID for more reliable authentication
    await agent.login({
      identifier: resolved.did,
      password: password,
    });
    
    ui.succeedSpinner('Logged in successfully!');
    ui.keyValue('DID', agent.session?.did || 'unknown');
    ui.keyValue('Handle', agent.session?.handle || 'unknown');
    console.log('');
    
    return agent;
  } catch (error) {
    const err = error as Error;
    ui.failSpinner('Login failed');
    
    // Provide more specific error messages
    if (err.message.includes('Failed to resolve identifier')) {
      throw new Error('Handle not found. Please check your AT Protocol handle.');
    } else if (err.message.includes('AuthFactorTokenRequired')) {
      throw new Error('Two-factor authentication required. Please use your app password.');
    } else if (err.message.includes('AccountTakedown') || err.message.includes('AccountSuspended')) {
      throw new Error('Account is suspended or has been taken down.');
    } else if (err.message.includes('InvalidCredentials')) {
      throw new Error('Invalid credentials. Please check your handle and app password.');
    } else {
      throw new Error(`Login failed: ${err.message || 'Unknown error'}`);
    }
  }
}
