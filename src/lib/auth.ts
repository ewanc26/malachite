import { AtpAgent } from '@atproto/api';
import { prompt } from '../utils/input.js';

interface ResolverResponse {
  did: string;
  pds: string;
}

/**
 * Resolves an AT Protocol identifier (handle or DID) to get PDS information
 */
async function resolveIdentifier(identifier: string, resolverUrl: string): Promise<ResolverResponse> {
  console.log(`Resolving identifier: ${identifier}`);
  
  const response = await fetch(
    `${resolverUrl}/xrpc/com.bad-example.identity.resolveMiniDoc?identifier=${encodeURIComponent(identifier)}`
  );
  
  if (!response.ok) {
    throw new Error(`Failed to resolve identifier: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json() as ResolverResponse;
  
  if (!data.did || !data.pds) {
    throw new Error('Invalid response from identity resolver');
  }
  
  console.log(`✓ Resolved to PDS: ${data.pds}`);
  return data;
}

/**
 * Login to ATProto using Slingshot resolver
 */
export async function login(
  identifier: string | undefined,
  password: string | undefined,
  resolverUrl: string
): Promise<AtpAgent> {
  console.log('\n=== ATProto Login ===');
  
  // Prompt for missing credentials
  if (!identifier) {
    identifier = await prompt('Handle or DID: ');
  } else {
    console.log(`Handle or DID: ${identifier}`);
  }
  
  if (!password) {
    password = await prompt('App password: ', true);
  } else {
    console.log('App password: [hidden]');
  }
  
  try {
    // Resolve the identifier to get PDS
    const resolved = await resolveIdentifier(identifier, resolverUrl);
    
    // Create agent with resolved PDS
    const pdsAgent = new AtpAgent({ service: resolved.pds });
    
    // Login using the resolved DID
    await pdsAgent.login({
      identifier: resolved.did,
      password: password,
    });
    
    console.log('✓ Logged in successfully!');
    console.log(`  DID: ${pdsAgent.session?.did}`);
    console.log(`  Handle: ${pdsAgent.session?.handle}\n`);
    
    return pdsAgent;
  } catch (error) {
    const err = error as Error;
    console.error('✗ Login failed:', err.message);
    
    // Provide more specific error messages
    if (err.message.includes('Failed to resolve identifier')) {
      throw new Error('Handle not found. Please check your AT Protocol handle.');
    } else if (err.message.includes('AuthFactorTokenRequired')) {
      throw new Error('Two-factor authentication required. Please use your app password.');
    } else if (err.message.includes('InvalidCredentials')) {
      throw new Error('Invalid credentials. Please check your handle and app password.');
    }
    
    throw error;
  }
}
