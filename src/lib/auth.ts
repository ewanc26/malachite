/**
 * CLI authentication wrapper.
 * Adds terminal prompts and credential persistence on top of the core login.
 */

import type { Agent } from '@atproto/api';
import { login as coreLogin, resolveIdentity } from '../core/auth.js';
import { prompt } from '../utils/input.js';
import * as ui from '../utils/ui.js';
import { saveCredentials } from '../utils/credentials.js';

export { resolveIdentity };

/**
 * CLI-aware login: prompts for missing credentials, surfaces progress via
 * spinners, and automatically persists credentials on success.
 */
export async function login(
  identifier: string | undefined,
  password: string | undefined,
  resolverOrPds?: string
): Promise<Agent> {
  ui.header('ATProto Login');

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

  // If resolverOrPds looks like a direct PDS URL (not Slingshot), use it as-is.
  const isSlingshot = !resolverOrPds || resolverOrPds.includes('slingshot');
  const pdsOverride = resolverOrPds && !isSlingshot ? resolverOrPds : undefined;

  try {
    ui.startSpinner(pdsOverride ? `Using provided PDS: ${pdsOverride}` : 'Resolving identity…');

    const agent = await coreLogin(identifier!, password!, pdsOverride);

    ui.succeedSpinner('Logged in successfully!');
    ui.keyValue('DID', (agent as any).session?.did || (agent as any).did || 'unknown');
    ui.keyValue('Handle', (agent as any).session?.handle || 'unknown');

    try {
      saveCredentials(identifier!, password!);
      ui.info('Credentials saved securely');
    } catch {
      ui.warning('Failed to save credentials — you may need to re-enter them next time');
    }

    console.log('');
    return agent;
  } catch (error) {
    const err = error as Error;
    ui.failSpinner('Login failed');

    if (err.message.includes('Failed to resolve identity')) {
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
