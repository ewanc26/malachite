/**
 * ATProto OAuth client — browser-only.
 * Wraps @atproto/oauth-client-browser for use across the app.
 */

import { BrowserOAuthClient } from '@atproto/oauth-client-browser';
import { Agent } from '@atproto/api';

// The loopback redirect_uri must use 127.0.0.1, not localhost — RFC 8252
// explicitly disallows the localhost hostname in loopback redirect URIs.
//
// In dev, BrowserOAuthClient.load() calls atprotoLoopbackClientMetadata()
// with our constructed client_id to generate virtual client metadata.
// The client_id must be http://localhost with no path (query params are fine).
//
// In production, load() fetches the metadata from the https:// URL.
const SCOPE = 'atproto transition:generic';

const CLIENT_ID = import.meta.env.DEV
  ? `http://localhost?${new URLSearchParams([
      ['redirect_uri', 'http://127.0.0.1:5173/import'],
      ['scope', SCOPE],
    ])}`
  : 'https://malachite.croft.click/client-metadata.json';

// Singleton promise — BrowserOAuthClient.load() is async.
let _client: Promise<BrowserOAuthClient> | null = null;

function getClient(): Promise<BrowserOAuthClient> {
  if (!_client) {
    // load() accepts clientId and dispatches correctly:
    //   http:  → atprotoLoopbackClientMetadata(clientId) for dev
    //   https: → fetches the metadata document for production
    _client = BrowserOAuthClient.load({
      clientId: CLIENT_ID,
      handleResolver: 'https://bsky.social',
    });
  }
  return _client;
}

/**
 * Call once on mount on the /import page.
 * Processes any OAuth callback params in the URL and restores stored sessions.
 * Returns an Agent if a session is active, or null if the user still needs to sign in.
 */
export async function initOAuth(): Promise<Agent | null> {
  const client = await getClient();
  const result = await client.init();
  if (!result) return null;
  return new Agent(result.session);
}

/**
 * Kicks off the OAuth sign-in flow for the given handle.
 * Redirects the browser away — this never resolves normally.
 */
export async function signInWithOAuth(handle: string): Promise<never> {
  const client = await getClient();
  await client.signIn(handle, { scope: SCOPE });
  throw new Error('redirect should have occurred');
}
