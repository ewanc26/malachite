/**
 * ATProto authentication — environment-agnostic.
 * No CLI prompts; credentials come from the caller.
 */

import { Agent, AtpAgent } from '@atproto/api';
import { SLINGSHOT_RESOLVER } from './config.js';

export interface ResolvedIdentity {
  did: string;
  handle: string;
  pds: string;
}

/**
 * Resolve an AT Protocol handle or DID to its PDS URL via the Slingshot resolver.
 */
export async function resolveIdentity(
  identifier: string,
  resolverBase = SLINGSHOT_RESOLVER
): Promise<ResolvedIdentity> {
  const url = `${resolverBase}/xrpc/com.bad-example.identity.resolveMiniDoc?identifier=${encodeURIComponent(identifier)}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to resolve identity: ${res.status} ${res.statusText}`);
  }
  const data = (await res.json()) as ResolvedIdentity;
  if (!data.did || !data.pds) {
    throw new Error('Invalid response from identity resolver');
  }
  return data;
}

/**
 * Log in to ATProto.
 * - If `pdsOverride` is supplied, skips identity resolution and hits that PDS directly.
 * - Otherwise uses the Slingshot resolver to find the correct PDS.
 */
export async function login(
  identifier: string,
  password: string,
  pdsOverride?: string
): Promise<Agent> {
  if (pdsOverride) {
    const agent = new AtpAgent({ service: pdsOverride });
    await agent.login({ identifier, password });
    return agent;
  }

  const identity = await resolveIdentity(identifier);
  const agent = new AtpAgent({ service: identity.pds });
  await agent.login({ identifier: identity.did, password });
  return agent;
}
