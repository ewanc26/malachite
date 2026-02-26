/**
 * Browser-compatible ATProto authentication.
 * No CLI prompts — credentials come from the web form.
 */

import { AtpAgent } from '@atproto/api';
import { SLINGSHOT_RESOLVER } from '../config.js';

interface ResolvedIdentity {
  did: string;
  handle: string;
  pds: string;
}

export async function resolveIdentity(identifier: string): Promise<ResolvedIdentity> {
  const url = `${SLINGSHOT_RESOLVER}/xrpc/com.bad-example.identity.resolveMiniDoc?identifier=${encodeURIComponent(identifier)}`;
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

export async function login(
  identifier: string,
  password: string,
  pdsOverride?: string
): Promise<AtpAgent> {
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
