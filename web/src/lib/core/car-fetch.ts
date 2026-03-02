/**
 * CAR export fetcher for ATProto repos — browser-safe, no Node.js deps.
 *
 * Calls com.atproto.sync.getRepo (sync namespace) — separate, far more
 * generous rate-limit envelope from the AppView. One HTTP request downloads
 * the entire repo as a CARv1 file, which is then parsed locally.
 *
 * Dependencies: @ipld/car  @ipld/dag-cbor  multiformats
 */

import { CarReader } from '@ipld/car';
import * as dagCbor from '@ipld/dag-cbor';
import type { CID } from 'multiformats';

// ─── ATProto repo CBOR shapes ─────────────────────────────────────────────────

interface RepoCommit {
  version: number;
  did: string;
  data: CID; // MST root
  rev: string;
  sig: Uint8Array;
}

interface MSTNode {
  l: CID | null;
  e: Array<{
    p: number;       // bytes of previous key to reuse as prefix
    k: Uint8Array;   // key suffix bytes
    v: CID;          // record CID
    t: CID | null;   // right subtree CID
  }>;
}

// ─── helpers ─────────────────────────────────────────────────────────────────

function cidStr(cid: CID): string {
  return cid.toString();
}

async function buildBlockMap(reader: CarReader): Promise<Map<string, Uint8Array>> {
  const blocks = new Map<string, Uint8Array>();
  for await (const { cid, bytes } of reader.blocks()) {
    blocks.set(cidStr(cid), bytes);
  }
  return blocks;
}

async function walkMST(
  rootCid: CID,
  blocks: Map<string, Uint8Array>,
  collection: string,
  onRecord: (rkey: string, cid: string, value: unknown) => void,
  prevKey = '',
): Promise<string> {
  const nodeBytes = blocks.get(cidStr(rootCid));
  if (!nodeBytes) return prevKey;

  const node = dagCbor.decode(nodeBytes) as MSTNode;
  let currentKey = prevKey;

  if (node.l) {
    currentKey = await walkMST(node.l, blocks, collection, onRecord, currentKey);
  }

  for (const entry of node.e ?? []) {
    const fullKey = currentKey.slice(0, entry.p) + new TextDecoder().decode(entry.k);
    currentKey = fullKey;

    const collPrefix = collection + '/';
    if (fullKey.startsWith(collPrefix)) {
      const rkey = fullKey.slice(collPrefix.length);
      const valBytes = blocks.get(cidStr(entry.v));
      if (valBytes) {
        try {
          onRecord(rkey, cidStr(entry.v), dagCbor.decode(valBytes));
        } catch {
          // malformed block — skip silently
        }
      }
    }

    if (entry.t) {
      currentKey = await walkMST(entry.t, blocks, collection, onRecord, currentKey);
    }
  }

  return currentKey;
}

// ─── public API ──────────────────────────────────────────────────────────────

export interface CARRecord {
  rkey: string;
  uri: string;
  cid: string;
  value: unknown;
}

/**
 * Fetch a user's entire ATProto repo as a CAR file and extract all records
 * from `collection`.
 *
 * @param signal  Optional AbortSignal — cancels the download mid-flight.
 */
export async function fetchRepoViaCAR(
  pdsUrl: string,
  did: string,
  collection: string,
  signal?: AbortSignal,
): Promise<CARRecord[]> {
  const url = `${pdsUrl.replace(/\/$/, '')}/xrpc/com.atproto.sync.getRepo?did=${encodeURIComponent(did)}`;

  const response = await fetch(url, {
    headers: { Accept: 'application/vnd.ipld.car' },
    signal,
  });

  if (!response.ok) {
    throw new Error(`CAR fetch failed: ${response.status} ${response.statusText}`);
  }

  const carBytes = new Uint8Array(await response.arrayBuffer());
  const reader = await CarReader.fromBytes(carBytes);
  const blocks = await buildBlockMap(reader);

  const [rootCid] = await reader.getRoots();
  if (!rootCid) throw new Error('CAR file has no roots');

  const commitBytes = blocks.get(cidStr(rootCid));
  if (!commitBytes) throw new Error('Commit block missing from CAR');

  const commit = dagCbor.decode(commitBytes) as RepoCommit;
  if (!commit.data) throw new Error('Commit has no MST root CID');

  const results: CARRecord[] = [];
  await walkMST(commit.data, blocks, collection, (rkey, cid, value) => {
    results.push({ rkey, uri: `at://${did}/${collection}/${rkey}`, cid, value });
  });

  return results;
}

/**
 * Extract the PDS base URL from an @atproto/api Agent or AtpAgent.
 * Returns null if it cannot be determined (caller should surface a useful error).
 */
export function getPdsUrlFromAgent(agent: unknown): string {
  const a = agent as Record<string, unknown>;

  // OAuth agent: session manager carries serverMetadata.issuer as the PDS base URL.
  const issuer = (a['sessionManager'] as any)?.serverMetadata?.issuer;
  if (issuer) return issuer.toString();

  // AtpAgent / password-auth agent: direct URL fields.
  for (const field of ['dispatchUrl', 'pdsUrl', 'serviceUrl', 'service']) {
    const v = a[field] ?? (a['sessionManager'] as any)?.[field];
    if (v) return v.toString();
  }

  throw new Error('Cannot determine PDS URL from agent');
}
