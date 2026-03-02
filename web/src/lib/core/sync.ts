/**
 * Browser-compatible sync helpers.
 * Fetches existing records via com.atproto.sync.getRepo (CAR export) —
 * sync namespace, separate rate-limit envelope, zero AppView quota cost.
 */

import type { Agent } from '@atproto/api';
import type { PlayRecord } from '../types.js';
import { RECORD_TYPE } from '../config.js';
import { fetchRepoViaCAR, getPdsUrlFromAgent } from './car-fetch.js';

export interface ExistingRecord {
  uri: string;
  cid: string;
  value: PlayRecord;
}

function recordKey(r: PlayRecord): string {
  const artist = (r.artists[0]?.artistName ?? '').toLowerCase().trim();
  return `${artist}|||${r.trackName.toLowerCase().trim()}|||${r.playedTime}`;
}

/** In-session cache so repeat calls within the same page session don't re-fetch. */
const sessionCache = new Map<string, Map<string, ExistingRecord>>();

export async function fetchExistingRecords(
  agent: Agent,
  onProgress?: (fetched: number) => void,
  forceRefresh = false,
  signal?: AbortSignal
): Promise<Map<string, ExistingRecord>> {
  const did = agent.did;
  if (!did) throw new Error('No authenticated session');

  if (!forceRefresh && sessionCache.has(did)) {
    return sessionCache.get(did)!;
  }

  signal?.throwIfAborted();

  const pdsUrl = getPdsUrlFromAgent(agent);
  const carRecords = await fetchRepoViaCAR(pdsUrl, did, RECORD_TYPE, signal);

  const map = new Map<string, ExistingRecord>();
  for (const rec of carRecords) {
    const value = rec.value as unknown as PlayRecord;
    map.set(recordKey(value), { uri: rec.uri, cid: rec.cid, value });
  }

  onProgress?.(map.size);
  sessionCache.set(did, map);
  return map;
}

export function filterNewRecords(
  records: PlayRecord[],
  existing: Map<string, ExistingRecord>
): PlayRecord[] {
  return records.filter((r) => !existing.has(recordKey(r)));
}

export async function fetchAllRecordsForDedup(
  agent: Agent,
  onProgress?: (fetched: number) => void,
  signal?: AbortSignal
): Promise<ExistingRecord[]> {
  const did = agent.did;
  if (!did) throw new Error('No authenticated session');

  signal?.throwIfAborted();

  const pdsUrl = getPdsUrlFromAgent(agent);
  const carRecords = await fetchRepoViaCAR(pdsUrl, did, RECORD_TYPE, signal);

  const all: ExistingRecord[] = carRecords.map((rec) => ({
    uri: rec.uri,
    cid: rec.cid,
    value: rec.value as unknown as PlayRecord,
  }));

  onProgress?.(all.length);
  return all;
}

export interface DedupGroup {
  key: string;
  records: ExistingRecord[];
}

export function findDuplicateGroups(records: ExistingRecord[]): DedupGroup[] {
  const groups = new Map<string, ExistingRecord[]>();
  for (const rec of records) {
    const key = recordKey(rec.value);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)!.push(rec);
  }
  const result: DedupGroup[] = [];
  for (const [key, recs] of groups) {
    if (recs.length > 1) result.push({ key, records: recs });
  }
  return result;
}

export async function removeDuplicateRecords(
  agent: Agent,
  groups: DedupGroup[],
  onProgress?: (removed: number) => void,
  signal?: AbortSignal
): Promise<number> {
  let removed = 0;
  for (const group of groups) {
    for (const rec of group.records.slice(1)) {
      signal?.throwIfAborted();
      try {
        await agent.com.atproto.repo.deleteRecord(
          { repo: agent.did ?? '', collection: RECORD_TYPE, rkey: rec.uri.split('/').pop()! },
          { signal }
        );
        removed++;
        onProgress?.(removed);
        await new Promise<void>((resolve, reject) => {
          const t = setTimeout(resolve, 100);
          signal?.addEventListener('abort', () => { clearTimeout(t); reject(signal.reason); }, { once: true });
        });
      } catch (err: any) {
        if (signal?.aborted) throw err;
      }
    }
  }
  return removed;
}
