import { Music2, Disc3, Layers2, RefreshCw, ListFilter } from '@lucide/svelte';
import type { ImportMode } from '$lib/types.js';
import type { Component } from 'svelte';

export type IconComponent = Component<{ size?: number; strokeWidth?: number; color?: string }>;

export interface ModeConfig {
  id: ImportMode;
  icon: IconComponent;
  title: string;
  description: string;
}

export const MODES: ModeConfig[] = [
  { id: 'lastfm',      icon: Music2 as IconComponent,    title: 'Last.fm',     description: 'Import scrobble history from a Last.fm CSV export' },
  { id: 'spotify',     icon: Disc3 as IconComponent,      title: 'Spotify',     description: 'Import play history from a Spotify JSON export' },
  { id: 'combined',    icon: Layers2 as IconComponent,    title: 'Combined',    description: 'Merge Last.fm + Spotify with smart deduplication' },
  { id: 'sync',        icon: RefreshCw as IconComponent,  title: 'Sync',        description: 'Only import records not already in Teal' },
  { id: 'deduplicate', icon: ListFilter as IconComponent, title: 'Deduplicate', description: 'Find and remove duplicate records from Teal' },
];

/** Which file sources does a given mode require? */
export function modeNeeds(mode: ImportMode | null) {
  return {
    lastfm:  mode === 'lastfm'  || mode === 'combined' || mode === 'sync',
    spotify: mode === 'spotify' || mode === 'combined',
    files:   mode !== 'deduplicate',
  };
}

/** Wizard step labels for a given mode. */
export function stepLabelsFor(mode: ImportMode | null): string[] {
  return mode === 'deduplicate'
    ? ['Mode', 'Sign in', 'Options', 'Run']
    : ['Mode', 'Sign in', 'Files',   'Options', 'Run'];
}
