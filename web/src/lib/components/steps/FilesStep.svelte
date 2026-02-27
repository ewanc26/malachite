<script lang="ts">
  import { CheckCircle2, Music2, Disc3 } from '@lucide/svelte';

  let {
    lastfmFiles  = $bindable<File[]>([]),
    spotifyFiles = $bindable<File[]>([]),
    needs,
    oncontinue,
    onback,
  }: {
    lastfmFiles:  File[];
    spotifyFiles: File[];
    needs: { lastfm: boolean; spotify: boolean; files: boolean };
    oncontinue: () => void;
    onback: () => void;
  } = $props();

  let lfDragging = $state(false);
  let spDragging = $state(false);

  function handleDrop(e: DragEvent, type: 'lf' | 'sp') {
    e.preventDefault();
    if (type === 'lf') {
      lfDragging  = false;
      lastfmFiles = Array.from(e.dataTransfer?.files ?? []).filter((f) => f.name.endsWith('.csv'));
    } else {
      spDragging   = false;
      spotifyFiles = Array.from(e.dataTransfer?.files ?? []).filter((f) => f.name.endsWith('.json'));
    }
  }

  let canContinue = $derived(
    (needs.lastfm && needs.spotify && lastfmFiles.length > 0 && spotifyFiles.length > 0) ||
    (needs.lastfm && !needs.spotify && lastfmFiles.length > 0) ||
    (!needs.lastfm && needs.spotify && spotifyFiles.length > 0),
  );
</script>

<section class="card-section">
  <button class="back-btn" onclick={onback}>← Back</button>
  <h2 class="section-title">
    Upload your export{needs.lastfm && needs.spotify ? 's' : ''}
  </h2>

  <div class="drop-zones">
    {#if needs.lastfm}
      <div
        class="drop-zone"
        class:dragging={lfDragging}
        class:filled={lastfmFiles.length > 0}
        role="button"
        tabindex="0"
        aria-label="Upload Last.fm CSV file"
        ondragover={(e) => { e.preventDefault(); lfDragging = true; }}
        ondragleave={() => (lfDragging = false)}
        ondrop={(e) => handleDrop(e, 'lf')}
        onclick={() => document.getElementById('lfInput')?.click()}
        onkeydown={(e) => e.key === 'Enter' && document.getElementById('lfInput')?.click()}
      >
        <input
          id="lfInput"
          type="file"
          accept=".csv"
          hidden
          onchange={(e) => { lastfmFiles = Array.from((e.target as HTMLInputElement).files ?? []); }}
        />
        {#if lastfmFiles.length > 0}
          <span class="drop-icon drop-done"><CheckCircle2 size={28} /></span>
          <span class="drop-filename">{lastfmFiles[0].name}</span>
          <span class="drop-meta">{(lastfmFiles[0].size / 1024).toFixed(0)} KB · CSV</span>
        {:else}
          <span class="drop-icon"><Music2 size={28} /></span>
          <span class="drop-title">Last.fm CSV</span>
          <span class="drop-hint">Drag & drop or click to select</span>
        {/if}
      </div>
    {/if}

    {#if needs.spotify}
      <div
        class="drop-zone"
        class:dragging={spDragging}
        class:filled={spotifyFiles.length > 0}
        role="button"
        tabindex="0"
        aria-label="Upload Spotify JSON files"
        ondragover={(e) => { e.preventDefault(); spDragging = true; }}
        ondragleave={() => (spDragging = false)}
        ondrop={(e) => handleDrop(e, 'sp')}
        onclick={() => document.getElementById('spInput')?.click()}
        onkeydown={(e) => e.key === 'Enter' && document.getElementById('spInput')?.click()}
      >
        <input
          id="spInput"
          type="file"
          accept=".json"
          multiple
          hidden
          onchange={(e) => { spotifyFiles = Array.from((e.target as HTMLInputElement).files ?? []); }}
        />
        {#if spotifyFiles.length > 0}
          <span class="drop-icon drop-done"><CheckCircle2 size={28} /></span>
          <span class="drop-filename">
            {spotifyFiles.length === 1 ? spotifyFiles[0].name : `${spotifyFiles.length} files selected`}
          </span>
          <span class="drop-meta">
            {spotifyFiles.length === 1
              ? `${(spotifyFiles[0].size / 1024).toFixed(0)} KB · JSON`
              : 'JSON · Streaming_History_Audio_*'}
          </span>
        {:else}
          <span class="drop-icon"><Disc3 size={28} /></span>
          <span class="drop-title">Spotify JSON</span>
          <span class="drop-hint">Select one or more JSON files</span>
        {/if}
      </div>
    {/if}
  </div>

  <div class="how-to">
    {#if needs.lastfm}
      <details>
        <summary>How to export from Last.fm</summary>
        <p>
          Go to <a href="https://www.last.fm/settings/dataexporter" target="_blank" rel="noopener">
            last.fm/settings/dataexporter
          </a>, request a data export, and download the CSV once ready.
        </p>
      </details>
    {/if}
    {#if needs.spotify}
      <details>
        <summary>How to export from Spotify</summary>
        <p>
          Go to <a href="https://www.spotify.com/account/privacy" target="_blank" rel="noopener">
            spotify.com/account/privacy
          </a>, request "Extended streaming history", and upload all
          <code>Streaming_History_Audio_*.json</code> files.
        </p>
      </details>
    {/if}
  </div>

  <button class="btn-primary" onclick={oncontinue} disabled={!canContinue}>Continue →</button>
</section>

<style>
  .drop-zones {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1.25rem;
  }

  .drop-zone {
    background: var(--surface-2);
    border: 1.5px dashed var(--border);
    border-radius: 8px;
    padding: 2rem 1.5rem;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.4rem;
    cursor: pointer;
    transition: border-color 0.15s, background 0.15s;
    text-align: center;
    user-select: none;
  }

  .drop-zone:hover,
  .drop-zone.dragging { border-color: var(--accent); background: var(--accent-glow); }
  .drop-zone.filled   { border-style: solid; border-color: var(--accent); }

  .drop-icon          { color: var(--muted); display: flex; }
  .drop-icon.drop-done { color: var(--accent); }
  .drop-title         { font-size: 0.875rem; font-weight: 500; color: var(--text); }
  .drop-filename      { font-size: 0.825rem; color: var(--accent); font-family: 'JetBrains Mono', monospace; word-break: break-all; }
  .drop-meta          { font-size: 0.7rem; color: var(--muted); }
  .drop-hint          { font-size: 0.75rem; color: var(--muted); }
</style>
