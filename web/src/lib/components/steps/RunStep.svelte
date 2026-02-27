<script lang="ts">
  import type { ImportMode, LogEntry } from '$lib/types.js';
  import type { PublishProgress } from '$lib/core/import.js';

  let {
    isRunning,
    stopping = false,
    mode,
    dryRun,
    logs,
    progress,
    result,
    importError,
    oncancel,
    onreset,
  }: {
    isRunning: boolean;
    stopping?: boolean;
    mode: ImportMode | null;
    dryRun: boolean;
    logs: LogEntry[];
    progress: PublishProgress | null;
    result: { success: number; errors: number; cancelled: boolean } | null;
    importError: string | null;
    oncancel: () => void;
    onreset: () => void;
  } = $props();

  const LOG_COLORS: Record<string, string> = {
    info:     'var(--text)',
    success:  'var(--accent)',
    warn:     'var(--warn)',
    error:    'var(--error)',
    progress: 'var(--muted)',
    section:  'var(--accent)',
  };

  const isDedup = $derived(mode === 'deduplicate');

  // Human-readable verb for the current mode.
  const verb = $derived(
    mode === 'deduplicate' ? 'Deduplicating'
    : mode === 'sync'      ? 'Syncing'
    : 'Importing'
  );

  let logEl = $state<HTMLElement | null>(null);
  $effect(() => { if (logs.length > 0 && logEl) logEl.scrollTop = logEl.scrollHeight; });

  let runTitle = $derived(
    result
      ? (result.cancelled ? `${verb} stopped` : `${verb} complete`)
      : stopping
        ? 'Stopping…'
        : isRunning
          ? (dryRun ? 'Previewing…' : `${verb}…`)
          : importError
            ? 'Something went wrong'
            : 'Starting…',
  );

  let pct = $derived(
    progress && progress.totalRecords > 0
      ? ((progress.recordsProcessed / progress.totalRecords) * 100).toFixed(1)
      : '0',
  );
</script>

<section class="card-section run-section">
  <div class="run-header">
    <h2 class="section-title">{runTitle}</h2>
    {#if isRunning && !result}
      <button
        class="btn-cancel"
        onclick={oncancel}
        disabled={stopping}
      >{stopping ? 'Stopping…' : 'Stop'}</button>
    {/if}
  </div>

  {#if progress && !result}
    <div class="progress-wrap">
      <div class="progress-bar" style="width: {pct}%"></div>
    </div>
    <p class="progress-text">
      {progress.recordsProcessed.toLocaleString()} / {progress.totalRecords.toLocaleString()} records
      &nbsp;·&nbsp; batch {progress.batchIndex} · {progress.currentBatchSize} per batch
    </p>
  {/if}

  <div class="log-terminal" bind:this={logEl}>
    {#each logs as entry}
      <div class="log-line" style="color: {LOG_COLORS[entry.level] ?? 'var(--text)'}">
        {#if entry.level === 'section'}
          <span class="log-section">{entry.message}</span>
        {:else}
          <span class="log-ts">{new Date(entry.timestamp).toTimeString().slice(0, 8)}</span>
          <span>{entry.message}</span>
        {/if}
      </div>
    {/each}
    {#if isRunning && !result && logs.length === 0}
      <div class="log-line" style="color: var(--muted)">Initialising…</div>
    {/if}
  </div>

  {#if result}
    <div class="result-card" class:success={!result.cancelled && !importError}>
      {#if importError}
        <p class="result-label error">Error</p>
        <p class="result-detail">{importError}</p>
      {:else if result.cancelled}
        <p class="result-label warn">Stopped</p>
        <p class="result-detail">
          {#if isDedup}
            {result.success.toLocaleString()} duplicate(s) removed before stopping.
          {:else}
            {result.success.toLocaleString()} record(s) published before stopping.
          {/if}
        </p>
      {:else}
        <p class="result-label success">{dryRun ? 'Preview complete' : '✓ Done'}</p>
        <p class="result-detail">
          {#if isDedup && dryRun}
            {result.success.toLocaleString()} duplicate(s) would be removed.
          {:else if isDedup}
            {result.success.toLocaleString()} duplicate(s) removed.
          {:else if dryRun}
            {result.success.toLocaleString()} record(s) would be imported.
          {:else}
            {result.success.toLocaleString()} record(s) published successfully.
            {#if result.errors > 0}&nbsp;{result.errors} failed.{/if}
          {/if}
        </p>
      {/if}
    </div>
    <button class="btn-secondary" onclick={onreset}>← Start over</button>
  {/if}
</section>

<style>
  .run-section { padding: 1.5rem; }

  .run-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 1rem;
  }

  .run-header .section-title { margin: 0; }

  .progress-wrap {
    height: 3px;
    background: var(--surface-2);
    border-radius: 2px;
    overflow: hidden;
    margin-bottom: 0.4rem;
  }

  .progress-bar {
    height: 100%;
    background: var(--accent);
    border-radius: 2px;
    transition: width 0.3s ease;
    box-shadow: 0 0 8px var(--accent);
  }

  .progress-text {
    font-size: 0.75rem;
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
    margin: 0 0 0.75rem;
  }

  .log-terminal {
    background: #060d09;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    height: 320px;
    overflow-y: auto;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
    line-height: 1.65;
  }

  .log-line    { display: flex; align-items: baseline; gap: 0.625rem; word-break: break-word; }
  .log-ts      { color: var(--border); flex-shrink: 0; font-size: 0.7rem; }
  .log-section { color: var(--accent); font-weight: 500; letter-spacing: 0.04em; width: 100%; }

  .result-card {
    background: var(--surface-2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.25rem;
    margin-top: 1rem;
    text-align: center;
  }

  .result-card.success { border-color: rgba(63, 185, 104, 0.4); background: var(--accent-glow); }

  .result-label { font-size: 1rem; font-weight: 600; margin: 0 0 0.25rem; }
  .result-label.success { color: var(--accent); }
  .result-label.warn    { color: var(--warn); }
  .result-label.error   { color: var(--error); }
  .result-detail { font-size: 0.875rem; color: var(--muted); margin: 0; }
</style>
