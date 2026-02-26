<script lang="ts">
  import { fly, scale } from 'svelte/transition';
  import { cubicOut, backOut } from 'svelte/easing';
  import { Check, Eye, EyeOff, Music2, Disc3, Layers2, RefreshCw, ListFilter, CheckCircle2 } from '@lucide/svelte';

  import { parseLastFmFile, convertToPlayRecord } from '$lib/core/csv.js';
  import { parseSpotifyFiles, convertSpotifyToPlayRecord } from '$lib/core/spotify.js';
  import { mergePlayRecords, deduplicateInputRecords, sortRecords } from '$lib/core/merge.js';
  import { fetchExistingRecords, filterNewRecords, fetchAllRecordsForDedup, findDuplicateGroups, removeDuplicateRecords } from '$lib/core/sync.js';
  import { publishRecords, type PublishProgress } from '$lib/core/publisher.js';
  import { login } from '$lib/core/auth.js';
  import type { AtpAgent } from '@atproto/api';
  import { modeNeeds, MODES } from '$lib';
  import type { ImportMode, LogEntry, PlayRecord } from '$lib';

  // ─── wizard state ────────────────────────────────────────────────────────────

  let step      = $state(0);
  let prevStep  = $state(0);
  let mode      = $state<ImportMode | null>(null);

  let handle      = $state('');
  let password    = $state('');
  let pdsOverride = $state('');
  let agent       = $state<AtpAgent | null>(null);

  let lastfmFiles  = $state<File[]>([]);
  let spotifyFiles = $state<File[]>([]);

  let dryRun       = $state(false);
  let reverseOrder = $state(false);
  let fresh        = $state(false);

  let isRunning   = $state(false);
  let cancelled   = $state(false);
  let logs        = $state<LogEntry[]>([]);
  let progress    = $state<PublishProgress | null>(null);
  let result      = $state<{ success: number; errors: number; cancelled: boolean } | null>(null);
  let importError = $state<string | null>(null);

  // ─── derived ──────────────────────────────────────────────────────────────────

  let needs = $derived(modeNeeds(mode));

  let stepLabels = $derived(
    mode === 'deduplicate'
      ? ['Mode', 'Sign in', 'Options', 'Run']
      : ['Mode', 'Sign in', 'Files', 'Options', 'Run']
  );

  let goingForward = $derived(step >= prevStep);

  // ─── nav ──────────────────────────────────────────────────────────────────────

  function goTo(n: number) { prevStep = step; step = n; }

  function selectMode(m: ImportMode) { mode = m; goTo(1); }

  function goBack() {
    if (step === 3 && mode === 'deduplicate') { goTo(1); return; }
    goTo(Math.max(0, step - 1));
  }

  function afterAuth(a: AtpAgent) {
    agent = a;
    goTo(needs.files ? 2 : 3);
  }

  // ─── auth state ───────────────────────────────────────────────────────────────

  let showPassword = $state(false);
  let showAdvanced = $state(false);
  let authError    = $state<string | null>(null);
  let authLoading  = $state(false);

  async function doAuth() {
    authError = null;
    authLoading = true;
    try {
      afterAuth(await login(handle.trim(), password, pdsOverride.trim() || undefined));
    } catch (err: any) {
      authError = err.message ?? 'Login failed';
    } finally {
      authLoading = false;
    }
  }

  // ─── files state ──────────────────────────────────────────────────────────────

  let lfDragging = $state(false);
  let spDragging = $state(false);

  function handleDrop(e: DragEvent, type: 'lf' | 'sp') {
    e.preventDefault();
    if (type === 'lf') { lfDragging = false; lastfmFiles  = Array.from(e.dataTransfer?.files ?? []).filter(f => f.name.endsWith('.csv')); }
    else               { spDragging = false; spotifyFiles = Array.from(e.dataTransfer?.files ?? []).filter(f => f.name.endsWith('.json')); }
  }

  let canContinue = $derived(
    (needs.lastfm && needs.spotify && lastfmFiles.length > 0 && spotifyFiles.length > 0) ||
    (needs.lastfm && !needs.spotify && lastfmFiles.length > 0) ||
    (!needs.lastfm && needs.spotify && spotifyFiles.length > 0)
  );

  // ─── log ──────────────────────────────────────────────────────────────────────

  let logEl = $state<HTMLElement | null>(null);
  $effect(() => { if (logs.length > 0 && logEl) logEl.scrollTop = logEl.scrollHeight; });

  function addLog(level: LogEntry['level'], message: string) {
    logs = [...logs, { level, message, timestamp: Date.now() }];
  }

  const LOG_COLORS: Record<string, string> = {
    info: 'var(--text)', success: 'var(--accent)', warn: 'var(--warn)',
    error: 'var(--error)', progress: 'var(--muted)', section: 'var(--accent)'
  };

  let runTitle = $derived(
    result
      ? (result.cancelled ? 'Import stopped' : 'Import complete')
      : isRunning ? (dryRun ? 'Previewing…' : 'Importing…')
      : importError ? 'Something went wrong' : 'Starting…'
  );

  let pct = $derived(
    progress && progress.totalRecords > 0
      ? (progress.recordsProcessed / progress.totalRecords * 100).toFixed(1)
      : '0'
  );

  // ─── import ───────────────────────────────────────────────────────────────────

  async function startImport() {
    if (!agent || !mode) return;
    isRunning = true; cancelled = false; logs = []; importError = null; result = null;
    goTo(4);
    try {
      if (mode === 'deduplicate') {
        addLog('section', '── Deduplication ──────────────────────────────────');
        addLog('info', 'Fetching existing records from Teal…');
        const all = await fetchAllRecordsForDedup(agent, n => addLog('progress', `  Fetched ${n.toLocaleString()} records…`));
        addLog('success', `Fetched ${all.length.toLocaleString()} records`);
        const groups = findDuplicateGroups(all);
        const totalDups = groups.reduce((s, g) => s + g.records.length - 1, 0);
        if (totalDups === 0) { addLog('success', 'No duplicates found — your records are clean.'); result = { success: 0, errors: 0, cancelled: false }; return; }
        addLog('warn', `Found ${totalDups.toLocaleString()} duplicate(s) across ${groups.length} groups`);
        if (dryRun) { addLog('info', `[DRY RUN] Would remove ${totalDups} duplicate record(s).`); result = { success: totalDups, errors: 0, cancelled: false }; return; }
        addLog('info', 'Removing duplicates…');
        const removed = await removeDuplicateRecords(agent, groups, n => { if (!cancelled) addLog('progress', `  Removed ${n}/${totalDups}…`); });
        addLog('success', `Removed ${removed.toLocaleString()} duplicate(s)`);
        result = { success: removed, errors: 0, cancelled: false };
        return;
      }

      addLog('section', '── Loading Records ─────────────────────────────────');
      let records: PlayRecord[] = [];
      if (mode === 'combined') {
        const lfRaw = await parseLastFmFile(lastfmFiles[0]);
        addLog('info', `Last.fm: ${lfRaw.length.toLocaleString()} scrobbles`);
        const spRaw = await parseSpotifyFiles(spotifyFiles);
        addLog('info', `Spotify: ${spRaw.length.toLocaleString()} tracks`);
        const { merged, stats } = mergePlayRecords(lfRaw.map(r => convertToPlayRecord(r)), spRaw.map(r => convertSpotifyToPlayRecord(r)));
        records = merged;
        addLog('success', `Merged: ${stats.mergedTotal.toLocaleString()} unique records (${stats.duplicatesRemoved} removed)`);
      } else if (mode === 'spotify') {
        const spRaw = await parseSpotifyFiles(spotifyFiles);
        records = spRaw.map(r => convertSpotifyToPlayRecord(r));
        addLog('success', `Loaded ${records.length.toLocaleString()} Spotify records`);
      } else {
        const lfRaw = await parseLastFmFile(lastfmFiles[0]);
        records = lfRaw.map(r => convertToPlayRecord(r));
        addLog('success', `Loaded ${records.length.toLocaleString()} Last.fm records`);
      }

      const { unique, duplicates: inputDups } = deduplicateInputRecords(records);
      records = unique;
      if (inputDups > 0) addLog('warn', `Removed ${inputDups.toLocaleString()} duplicate(s) from input`);

      addLog('section', '── Sync Check ───────────────────────────────────────');
      addLog('info', 'Fetching existing records from Teal…');
      const existing = await fetchExistingRecords(agent, n => addLog('progress', `  Fetched ${n.toLocaleString()} existing records…`), fresh);
      const before = records.length;
      records = filterNewRecords(records, existing);
      const skipped = before - records.length;
      if (skipped > 0) addLog('info', `Skipped ${skipped.toLocaleString()} already-imported record(s)`);
      addLog('success', `${records.length.toLocaleString()} new record(s) to import`);

      if (records.length === 0) { addLog('success', '✓ Nothing to import — all records already exist in Teal!'); result = { success: 0, errors: 0, cancelled: false }; return; }
      if (mode !== 'combined') records = sortRecords(records, reverseOrder);

      addLog('section', '── Publishing ───────────────────────────────────────');
      const res = await publishRecords(agent, records, dryRun, {
        onProgress: p => { progress = p; },
        onLog: (level, msg) => addLog(level as LogEntry['level'], msg),
        isCancelled: () => cancelled
      });
      result = { success: res.successCount, errors: res.errorCount, cancelled: res.cancelled };
      if (res.cancelled) addLog('warn', `Stopped. ${res.successCount.toLocaleString()} records published.`);
      else if (dryRun)   addLog('success', `Dry run complete — ${res.successCount.toLocaleString()} records would be imported.`);
      else { addLog('success', `Import complete! ${res.successCount.toLocaleString()} records published.`); if (res.errorCount > 0) addLog('warn', `${res.errorCount} record(s) failed.`); }
    } catch (err: any) {
      importError = err.message ?? 'An unexpected error occurred';
      addLog('error', `Fatal: ${importError}`);
    } finally {
      isRunning = false;
    }
  }

  // ─── reset ────────────────────────────────────────────────────────────────────

  function reset() {
    prevStep = step; step = 0; mode = null; agent = null;
    lastfmFiles = []; spotifyFiles = [];
    dryRun = false; reverseOrder = false; fresh = false;
    logs = []; progress = null; result = null; importError = null; cancelled = false;
  }
</script>

<svelte:head>
  <title>Malachite — Import to Teal</title>
</svelte:head>

<main>
  <header>
    <p class="logo-text">Malachite</p>
    <p class="tagline">Import Last.fm &amp; Spotify history to ATProto / Teal</p>
  </header>

  <!-- Step indicator -->
  {#if step > 0 && step < 5}
    <nav class="steps" aria-label="Progress">
      {#each stepLabels as label, i}
        <div class="step-item" class:done={i < step} class:active={i === step}>
          <span class="step-dot">
            {#key i < step}
              <span
                class="dot-inner"
                in:scale={{ duration: 250, start: 0.4, easing: backOut }}
              >
                {#if i < step}<Check size={12} strokeWidth={3} />{:else}{i + 1}{/if}
              </span>
            {/key}
          </span>
          <span class="step-label">{label}</span>
        </div>
        {#if i < stepLabels.length - 1}
          <div class="step-line" class:done={i < step}></div>
        {/if}
      {/each}
    </nav>
  {/if}

  <!-- Step content -->
  <div class="step-viewport">
    {#key step}
      <div
        class="step-slide"
        in:fly={{ x: goingForward ? 40 : -40, duration: 280, easing: cubicOut }}
        out:fly={{ x: goingForward ? -40 : 40, duration: 200, easing: cubicOut }}
      >

        <!-- ── Step 0: Mode ─────────────────────────────────────────────────── -->
        {#if step === 0}
          <section class="card-section">
            <h2 class="section-title">What would you like to do?</h2>
            <div class="mode-grid">
              {#each MODES as m}
                <button class="mode-card" onclick={() => selectMode(m.id)}>
                  <span class="mode-icon"><m.icon size={20} /></span>
                  <span class="mode-title">{m.title}</span>
                  <span class="mode-desc">{m.description}</span>
                </button>
              {/each}
            </div>
            <p class="footer-note">
              <a href="https://github.com/ewanc26/malachite" target="_blank" rel="noopener">↗ View on GitHub</a>
              &nbsp;·&nbsp;
              <a href="https://ko-fi.com/ewancroft" target="_blank" rel="noopener">♥ Support Malachite</a>
            </p>
          </section>

        <!-- ── Step 1: Auth ─────────────────────────────────────────────────── -->
        {:else if step === 1}
          <section class="card-section">
            <button class="back-btn" onclick={goBack}>← Back</button>
            <h2 class="section-title">Sign in to ATProto</h2>
            <p class="section-sub">
              Use your Bluesky handle and an
              <a href="https://bsky.app/settings/app-passwords" target="_blank" rel="noopener">app password</a>.
            </p>
            <div class="form">
              <label class="field">
                <span class="field-label">Handle</span>
                <input type="text" bind:value={handle} placeholder="you.bsky.social" autocomplete="username" spellcheck="false" />
              </label>
              <label class="field">
                <span class="field-label">App password</span>
                <div class="password-wrap">
                  <input type={showPassword ? 'text' : 'password'} bind:value={password} placeholder="xxxx-xxxx-xxxx-xxxx" autocomplete="current-password" />
                  <button class="pw-toggle" onclick={() => (showPassword = !showPassword)} type="button" aria-label={showPassword ? 'Hide password' : 'Show password'}>
                    {#if showPassword}<EyeOff size={14} />{:else}<Eye size={14} />{/if}
                  </button>
                </div>
              </label>
              <button class="expand-btn" onclick={() => (showAdvanced = !showAdvanced)} type="button">
                {showAdvanced ? '▾' : '▸'} Advanced options
              </button>
              {#if showAdvanced}
                <label class="field">
                  <span class="field-label">PDS URL override <span class="badge">optional</span></span>
                  <input type="url" bind:value={pdsOverride} placeholder="https://your.pds.example" spellcheck="false" />
                  <span class="field-hint">Skip Slingshot identity resolution and connect directly to your PDS.</span>
                </label>
              {/if}
              {#if authError}
                <div class="alert alert-error">{authError}</div>
              {/if}
              <button class="btn-primary" onclick={doAuth} disabled={authLoading || !handle || !password}>
                {#if authLoading}<span class="spinner"></span> Signing in…{:else}Sign in →{/if}
              </button>
            </div>
          </section>

        <!-- ── Step 2: Files ────────────────────────────────────────────────── -->
        {:else if step === 2}
          <section class="card-section">
            <button class="back-btn" onclick={goBack}>← Back</button>
            <h2 class="section-title">Upload your export{needs.lastfm && needs.spotify ? 's' : ''}</h2>
            <div class="drop-zones">
              {#if needs.lastfm}
                <div
                  class="drop-zone" class:dragging={lfDragging} class:filled={lastfmFiles.length > 0}
                  role="button" tabindex="0" aria-label="Upload Last.fm CSV file"
                  ondragover={e => { e.preventDefault(); lfDragging = true; }}
                  ondragleave={() => (lfDragging = false)}
                  ondrop={e => handleDrop(e, 'lf')}
                  onclick={() => document.getElementById('lfInput')?.click()}
                  onkeydown={e => e.key === 'Enter' && document.getElementById('lfInput')?.click()}
                >
                  <input id="lfInput" type="file" accept=".csv" hidden onchange={e => { lastfmFiles = Array.from((e.target as HTMLInputElement).files ?? []); }} />
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
                  class="drop-zone" class:dragging={spDragging} class:filled={spotifyFiles.length > 0}
                  role="button" tabindex="0" aria-label="Upload Spotify JSON files"
                  ondragover={e => { e.preventDefault(); spDragging = true; }}
                  ondragleave={() => (spDragging = false)}
                  ondrop={e => handleDrop(e, 'sp')}
                  onclick={() => document.getElementById('spInput')?.click()}
                  onkeydown={e => e.key === 'Enter' && document.getElementById('spInput')?.click()}
                >
                  <input id="spInput" type="file" accept=".json" multiple hidden onchange={e => { spotifyFiles = Array.from((e.target as HTMLInputElement).files ?? []); }} />
                  {#if spotifyFiles.length > 0}
                    <span class="drop-icon drop-done"><CheckCircle2 size={28} /></span>
                    <span class="drop-filename">{spotifyFiles.length === 1 ? spotifyFiles[0].name : `${spotifyFiles.length} files selected`}</span>
                    <span class="drop-meta">{spotifyFiles.length === 1 ? `${(spotifyFiles[0].size / 1024).toFixed(0)} KB · JSON` : 'JSON · Streaming_History_Audio_*'}</span>
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
                  <p>Go to <a href="https://www.last.fm/settings/dataexporter" target="_blank" rel="noopener">last.fm/settings/dataexporter</a>, request a data export, and download the CSV once ready.</p>
                </details>
              {/if}
              {#if needs.spotify}
                <details>
                  <summary>How to export from Spotify</summary>
                  <p>Go to <a href="https://www.spotify.com/account/privacy" target="_blank" rel="noopener">spotify.com/account/privacy</a>, request "Extended streaming history", and upload all <code>Streaming_History_Audio_*.json</code> files.</p>
                </details>
              {/if}
            </div>
            <button class="btn-primary" onclick={() => goTo(3)} disabled={!canContinue}>Continue →</button>
          </section>

        <!-- ── Step 3: Options ──────────────────────────────────────────────── -->
        {:else if step === 3}
          <section class="card-section">
            <button class="back-btn" onclick={goBack}>← Back</button>
            <h2 class="section-title">Import options</h2>
            <div class="options">
              <div class="option-row">
                <div class="option-info">
                  <span class="option-name">Dry run</span>
                  <span class="option-desc">Preview what would be imported without making changes</span>
                </div>
                <button class="toggle" class:on={dryRun} onclick={() => (dryRun = !dryRun)} type="button" aria-label="Toggle dry run" aria-pressed={dryRun}>
                  <span class="toggle-thumb"></span>
                </button>
              </div>
              {#if mode !== 'deduplicate'}
                <div class="option-row">
                  <div class="option-info">
                    <span class="option-name">Reverse order</span>
                    <span class="option-desc">Process newest records first (default: oldest first)</span>
                  </div>
                  <button class="toggle" class:on={reverseOrder} onclick={() => (reverseOrder = !reverseOrder)} type="button" aria-label="Toggle reverse order" aria-pressed={reverseOrder}>
                    <span class="toggle-thumb"></span>
                  </button>
                </div>
                <div class="option-row">
                  <div class="option-info">
                    <span class="option-name">Fresh start</span>
                    <span class="option-desc">Re-fetch existing records instead of using the session cache</span>
                  </div>
                  <button class="toggle" class:on={fresh} onclick={() => (fresh = !fresh)} type="button" aria-label="Toggle fresh start" aria-pressed={fresh}>
                    <span class="toggle-thumb"></span>
                  </button>
                </div>
              {/if}
            </div>
            {#if dryRun}
              <div class="alert alert-info">Dry run enabled — no records will be written to Teal.</div>
            {/if}
            <button class="btn-primary" onclick={startImport}>{dryRun ? 'Preview import →' : 'Start import →'}</button>
          </section>

        <!-- ── Step 4: Run ──────────────────────────────────────────────────── -->
        {:else if step === 4}
          <section class="card-section run-section">
            <div class="run-header">
              <h2 class="section-title">{runTitle}</h2>
              {#if isRunning && !result}
                <button class="btn-cancel" onclick={() => (cancelled = true)}>Stop</button>
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
                  <p class="result-detail">{result.success.toLocaleString()} record(s) published before stopping.</p>
                {:else}
                  <p class="result-label success">{dryRun ? 'Preview complete' : '✓ Done'}</p>
                  <p class="result-detail">
                    {#if dryRun}{result.success.toLocaleString()} record(s) would be imported.
                    {:else}{result.success.toLocaleString()} record(s) published successfully.{#if result.errors > 0}&nbsp;{result.errors} failed.{/if}
                    {/if}
                  </p>
                {/if}
              </div>
              <button class="btn-secondary" onclick={reset}>← Start over</button>
            {/if}
          </section>
        {/if}

      </div>
    {/key}
  </div>
</main>

<style>
  main {
    max-width: 680px;
    margin: 0 auto;
    padding: 2.5rem 1.5rem 5rem;
    min-height: 100vh;
  }

  header { margin-bottom: 2.5rem; text-align: center; }
  .logo-text {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.5rem; font-weight: 500; letter-spacing: -0.02em;
    color: var(--text); margin: 0 0 0.25rem;
  }
  .tagline { color: var(--muted); font-size: 0.875rem; margin: 0; }

  /* ── Step indicator ───────────────────────────────────────────────────────── */
  .steps { display: flex; align-items: center; justify-content: center; margin-bottom: 2rem; }
  .step-item { display: flex; flex-direction: column; align-items: center; gap: 0.25rem; flex-shrink: 0; }
  .step-dot {
    width: 28px; height: 28px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; font-weight: 500;
    border: 1.5px solid var(--border); background: var(--surface); color: var(--muted);
    transition: all 0.2s; overflow: hidden;
  }
  .dot-inner { display: flex; align-items: center; justify-content: center; }
  .step-item.done   .step-dot { background: var(--accent); border-color: var(--accent); color: #000; }
  .step-item.active .step-dot { border-color: var(--accent); color: var(--accent); box-shadow: 0 0 0 3px var(--accent-glow); }
  .step-label { font-size: 0.65rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; white-space: nowrap; }
  .step-item.active .step-label { color: var(--accent); }
  .step-line { flex: 1; height: 1.5px; background: var(--border); margin: 0 4px 18px; min-width: 16px; transition: background 0.2s; }
  .step-line.done { background: var(--accent); }
  @media (max-width: 480px) { .step-label { display: none; } }

  /* ── Slide viewport ───────────────────────────────────────────────────────── */
  .step-viewport { display: grid; overflow: hidden; }
  .step-slide { grid-area: 1 / 1; min-width: 0; }

  /* ── Mode step ────────────────────────────────────────────────────────────── */
  .mode-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(175px, 1fr));
    gap: 0.75rem; margin-bottom: 1.5rem;
  }
  .mode-card {
    background: var(--surface-2); border: 1px solid var(--border); border-radius: 8px;
    padding: 1.25rem 1rem; cursor: pointer; text-align: left;
    display: flex; flex-direction: column; gap: 0.4rem;
    transition: border-color 0.15s, background 0.15s, transform 0.1s;
  }
  .mode-card:hover {
    border-color: var(--accent);
    background: linear-gradient(135deg, var(--surface-2), rgba(63,185,104,0.06));
    transform: translateY(-1px);
  }
  .mode-icon  { color: var(--accent); display: flex; }
  .mode-title { font-size: 0.9rem; font-weight: 500; color: var(--text); }
  .mode-desc  { font-size: 0.75rem; color: var(--muted); line-height: 1.4; }
  .footer-note { text-align: center; font-size: 0.78rem; color: var(--muted); margin: 0; }
  .footer-note a { color: var(--muted); text-decoration: underline; text-underline-offset: 3px; }
  .footer-note a:hover { color: var(--accent); }
  @media (max-width: 480px) { .mode-grid { grid-template-columns: 1fr 1fr; } }

  /* ── Auth step ────────────────────────────────────────────────────────────── */
  .pw-toggle {
    position: absolute; right: 0.75rem; top: 50%; transform: translateY(-50%);
    background: none; border: none; color: var(--muted); cursor: pointer;
    display: flex; align-items: center; padding: 0;
  }
  .pw-toggle:hover { color: var(--text); }

  /* ── Files step ───────────────────────────────────────────────────────────── */
  .drop-zones {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.75rem; margin-bottom: 1.25rem;
  }
  .drop-zone {
    background: var(--surface-2); border: 1.5px dashed var(--border); border-radius: 8px;
    padding: 2rem 1.5rem; display: flex; flex-direction: column; align-items: center;
    gap: 0.4rem; cursor: pointer; transition: border-color 0.15s, background 0.15s;
    text-align: center; user-select: none;
  }
  .drop-zone:hover, .drop-zone.dragging { border-color: var(--accent); background: var(--accent-glow); }
  .drop-zone.filled { border-style: solid; border-color: var(--accent); }
  .drop-icon      { color: var(--muted); display: flex; }
  .drop-icon.drop-done { color: var(--accent); }
  .drop-title    { font-size: 0.875rem; font-weight: 500; color: var(--text); }
  .drop-filename { font-size: 0.825rem; color: var(--accent); font-family: 'JetBrains Mono', monospace; word-break: break-all; }
  .drop-meta     { font-size: 0.7rem; color: var(--muted); }
  .drop-hint     { font-size: 0.75rem; color: var(--muted); }

  /* ── Options step ─────────────────────────────────────────────────────────── */
  .options { display: flex; flex-direction: column; margin-bottom: 1.25rem; }
  .option-row {
    display: flex; align-items: center; justify-content: space-between;
    gap: 1rem; padding: 0.875rem 0; border-bottom: 1px solid var(--border);
  }
  .option-row:last-child { border-bottom: none; }
  .option-info { flex: 1; }
  .option-name { font-size: 0.875rem; color: var(--text); display: block; }
  .option-desc { font-size: 0.75rem; color: var(--muted); display: block; margin-top: 0.15rem; }
  .toggle {
    width: 40px; height: 22px; border-radius: 11px;
    background: var(--surface-2); border: 1.5px solid var(--border);
    cursor: pointer; position: relative; flex-shrink: 0;
    transition: background 0.2s, border-color 0.2s;
  }
  .toggle.on { background: var(--accent); border-color: var(--accent); }
  .toggle-thumb {
    position: absolute; width: 14px; height: 14px; border-radius: 50%;
    background: var(--muted); top: 2px; left: 2px;
    transition: transform 0.2s, background 0.2s;
  }
  .toggle.on .toggle-thumb { transform: translateX(18px); background: #000; }

  /* ── Run step ─────────────────────────────────────────────────────────────── */
  .run-section { padding: 1.5rem; }
  .run-header  { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: 1rem; }
  .run-header .section-title { margin: 0; }
  .progress-wrap { height: 3px; background: var(--surface-2); border-radius: 2px; overflow: hidden; margin-bottom: 0.4rem; }
  .progress-bar  { height: 100%; background: var(--accent); border-radius: 2px; transition: width 0.3s ease; box-shadow: 0 0 8px var(--accent); }
  .progress-text { font-size: 0.75rem; color: var(--muted); font-family: 'JetBrains Mono', monospace; margin: 0 0 0.75rem; }
  .log-terminal {
    background: #060d09; border: 1px solid var(--border); border-radius: 6px;
    padding: 0.75rem 1rem; height: 320px; overflow-y: auto;
    font-family: 'JetBrains Mono', monospace; font-size: 0.78rem; line-height: 1.65;
  }
  .log-line    { display: flex; align-items: baseline; gap: 0.625rem; word-break: break-word; }
  .log-ts      { color: var(--border); flex-shrink: 0; font-size: 0.7rem; }
  .log-section { color: var(--accent); font-weight: 500; letter-spacing: 0.04em; width: 100%; }
  .result-card { background: var(--surface-2); border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; margin-top: 1rem; text-align: center; }
  .result-card.success { border-color: rgba(63,185,104,0.4); background: var(--accent-glow); }
  .result-label { font-size: 1rem; font-weight: 600; margin: 0 0 0.25rem; }
  .result-label.success { color: var(--accent); }
  .result-label.warn    { color: var(--warn); }
  .result-label.error   { color: var(--error); }
  .result-detail { font-size: 0.875rem; color: var(--muted); margin: 0; }
</style>
