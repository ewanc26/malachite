<script lang="ts">
  import { fly } from 'svelte/transition';
  import { cubicOut } from 'svelte/easing';
  import type { AtpAgent } from '@atproto/api';

  import { modeNeeds, stepLabelsFor } from '$lib/modes.js';
  import { runImport, type PublishProgress } from '$lib/core/import.js';
  import type { ImportMode, LogEntry } from '$lib/types.js';

  import StepIndicator from '$lib/components/StepIndicator.svelte';
  import ModeStep      from '$lib/components/steps/ModeStep.svelte';
  import AuthStep      from '$lib/components/steps/AuthStep.svelte';
  import FilesStep     from '$lib/components/steps/FilesStep.svelte';
  import OptionsStep   from '$lib/components/steps/OptionsStep.svelte';
  import RunStep       from '$lib/components/steps/RunStep.svelte';

  // ─── wizard state ────────────────────────────────────────────────────────────

  let step     = $state(0);
  let prevStep = $state(0);
  let mode     = $state<ImportMode | null>(null);

  let agent        = $state<AtpAgent | null>(null);
  let lastfmFiles  = $state<File[]>([]);
  let spotifyFiles = $state<File[]>([]);

  let dryRun       = $state(false);
  let reverseOrder = $state(false);
  let fresh        = $state(false);

  let isRunning   = $state(false);
  // Plain variable (not $state) — only ever read inside the isCancelled() closure.
  // Using $state here caused stale reads: Svelte batches signal updates to the next
  // microtask, but isCancelled() can be polled synchronously before that flush.
  let cancelled = false;
  // Separate reactive flag purely for UI feedback (shows 'Stopping…').
  let stopping = $state(false);
  let logs        = $state<LogEntry[]>([]);
  let progress    = $state<PublishProgress | null>(null);
  let result      = $state<{ success: number; errors: number; cancelled: boolean } | null>(null);
  let importError = $state<string | null>(null);

  // ─── derived ─────────────────────────────────────────────────────────────────

  let needs       = $derived(modeNeeds(mode));
  let stepLabels  = $derived(stepLabelsFor(mode));
  let goingForward = $derived(step >= prevStep);

  // ─── navigation ──────────────────────────────────────────────────────────────

  function goTo(n: number) { prevStep = step; step = n; }

  function handleSelectMode(m: ImportMode) { mode = m; goTo(1); }

  function handleBack() {
    if (step === 3 && mode === 'deduplicate') { goTo(1); return; }
    goTo(Math.max(0, step - 1));
  }

  function handleAuth(a: AtpAgent) {
    agent = a;
    goTo(needs.files ? 2 : 3);
  }

  // ─── import ──────────────────────────────────────────────────────────────────

  function addLog(level: LogEntry['level'], message: string) {
    logs = [...logs, { level, message, timestamp: Date.now() }];
  }

  async function handleStartImport() {
    if (!agent || !mode) return;
    isRunning = true; cancelled = false; stopping = false; logs = []; importError = null; result = null;
    goTo(4);
    try {
      result = await runImport(
        agent,
        mode,
        lastfmFiles,
        spotifyFiles,
        { dryRun, reverseOrder, fresh },
        {
          onLog:       addLog,
          onProgress:  (p) => { progress = p; },
          isCancelled: () => cancelled,
        },
      );
      const n = result.success.toLocaleString();
      if (mode === 'deduplicate') {
        if (result.cancelled)  addLog('warn',    `Stopped. ${n} duplicate(s) removed.`);
        else if (dryRun)       addLog('success', `Dry run complete — ${n} duplicate(s) would be removed.`);
        else                   addLog('success', `Done! ${n} duplicate(s) removed.`);
      } else {
        if (result.cancelled)  addLog('warn',    `Stopped. ${n} record(s) published.`);
        else if (dryRun)       addLog('success', `Dry run complete — ${n} record(s) would be imported.`);
        else {
          addLog('success', `Import complete! ${n} record(s) published.`);
          if (result.errors > 0) addLog('warn', `${result.errors} record(s) failed.`);
        }
      }
    } catch (err: any) {
      importError = err.message ?? 'An unexpected error occurred';
      addLog('error', `Fatal: ${importError}`);
    } finally {
      isRunning = false;
      stopping  = false;
    }
  }

  function handleReset() {
    prevStep = step; step = 0; mode = null; agent = null;
    lastfmFiles = []; spotifyFiles = [];
    dryRun = false; reverseOrder = false; fresh = false;
    logs = []; progress = null; result = null; importError = null;
    cancelled = false; stopping = false;
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

  {#if step < 5}
    <StepIndicator {step} {stepLabels} />
  {/if}

  <div class="step-viewport">
    {#key step}
      <div
        class="step-slide"
        in:fly={{ x: goingForward ? 40 : -40, duration: 280, easing: cubicOut }}
        out:fly={{ x: goingForward ? -40 : 40, duration: 200, easing: cubicOut }}
      >
        {#if step === 0}
          <ModeStep onselect={handleSelectMode} />

        {:else if step === 1}
          <AuthStep onauth={handleAuth} onback={handleBack} />

        {:else if step === 2}
          <FilesStep
            bind:lastfmFiles
            bind:spotifyFiles
            {needs}
            oncontinue={() => goTo(3)}
            onback={handleBack}
          />

        {:else if step === 3}
          <OptionsStep
            {mode}
            bind:dryRun
            bind:reverseOrder
            bind:fresh
            onstartimport={handleStartImport}
            onback={handleBack}
          />

        {:else if step === 4}
          <RunStep
            {isRunning}
            {stopping}
            {mode}
            {dryRun}
            {logs}
            {progress}
            {result}
            {importError}
            oncancel={() => { cancelled = true; stopping = true; }}
            onreset={handleReset}
          />
        {/if}
      </div>
    {/key}
  </div>
  <footer>
    <a href="https://github.com/ewanc26/malachite" target="_blank" rel="noopener">↗ View on GitHub</a>
    <span class="sep">·</span>
    <a href="https://ko-fi.com/ewancroft" target="_blank" rel="noopener">♥ Support Malachite</a>
  </footer>
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
    font-size: 1.5rem;
    font-weight: 500;
    letter-spacing: -0.02em;
    color: var(--text);
    margin: 0 0 0.25rem;
  }

  .tagline { color: var(--muted); font-size: 0.875rem; margin: 0; }

  .step-viewport { display: grid; overflow: hidden; }
  .step-slide    { grid-area: 1 / 1; min-width: 0; }

  footer {
    text-align: center;
    font-size: 0.78rem;
    color: var(--muted);
    margin-top: 2rem;
  }

  footer a { color: var(--muted); text-decoration: underline; text-underline-offset: 3px; }
  footer a:hover { color: var(--accent); }
  .sep { margin: 0 0.4rem; }
</style>
