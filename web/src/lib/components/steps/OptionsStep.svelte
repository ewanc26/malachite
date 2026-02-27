<script lang="ts">
  import type { ImportMode } from '$lib/types.js';

  let {
    mode,
    dryRun       = $bindable(false),
    reverseOrder = $bindable(false),
    fresh        = $bindable(false),
    onstartimport,
    onback,
  }: {
    mode: ImportMode | null;
    dryRun: boolean;
    reverseOrder: boolean;
    fresh: boolean;
    onstartimport: () => void;
    onback: () => void;
  } = $props();
</script>

<section class="card-section">
  <button class="back-btn" onclick={onback}>← Back</button>
  <h2 class="section-title">Import options</h2>

  <div class="options">
    <div class="option-row">
      <div class="option-info">
        <span class="option-name">Dry run</span>
        <span class="option-desc">Preview what would be imported without making changes</span>
      </div>
      <button
        class="toggle"
        class:on={dryRun}
        onclick={() => (dryRun = !dryRun)}
        type="button"
        aria-label="Toggle dry run"
        aria-pressed={dryRun}
      >
        <span class="toggle-thumb"></span>
      </button>
    </div>

    {#if mode !== 'deduplicate'}
      <div class="option-row">
        <div class="option-info">
          <span class="option-name">Reverse order</span>
          <span class="option-desc">Process newest records first (default: oldest first)</span>
        </div>
        <button
          class="toggle"
          class:on={reverseOrder}
          onclick={() => (reverseOrder = !reverseOrder)}
          type="button"
          aria-label="Toggle reverse order"
          aria-pressed={reverseOrder}
        >
          <span class="toggle-thumb"></span>
        </button>
      </div>

      <div class="option-row">
        <div class="option-info">
          <span class="option-name">Fresh start</span>
          <span class="option-desc">Re-fetch existing records instead of using the session cache</span>
        </div>
        <button
          class="toggle"
          class:on={fresh}
          onclick={() => (fresh = !fresh)}
          type="button"
          aria-label="Toggle fresh start"
          aria-pressed={fresh}
        >
          <span class="toggle-thumb"></span>
        </button>
      </div>
    {/if}
  </div>

  {#if dryRun}
    <div class="alert alert-info">Dry run enabled — no records will be written to Teal.</div>
  {/if}

  <button class="btn-primary" onclick={onstartimport}>
    {dryRun ? 'Preview import →' : 'Start import →'}
  </button>
</section>

<style>
  .options {
    display: flex;
    flex-direction: column;
    margin-bottom: 1.25rem;
  }

  .option-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 0.875rem 0;
    border-bottom: 1px solid var(--border);
  }

  .option-row:last-child { border-bottom: none; }

  .option-info  { flex: 1; }
  .option-name  { font-size: 0.875rem; color: var(--text); display: block; }
  .option-desc  { font-size: 0.75rem; color: var(--muted); display: block; margin-top: 0.15rem; }

  .toggle {
    width: 40px;
    height: 22px;
    border-radius: 11px;
    background: var(--surface-2);
    border: 1.5px solid var(--border);
    cursor: pointer;
    position: relative;
    flex-shrink: 0;
    transition: background 0.2s, border-color 0.2s;
  }

  .toggle.on { background: var(--accent); border-color: var(--accent); }

  .toggle-thumb {
    position: absolute;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background: var(--muted);
    top: 2px;
    left: 2px;
    transition: transform 0.2s, background 0.2s;
  }

  .toggle.on .toggle-thumb { transform: translateX(18px); background: #000; }
</style>
