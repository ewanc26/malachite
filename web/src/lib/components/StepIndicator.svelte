<script lang="ts">
  import { scale } from 'svelte/transition';
  import { backOut } from 'svelte/easing';
  import { Check } from '@lucide/svelte';

  let {
    step,
    stepLabels,
  }: {
    step: number;
    stepLabels: string[];
  } = $props();
</script>

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

<style>
  .steps {
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 2rem;
  }

  .step-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.25rem;
    flex-shrink: 0;
  }

  .step-dot {
    width: 28px;
    height: 28px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.75rem;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
    border: 1.5px solid var(--border);
    background: var(--surface);
    color: var(--muted);
    transition: all 0.2s;
    overflow: hidden;
  }

  .dot-inner {
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .step-item.done .step-dot  { background: var(--accent); border-color: var(--accent); color: #000; }
  .step-item.active .step-dot { border-color: var(--accent); color: var(--accent); box-shadow: 0 0 0 3px var(--accent-glow); }

  .step-label {
    font-size: 0.65rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    white-space: nowrap;
  }

  .step-item.active .step-label { color: var(--accent); }

  .step-line {
    flex: 1;
    height: 1.5px;
    background: var(--border);
    margin: 0 4px 18px;
    min-width: 16px;
    transition: background 0.2s;
  }

  .step-line.done { background: var(--accent); }

  @media (max-width: 480px) {
    .step-label { display: none; }
  }
</style>
