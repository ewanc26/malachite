<script lang="ts">
  import { MODES } from '$lib/modes.js';
  import type { ImportMode } from '$lib/types.js';

  let {
    onselect,
  }: {
    onselect: (mode: ImportMode) => void;
  } = $props();
</script>

<section class="card-section">
  <h2 class="section-title">What would you like to do?</h2>
  <div class="mode-grid">
    {#each MODES as m}
      <button class="mode-card" onclick={() => onselect(m.id)}>
        <span class="mode-icon"><m.icon size={20} /></span>
        <span class="mode-title">{m.title}</span>
        <span class="mode-desc">{m.description}</span>
      </button>
    {/each}
  </div>
</section>

<style>
  .mode-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(175px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1.5rem;
  }

  .mode-card {
    background: var(--surface-2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.25rem 1rem;
    cursor: pointer;
    text-align: left;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    transition: border-color 0.15s, background 0.15s, transform 0.1s;
  }

  .mode-card:hover {
    border-color: var(--accent);
    background: linear-gradient(135deg, var(--surface-2), rgba(63, 185, 104, 0.06));
    transform: translateY(-1px);
  }

  .mode-icon  { color: var(--accent); display: flex; }
  .mode-title { font-size: 0.9rem; font-weight: 500; color: var(--text); }
  .mode-desc  { font-size: 0.75rem; color: var(--muted); line-height: 1.4; }

  @media (max-width: 480px) {
    .mode-grid { grid-template-columns: 1fr 1fr; }
  }
</style>
