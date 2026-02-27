<script lang="ts">
  import { Eye, EyeOff } from '@lucide/svelte';
  import { login } from '$lib/core/auth.js';
  import type { AtpAgent } from '@atproto/api';

  let {
    onauth,
    onback,
  }: {
    onauth: (agent: AtpAgent) => void;
    onback: () => void;
  } = $props();

  let handle      = $state('');
  let password    = $state('');
  let pdsOverride = $state('');

  let showPassword = $state(false);
  let showAdvanced = $state(false);
  let authError    = $state<string | null>(null);
  let authLoading  = $state(false);

  async function doAuth() {
    authError   = null;
    authLoading = true;
    try {
      onauth(await login(handle.trim(), password, pdsOverride.trim() || undefined));
    } catch (err: any) {
      authError = err.message ?? 'Login failed';
    } finally {
      authLoading = false;
    }
  }
</script>

<section class="card-section">
  <button class="back-btn" onclick={onback}>← Back</button>
  <h2 class="section-title">Sign in to ATProto</h2>
  <p class="section-sub">
    Use your Bluesky handle and an
    <a href="https://bsky.app/settings/app-passwords" target="_blank" rel="noopener">app password</a>.
  </p>

  <div class="form">
    <label class="field">
      <span class="field-label">Handle</span>
      <input
        type="text"
        bind:value={handle}
        placeholder="you.bsky.social"
        autocomplete="username"
        spellcheck="false"
      />
    </label>

    <label class="field">
      <span class="field-label">App password</span>
      <div class="password-wrap">
        <input
          type={showPassword ? 'text' : 'password'}
          bind:value={password}
          placeholder="xxxx-xxxx-xxxx-xxxx"
          autocomplete="current-password"
        />
        <button
          class="pw-toggle"
          onclick={() => (showPassword = !showPassword)}
          type="button"
          aria-label={showPassword ? 'Hide password' : 'Show password'}
        >
          {#if showPassword}<EyeOff size={14} />{:else}<Eye size={14} />{/if}
        </button>
      </div>
    </label>

    <button
      class="expand-btn"
      onclick={() => (showAdvanced = !showAdvanced)}
      type="button"
    >
      {showAdvanced ? '▾' : '▸'} Advanced options
    </button>

    {#if showAdvanced}
      <label class="field">
        <span class="field-label">PDS URL override <span class="badge">optional</span></span>
        <input
          type="url"
          bind:value={pdsOverride}
          placeholder="https://your.pds.example"
          spellcheck="false"
        />
        <span class="field-hint">
          Skip Slingshot identity resolution and connect directly to your PDS.
        </span>
      </label>
    {/if}

    {#if authError}
      <div class="alert alert-error">{authError}</div>
    {/if}

    <button
      class="btn-primary"
      onclick={doAuth}
      disabled={authLoading || !handle || !password}
    >
      {#if authLoading}<span class="spinner"></span> Signing in…{:else}Sign in →{/if}
    </button>
  </div>
</section>

<style>
  .pw-toggle {
    position: absolute;
    right: 0.75rem;
    top: 50%;
    transform: translateY(-50%);
    background: none;
    border: none;
    color: var(--muted);
    cursor: pointer;
    display: flex;
    align-items: center;
    padding: 0;
  }

  .pw-toggle:hover { color: var(--text); }
</style>
