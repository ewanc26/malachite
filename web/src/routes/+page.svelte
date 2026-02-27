<script lang="ts">
  import { Music2, Disc3, Layers2, RefreshCw, ListFilter, ArrowRight, Github } from '@lucide/svelte';
</script>

<svelte:head>
  <title>Malachite — Import your music history to Teal</title>
  <meta name="description" content="Import your Last.fm and Spotify listening history into Teal on ATProto. Free, open-source, runs in your browser." />
  <link rel="canonical" href="https://malachite.croft.click" />

  <!-- Open Graph -->
  <meta property="og:type" content="website" />
  <meta property="og:url" content="https://malachite.croft.click" />
  <meta property="og:title" content="Malachite — Import your music history to Teal" />
  <meta property="og:description" content="Import your Last.fm and Spotify listening history into Teal on ATProto. Free, open-source, runs in your browser." />
  <meta property="og:image" content="https://malachite.croft.click/og-home.svg" />

  <!-- Twitter / X card -->
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="Malachite — Import your music history to Teal" />
  <meta name="twitter:description" content="Import your Last.fm and Spotify listening history into Teal on ATProto. Free, open-source, runs in your browser." />
  <meta name="twitter:image" content="https://malachite.croft.click/og-home.svg" />
</svelte:head>

<main>

  <!-- ── Hero ───────────────────────────────────────────────────────────────── -->
  <section class="hero">
    <p class="eyebrow">Open source · Runs in your browser</p>
    <h1>Your listening history,<br />on ATProto.</h1>
    <p class="sub">
      Malachite imports your Last.fm scrobbles and Spotify plays into
      <a href="https://teal.fm" target="_blank" rel="noopener">Teal</a>
      — the open music layer for the AT Protocol.
    </p>
    <div class="hero-actions">
      <a href="/import" class="btn-primary">
        Start importing <ArrowRight size={16} />
      </a>
      <a href="https://github.com/ewanc26/malachite" target="_blank" rel="noopener" class="btn-ghost">
        <Github size={15} /> View on GitHub
      </a>
    </div>
  </section>

  <!-- ── Features ───────────────────────────────────────────────────────────── -->
  <section class="features">
    <div class="feature-card">
      <span class="feature-icon"><Music2 size={20} /></span>
      <h3>Last.fm</h3>
      <p>Import your full scrobble history from a Last.fm CSV data export.</p>
    </div>
    <div class="feature-card">
      <span class="feature-icon"><Disc3 size={20} /></span>
      <h3>Spotify</h3>
      <p>Import extended streaming history from Spotify's JSON data export.</p>
    </div>
    <div class="feature-card">
      <span class="feature-icon"><Layers2 size={20} /></span>
      <h3>Combined</h3>
      <p>Merge both sources together with smart deduplication to avoid double-counting plays.</p>
    </div>
    <div class="feature-card">
      <span class="feature-icon"><RefreshCw size={20} /></span>
      <h3>Sync</h3>
      <p>Only import records not already in Teal — safe to re-run any time.</p>
    </div>
    <div class="feature-card">
      <span class="feature-icon"><ListFilter size={20} /></span>
      <h3>Deduplicate</h3>
      <p>Find and remove duplicate records already in your Teal account.</p>
    </div>
  </section>

  <!-- ── How it works ───────────────────────────────────────────────────────── -->
  <section class="how">
    <h2>How it works</h2>
    <ol class="steps-list">
      <li>
        <span class="step-num">1</span>
        <div>
          <strong>Choose a mode</strong>
          <p>Pick Last.fm, Spotify, combined, sync, or deduplicate.</p>
        </div>
      </li>
      <li>
        <span class="step-num">2</span>
        <div>
          <strong>Sign in with ATProto</strong>
          <p>Use your Bluesky handle and an app password. Nothing is stored.</p>
        </div>
      </li>
      <li>
        <span class="step-num">3</span>
        <div>
          <strong>Upload your export</strong>
          <p>Drop in your CSV or JSON files — everything is processed locally in your browser.</p>
        </div>
      </li>
      <li>
        <span class="step-num">4</span>
        <div>
          <strong>Import</strong>
          <p>Malachite publishes your records to your PDS with automatic rate-limit handling.</p>
        </div>
      </li>
    </ol>
  </section>

  <!-- ── CTA ────────────────────────────────────────────────────────────────── -->
  <section class="cta">
    <h2>Ready?</h2>
    <p>No account needed. No data leaves your browser except to your own PDS.</p>
    <a href="/import" class="btn-primary">
      Open the import tool <ArrowRight size={16} />
    </a>
  </section>

  <footer>
    <a href="/about">About &amp; privacy</a>
    <span class="sep">·</span>
    <a href="https://github.com/ewanc26/malachite" target="_blank" rel="noopener">↗ GitHub</a>
    <span class="sep">·</span>
    <a href="https://ko-fi.com/ewancroft" target="_blank" rel="noopener">♥ Support</a>
  </footer>

</main>

<style>
  main {
    max-width: 720px;
    margin: 0 auto;
    padding: 4rem 1.5rem 5rem;
  }

  /* ── Hero ─────────────────────────────────────────────────────────────────── */
  .hero {
    text-align: center;
    padding: 3rem 0 3.5rem;
  }

  .eyebrow {
    font-size: 0.75rem;
    font-family: 'JetBrains Mono', monospace;
    color: var(--accent);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin: 0 0 1.25rem;
  }

  h1 {
    font-size: clamp(2rem, 5vw, 3rem);
    font-weight: 600;
    line-height: 1.15;
    letter-spacing: -0.03em;
    color: var(--text);
    margin: 0 0 1.25rem;
  }

  .sub {
    font-size: 1.05rem;
    color: var(--muted);
    line-height: 1.6;
    max-width: 480px;
    margin: 0 auto 2.25rem;
  }

  .sub a { color: var(--accent); }

  .hero-actions {
    display: flex;
    gap: 0.75rem;
    justify-content: center;
    flex-wrap: wrap;
  }

  /* ── Buttons ──────────────────────────────────────────────────────────────── */
  .btn-primary {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: var(--accent);
    color: #000;
    font-weight: 600;
    font-size: 0.9rem;
    padding: 0.65rem 1.25rem;
    border-radius: 6px;
    text-decoration: none;
    transition: background 0.15s, transform 0.1s;
  }

  .btn-primary:hover { background: #48d47a; transform: translateY(-1px); text-decoration: none; }

  .btn-ghost {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: var(--surface);
    color: var(--muted);
    font-size: 0.875rem;
    padding: 0.65rem 1.125rem;
    border-radius: 6px;
    border: 1px solid var(--border);
    text-decoration: none;
    transition: border-color 0.15s, color 0.15s;
  }

  .btn-ghost:hover { border-color: var(--accent); color: var(--text); text-decoration: none; }

  /* ── Features ─────────────────────────────────────────────────────────────── */
  .features {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(190px, 1fr));
    gap: 0.75rem;
    margin-bottom: 4rem;
  }

  .feature-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1.25rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .feature-icon { color: var(--accent); display: flex; }

  .feature-card h3 {
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--text);
    margin: 0;
  }

  .feature-card p {
    font-size: 0.8rem;
    color: var(--muted);
    line-height: 1.5;
    margin: 0;
  }

  /* ── How it works ─────────────────────────────────────────────────────────── */
  .how {
    margin-bottom: 4rem;
  }

  h2 {
    font-size: 1.25rem;
    font-weight: 500;
    color: var(--text);
    margin: 0 0 1.5rem;
    letter-spacing: -0.02em;
  }

  .steps-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0;
  }

  .steps-list li {
    display: flex;
    gap: 1.25rem;
    align-items: flex-start;
    padding: 1.25rem 0;
    border-bottom: 1px solid var(--border);
  }

  .steps-list li:last-child { border-bottom: none; }

  .step-num {
    width: 28px;
    height: 28px;
    border-radius: 50%;
    background: var(--surface);
    border: 1.5px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.75rem;
    font-family: 'JetBrains Mono', monospace;
    color: var(--accent);
    flex-shrink: 0;
    margin-top: 1px;
  }

  .steps-list strong {
    display: block;
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--text);
    margin-bottom: 0.2rem;
  }

  .steps-list p {
    font-size: 0.825rem;
    color: var(--muted);
    line-height: 1.5;
    margin: 0;
  }

  /* ── CTA ──────────────────────────────────────────────────────────────────── */
  .cta {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 2.5rem;
    text-align: center;
    margin-bottom: 3rem;
  }

  .cta h2 { margin-bottom: 0.5rem; }

  .cta p {
    font-size: 0.875rem;
    color: var(--muted);
    margin: 0 0 1.75rem;
  }

  /* ── Footer ───────────────────────────────────────────────────────────────── */
  footer {
    text-align: center;
    font-size: 0.78rem;
    color: var(--muted);
  }

  footer a { color: var(--muted); text-decoration: underline; text-underline-offset: 3px; }
  footer a:hover { color: var(--accent); }
  .sep { margin: 0 0.4rem; }

  /* ── Responsive ───────────────────────────────────────────────────────────── */
  @media (max-width: 480px) {
    .hero { padding: 1.5rem 0 2.5rem; }
    .features { grid-template-columns: 1fr 1fr; }
    .cta { padding: 1.75rem 1.25rem; }
  }
</style>
