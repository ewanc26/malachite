<svelte:head>
  <title>About — Malachite</title>
  <meta name="description" content="Privacy, credits, and licence information for Malachite." />
</svelte:head>

<main>

  <a href="/" class="back">← Back</a>

  <h1>About Malachite</h1>

  <!-- ── Privacy & data ─────────────────────────────────────────────────────── -->
  <section>
    <h2>Privacy &amp; data</h2>

    <div class="card">
      <div class="pill pill-good">No tracking</div>
      <div class="pill pill-good">No accounts</div>
      <div class="pill pill-good">No server storage</div>
    </div>

    <p>
      Malachite runs entirely in your browser. Your export files are parsed locally — they are
      never uploaded to any server run by this project.
    </p>
    <p>
      The only network requests made are:
    </p>
    <ul>
      <li>
        <strong>Slingshot</strong> (<code>slingshot.microcosm.blue</code>) — resolves your
        ATProto handle to a DID and PDS URL. This is a standard identity lookup, equivalent
        to a DNS query. No personally identifiable data beyond your handle is sent.
      </li>
      <li>
        <strong>Your PDS</strong> — Malachite authenticates directly with your Personal Data
        Server and publishes records there on your behalf, exactly as any other ATProto client
        would. Your app password is used only for this request and is never stored or logged.
      </li>
      <li>
        <strong>Google Fonts</strong> — the layout loads Inter and JetBrains Mono via Google
        Fonts. If you prefer not to make this request, you can self-host the fonts or use a
        content-blocking extension.
      </li>
    </ul>
    <p>
      No analytics, no cookies, no local storage, no fingerprinting.
    </p>
  </section>

  <!-- ── Rate limits ────────────────────────────────────────────────────────── -->
  <section>
    <h2>Rate limits &amp; PDS safety</h2>
    <p>
      ATProto PDS instances enforce rate limits on write operations. Exceeding them can
      temporarily affect all users on a shared PDS. Malachite protects against this by:
    </p>
    <ul>
      <li>Reading the <code>ratelimit-*</code> headers from each response</li>
      <li>Maintaining a 15% headroom buffer before the quota ceiling</li>
      <li>Automatically adjusting batch size (up to 200 records) in real time</li>
      <li>Pausing immediately when the abort signal fires if you press Stop</li>
    </ul>
  </section>

  <!-- ── Licence ────────────────────────────────────────────────────────────── -->
  <section>
    <h2>Licence</h2>
    <p>
      Malachite is free software released under the
      <a href="https://www.gnu.org/licenses/agpl-3.0.html" target="_blank" rel="noopener">
        GNU Affero General Public License v3.0
      </a> (AGPL-3.0-only).
    </p>
    <p>
      In short: you are free to use, modify, and redistribute this software, but any modified
      version you run as a network service must also be released under the same licence with
      its source code made available.
    </p>
    <p>
      The full licence text is included in the
      <a href="https://github.com/ewanc26/malachite/blob/main/LICENCE" target="_blank" rel="noopener">
        repository
      </a>.
    </p>
  </section>

  <!-- ── Credits ───────────────────────────────────────────────────────────── -->
  <section>
    <h2>Credits</h2>

    <h3>Created by</h3>
    <div class="person-card">
      <div class="person-info">
        <span class="person-name">Ewan Croft</span>
        <span class="person-role">Author &amp; maintainer</span>
      </div>
      <div class="person-links">
        <a href="https://github.com/ewanc26" target="_blank" rel="noopener">GitHub</a>
        <a href="https://ko-fi.com/ewancroft" target="_blank" rel="noopener">Ko-fi</a>
      </div>
    </div>

    <h3>Contributors</h3>
    <p class="contrib-note">
      Contributions via
      <a href="https://github.com/ewanc26/malachite/graphs/contributors" target="_blank" rel="noopener">
        GitHub
      </a>
      are always welcome. The full contributor list is maintained there.
    </p>

    <h3>Dependencies</h3>
    <ul class="deps">
      <li>
        <a href="https://github.com/bluesky-social/atproto" target="_blank" rel="noopener">@atproto/api</a>
        — ATProto client
      </li>
      <li>
        <a href="https://svelte.dev" target="_blank" rel="noopener">Svelte / SvelteKit</a>
        — UI framework
      </li>
      <li>
        <a href="https://lucide.dev" target="_blank" rel="noopener">Lucide</a>
        — icons
      </li>
      <li>
        <a href="https://tailwindcss.com" target="_blank" rel="noopener">Tailwind CSS</a>
        — utility styles
      </li>
      <li>
        <a href="https://slingshot.microcosm.blue" target="_blank" rel="noopener">Slingshot</a>
        — ATProto identity resolution
      </li>
      <li>
        <a href="https://teal.fm" target="_blank" rel="noopener">Teal</a>
        — the <code>fm.teal.alpha</code> lexicon this tool publishes to
      </li>
    </ul>
  </section>

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
    padding: 3rem 1.5rem 5rem;
  }

  .back {
    display: inline-block;
    font-size: 0.8rem;
    color: var(--muted);
    margin-bottom: 2rem;
    text-decoration: none;
    transition: color 0.15s;
  }

  .back:hover { color: var(--text); }

  h1 {
    font-size: 1.75rem;
    font-weight: 600;
    letter-spacing: -0.03em;
    color: var(--text);
    margin: 0 0 2.5rem;
  }

  section {
    margin-bottom: 2.75rem;
    padding-bottom: 2.75rem;
    border-bottom: 1px solid var(--border);
  }

  section:last-of-type { border-bottom: none; }

  h2 {
    font-size: 1rem;
    font-weight: 500;
    color: var(--text);
    margin: 0 0 1rem;
    letter-spacing: -0.01em;
  }

  h3 {
    font-size: 0.825rem;
    font-weight: 500;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin: 1.5rem 0 0.75rem;
  }

  h3:first-of-type { margin-top: 0; }

  p {
    font-size: 0.875rem;
    color: var(--muted);
    line-height: 1.7;
    margin: 0 0 0.75rem;
  }

  ul {
    padding-left: 1.25rem;
    margin: 0 0 0.75rem;
  }

  li {
    font-size: 0.875rem;
    color: var(--muted);
    line-height: 1.7;
    margin-bottom: 0.4rem;
  }

  strong { color: var(--text); font-weight: 500; }
  code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.82em;
    color: var(--accent);
    background: var(--surface-2);
    padding: 0.1em 0.35em;
    border-radius: 3px;
  }

  /* ── Pills ──────────────────────────────────────────────────────────────── */
  .card {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 1.25rem;
  }

  .pill {
    font-size: 0.72rem;
    font-family: 'JetBrains Mono', monospace;
    padding: 0.2rem 0.6rem;
    border-radius: 999px;
    border: 1px solid;
  }

  .pill-good {
    color: var(--accent);
    border-color: rgba(63, 185, 104, 0.35);
    background: var(--accent-glow);
  }

  /* ── Person card ────────────────────────────────────────────────────────── */
  .person-card {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.875rem 1rem;
  }

  .person-info {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }

  .person-name {
    font-size: 0.9rem;
    font-weight: 500;
    color: var(--text);
  }

  .person-role {
    font-size: 0.75rem;
    color: var(--muted);
  }

  .person-links {
    display: flex;
    gap: 0.75rem;
  }

  .person-links a {
    font-size: 0.8rem;
    color: var(--muted);
    text-decoration: underline;
    text-underline-offset: 3px;
  }

  .person-links a:hover { color: var(--accent); }

  /* ── Contrib note ───────────────────────────────────────────────────────── */
  .contrib-note { font-size: 0.875rem; color: var(--muted); }

  /* ── Deps list ──────────────────────────────────────────────────────────── */
  .deps { list-style: none; padding: 0; }
  .deps li {
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--border);
  }
  .deps li:last-child { border-bottom: none; }

  /* ── Footer ─────────────────────────────────────────────────────────────── */
  footer {
    text-align: center;
    font-size: 0.78rem;
    color: var(--muted);
    margin-top: 3rem;
  }

  footer a { color: var(--muted); text-decoration: underline; text-underline-offset: 3px; }
  footer a:hover { color: var(--accent); }
  .sep { margin: 0 0.4rem; }

  @media (max-width: 480px) {
    .person-card { flex-direction: column; align-items: flex-start; }
  }
</style>
