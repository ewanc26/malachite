# AGENTS.md

Guidance for the archived Malachite repository. Active development moved, with history, to `ewanc26/pkgs`: the CLI is `packages/malachite/`, the web app is `packages/malachite-web/`, and TID support is `packages/tid/`. Unless the user explicitly asks for an archival or security repair here, make the change in that monorepo instead.

## Historical layout

- `src/index.ts` enters the Node CLI through `src/lib/cli.ts`. Environment-neutral import, CAR sync, merge, publishing, rate-limit, CSV, Spotify, and TID logic lives mainly in `src/core/`; `src/lib/` adds terminal-facing orchestration.
- `src/tests/` is compiled with the application and run from `dist/tests/` using Node's test runner. CLI state and encrypted app-password storage are managed under the platform-specific Malachite state directory by `src/utils/`.
- `web/` is a separate SvelteKit 2/Svelte 5 browser importer using AT Protocol OAuth. It mirrors and aliases core code, performs file parsing and repository writes in the browser, and has its own package metadata and lockfile.
- `packages/tid/` is the historical zero-runtime-dependency TID package. `lexicons/fm.teal.alpha/` describes the play, actor, and stats contracts; it is protocol surface, not incidental JSON.
- `scripts/rate-limit-monitor.js` is an operational helper. Root `package.json`, `src/config.ts`, and web metadata contain separately maintained version/client strings; keep them aligned if an explicitly requested release repair touches them.

## Safety and compatibility

- Imports write `fm.teal.alpha.feed.play` records, and deduplication can delete records. Dry-run must cover every create and delete path, cancellation must stop subsequent writes, and retry/fallback logic must not turn a partial failure into duplicate publication.
- Preserve record-key/TID compatibility, timestamp ordering, Last.fm and Spotify normalization, and the exact deduplication key. The in-memory existing-record cache is per DID; force-refresh and session changes must not reuse another account's state.
- CAR export is the preferred existing-record check. Its fallback intentionally proceeds to `applyWrites`, where already-existing rkeys may fail while new records land; report partial outcomes honestly and retain rate-limit headroom.
- The CLI can persist an encrypted app password and import progress locally. Never log credentials/tokens, weaken file permissions, commit state files, or describe machine-derived encryption as protection from an attacker with access to the same account and machine.
- The web client uses OAuth scope `atproto transition:generic`, browser session storage, user-selected history files, and production metadata in `web/static/client-metadata.json`. Keep redirect URIs, client ID, scopes, and deployment origin in sync. Do not add server secrets to browser code.
- Treat the lexicons as published compatibility contracts. Coordinate schema changes with readers, writers, generated types, tests, and the maintained monorepo; do not fork the protocol accidentally in this archived copy.

## Working and validation

- The workspace is pnpm-based (`pnpm-lock.yaml`, `pnpm-workspace.yaml`), although some root scripts deliberately invoke npm internally. Do not replace those commands or update the separate `web/pnpm-lock.yaml` incidentally.
- For a sanctioned root change run `pnpm run type-check`, `pnpm test`, and `pnpm run build:all`. Exercise `pnpm run dry-run` only with disposable/non-production credentials and confirm it performs no create or delete.
- For web-only work, run `pnpm --dir web check`, `pnpm --dir web lint`, and `pnpm --dir web build`; manually test OAuth callback/restoration, Last.fm and Spotify inputs, cancellation, CAR failure, rate limiting, deduplicate dry-run, and partial writes.
- Do not commit `.env`, credentials, import state, listening-history exports, OAuth tokens, `dist/`, SvelteKit/Vercel output, or logs. Preserve the repository's archived notices and AGPL licensing.
