# Contributing to Malachite

Thanks for wanting to help. This document covers the layout of the project, how to get a dev environment running, and what to keep in mind when opening a PR.

## Table of Contents

- [Project layout](#project-layout)
- [Prerequisites](#prerequisites)
- [Getting started](#getting-started)
- [Running the CLI](#running-the-cli)
- [Running the web app](#running-the-web-app)
- [Running tests](#running-tests)
- [Code architecture](#code-architecture)
- [Making changes](#making-changes)
- [Opening a pull request](#opening-a-pull-request)
- [Publishing `@ewanc26/tid`](#publishing-ewanc26tid)

---

## Project layout

This is a pnpm monorepo with three separate projects:

```
malachite/
├── src/               # CLI — TypeScript, compiled to dist/ by tsc
│   ├── core/          # Environment-agnostic logic (shared with web)
│   ├── lib/           # CLI wrappers around core (Node.js-specific)
│   └── utils/         # Legacy CLI utilities (gradually migrating to core/)
├── web/               # SvelteKit web app
│   └── src/lib/
│       ├── core/      # Thin re-exports of src/core/ via the $core alias
│       └── ...        # Svelte components, routes, config
├── packages/
│   └── tid/           # @ewanc26/tid — standalone npm package
└── lexicons/          # fm.teal.alpha lexicon definitions
```

The key architectural rule is that **`src/core/` is the single source of truth** for all non-UI logic. The CLI wrappers in `src/lib/` adapt it for terminal use (spinners, file I/O, credentials). The web app re-exports it via `$core` path alias — there should be no duplicated logic between the two surfaces.

---

## Prerequisites

- **Node.js 20+** (required for Web Crypto API via `globalThis.crypto`)
- **pnpm 9+** — install with `npm i -g pnpm` if you don't have it

---

## Getting started

```sh
# Clone and install everything in one shot — pnpm workspaces handles all three projects
git clone https://github.com/ewanc26/malachite
cd malachite
pnpm install
```

---

## Running the CLI

```sh
# Build
pnpm build

# Dry run against a real Last.fm export
node dist/index.js -i my-export.csv --dry-run

# Interactive mode
node dist/index.js

# Rebuild and run in one step
pnpm dev
```

For development work it's worth using `--dev` mode, which enables verbose logging and caps batch sizes to 20 records so you're not waiting through thousands of API calls:

```sh
node dist/index.js -i my-export.csv --dev --dry-run
```

---

## Running the web app

```sh
cd web
pnpm dev   # starts at http://127.0.0.1:5173
```

The dev server **must** run on `127.0.0.1:5173` exactly — the ATProto OAuth loopback `redirect_uri` is pinned to that origin per RFC 8252. Don't change the host or port without also updating `web/src/lib/core/oauth.ts`.

For changes to shared `src/core/` files, the web app picks them up immediately via the `$core` alias — no separate build step needed.

---

## Running tests

Tests use Node's built-in test runner — no Jest, no Vitest.

```sh
# Build first, then run all tests
pnpm test

# Run just the TID tests
pnpm test:tid

# Watch mode
pnpm test:watch
```

Tests live in `src/tests/`. If you're adding a new file to `src/core/`, add a corresponding test file there. The existing tests are a good reference for the style — `node:test` + `node:assert`, no extra dependencies.

---

## Code architecture

### The `src/core/` contract

Any code that lives in `src/core/` must be:

- **Zero Node.js dependencies** — no `fs`, `path`, `crypto` module, etc. Web Crypto (`globalThis.crypto`) is fine because Node 20+ and all modern browsers support it.
- **Callback-based for progress/logging** — functions accept optional `onProgress` callbacks rather than calling `console.log` directly.
- **Tested** — new files should have coverage in `src/tests/`.

If you need Node.js-specific behaviour (file I/O, terminal spinners, credential storage), put it in `src/lib/` as a thin wrapper that calls into `src/core/`.

### Adding something to the web

The web's `web/src/lib/core/` files are almost all one-liners:

```ts
export * from '$core/your-new-file.js';
```

The only exceptions are `csv.ts` and `spotify.ts`, which add browser `File` API loaders on top of the core parsers, and `oauth.ts` / `import.ts`, which are web-only. Follow that same pattern for anything new.

### Rate limiting

The rate limiter in `src/core/rate-limiter.ts` learns quota from response headers on the first successful batch and then gates all subsequent batches. If you're changing publish behaviour, keep the 15% headroom buffer intact — exceeding the PDS daily limit affects every user on a shared instance, not just the one importing.

### TIDs

Record keys are generated from `playedTime` using the TID clock in `src/core/tid.ts`. The clock is monotonic — even if records arrive out of order, every call produces a strictly increasing TID within the same process/page. Don't make the generation async (it isn't) and don't reset `lastUs` outside of tests.

---

## Making changes

### Bugfixes

Open a PR against `main` with a description of what was broken and how you've verified the fix. A failing test that now passes is ideal.

### New features

Open an issue first if the change is significant — it's worth a quick discussion before writing a lot of code. For smaller additions (a new CLI flag, a missing field in the data mapping, an extra export from `@ewanc26/tid`) just open the PR directly.

### Changing shared core logic

Changes to `src/core/` affect all three surfaces (CLI, web, `@ewanc26/tid` if relevant). Run both `pnpm test` and `cd web && pnpm check` before submitting to make sure nothing is broken on either side.

### Style

- TypeScript strict mode is on — no `any` without a comment explaining why.
- No new runtime dependencies in `src/core/` or `packages/tid/`.
- Prefer named exports over default exports.
- Keep comments on the *why*, not the *what*.

---

## Opening a pull request

1. Fork the repo and create a branch from `main`.
2. Make your changes, including tests where applicable.
3. Verify `pnpm test` passes and `pnpm run type-check` is clean.
4. If you touched the web app, verify `cd web && pnpm check` is clean too.
5. Open a PR with a clear description of what changed and why.

There's no formal CLA or contributor agreement — AGPL-3.0 covers contributions automatically.

---

## Publishing `@ewanc26/tid`

The `packages/tid/` directory is the source for the [`@ewanc26/tid`](https://www.npmjs.com/package/@ewanc26/tid) package. It is versioned and published independently from the main CLI.

To cut a new release:

```sh
cd packages/tid

# Bump the version in package.json, then:
pnpm build
npm publish --access public --otp=<your-2fa-code>
```

The package has no runtime dependencies and must stay that way. If a change to `src/core/tid.ts` affects the public API of the package, update `packages/tid/src/index.ts` to match and bump the version accordingly (semver — patch for fixes, minor for new exports, major for breaking changes).
