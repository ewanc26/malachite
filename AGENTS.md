# AGENTS.md

Guidance for agents working on Malachite. This repository is archived; treat it as historical source unless the user explicitly requests reactivation. Canonical maintained functionality may live in successor packages.

## Structure

- `src/` contains the TypeScript service/CLI and tests.
- `packages/` contains workspace packages with their own build boundaries.
- `lexicons/` defines AT Protocol records; `scripts/` contains operational checks.
- `web/` is a separate presentation surface.

## Rules

- Prefer making fixes in the documented successor repository when applicable; do not create parallel maintained implementations here.
- Use pnpm for workspace work and preserve existing lockfiles.
- Keep lexicon schemas and generated bindings aligned; preserve AT Protocol identifiers and record compatibility.
- Dry-run behavior must never perform writes. Rate-limit monitoring must not expose credentials.
- Avoid dependency upgrades or broad refactors in archived code unless needed for a requested archival/security fix.

## Validation

For an intentional change run `pnpm run type-check`, `pnpm test`, and `pnpm run build:all`; use `pnpm run dry-run` with non-production configuration when behavior needs exercising. Verify no write occurs in dry-run and do not commit credentials, `dist/`, or logs.
