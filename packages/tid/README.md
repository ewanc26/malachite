# @ewanc26/tid

> **Canonical source:** This package is now maintained in the [`@ewanc26/pkgs`](https://github.com/ewanc26/pkgs) monorepo under [`packages/tid`](https://github.com/ewanc26/pkgs/tree/main/packages/tid). This copy exists for historical context — please open issues and PRs there.

Zero-dependency [AT Protocol](https://atproto.com/) TID (Timestamp Identifier) generation for Node.js and browsers.

This package is **written in TypeScript** and compiled to **plain JavaScript**, so it ships with type definitions for TypeScript users and runs anywhere the Web Crypto API is available — Node.js 20+, Deno, Bun, and modern browsers.

TIDs are 13-character, lexicographically sortable record keys used across the AT Protocol and Bluesky. They're monotonic identifiers derived from a microsecond timestamp and a 5-bit clock ID. When multiple TIDs would otherwise share the same microsecond, this package avoids collisions by nudging the clock ID (initialised per JS context) so each generated TID stays unique and strictly increasing within that runtime.

---

## Why this package?

Other TID implementations either require native bindings (e.g. `node-gyp`) or pull in large dependency trees. This package is **pure JavaScript**, has **no runtime dependencies**, ships with `.d.ts` typings, and is intentionally tiny — ideal for libraries, servers and client code where bundle size and portability matter.

---

## Install

```bash
npm install @ewanc26/tid
# or
pnpm add @ewanc26/tid
```

---

## Usage

### TypeScript (recommended)

```ts
import {
  generateTID,
  generateNextTID,
  validateTid,
  decodeTid,
  compareTids,
} from '@ewanc26/tid';

// From ISO string or Date
const tid: string = generateTID('2023-11-01T12:00:00Z');
const tid2: string = generateTID(new Date('2024-03-15T09:30:00Z'));

// Now
const currentTid: string = generateNextTID();

// Validate
const ok: boolean = validateTid('3jzfcijpj2z2a');

// Decode
const decoded = decodeTid('3jzfcijpj2z2a');
console.log(decoded.timestampUs, decoded.clockId, decoded.date);

// Sort
const tids: string[] = ['3jzfcijpj2z2a', '3jzfabc000022', '3jzfzzzzzzz2a'];
tids.sort(compareTids);
```

### JavaScript (ESM)

```js
import {
  generateTID,
  generateNextTID,
  validateTid,
  decodeTid,
  compareTids,
} from '@ewanc26/tid';

const tid = generateTID('2023-11-01T12:00:00Z');
const currentTid = generateNextTID();
console.log(validateTid(tid));
```

### JavaScript (CommonJS)

```js
const {
  generateTID,
  generateNextTID,
  validateTid,
  decodeTid,
  compareTids,
} = require('@ewanc26/tid');

const tid = generateTID('2023-11-01T12:00:00Z');
```

---

## API

| Export            | Signature                     | Description                                       |                                           |                                      |
| ----------------- | ----------------------------- | ------------------------------------------------- | ----------------------------------------- | ------------------------------------ |
| `generateTID`     | `(source: string              | Date) => string`                                  | Generate a TID for a historical timestamp |                                      |
| `generateNextTID` | `() => string`                | Generate a TID for the current wall-clock time    |                                           |                                      |
| `validateTid`     | `(tid: string) => boolean`    | Returns `true` if the string is a well-formed TID |                                           |                                      |
| `decodeTid`       | `(tid: string) => DecodedTid` | Decode a TID into timestamp, clockId, and Date    |                                           |                                      |
| `compareTids`     | `(a: string, b: string) => -1 | 0                                                 | 1`                                        | Lexicographic comparator for sorting |
| `resetTidClock`   | `() => void`                  | Reset the monotonic clock (**tests only**)        |                                           |                                      |

### `DecodedTid`

```ts
interface DecodedTid {
  timestampUs: number; // microseconds since Unix epoch
  clockId: number;     // 0–31
  date: Date;          // millisecond-precision equivalent
}
```

---

## Spec notes

* TIDs are 13 characters in the AT Protocol base-32 alphabet: `234567abcdefghijklmnopqrstuvwxyz`.
* The first 11 characters encode a microsecond-precision Unix timestamp.
* The last 2 characters encode a 5-bit clock ID (0–31) which disambiguates TIDs generated on different machines or processes within the same microsecond.
* The clock ID is randomised once at module load time (per JS context). When multiple TIDs would collide at the same microsecond, the implementation adjusts (nudges) the clock ID so collisions are avoided while preserving lexicographic ordering and monotonicity within that runtime.
* Full specification: [https://atproto.com/specs/tid](https://atproto.com/specs/tid)

---

## Behavioural notes

* Monotonicity is maintained per JS context. If records arrive out of chronological order in the same runtime, the package bumps the timestamp forward to ensure every generated TID is strictly increasing.
* Clock ID collisions across processes or machines are extremely unlikely due to the randomised 5-bit clock ID; the clock-nudging only applies inside a JS context to disambiguate simultaneous generations.
* The package does not attempt cross-process coordination — if you need globally unique sequencing beyond the TID spec, consider a server-side sequencer or combining TIDs with per-host identifiers.

---

## Testing & development

Development happens in the [`@ewanc26/pkgs`](https://github.com/ewanc26/pkgs) monorepo. See the monorepo README for setup instructions.

* `resetTidClock()` is exported for tests to make deterministic TID generation possible.
* The library has zero runtime deps and uses the Web Crypto API for secure randomness. When running in older environments, provide a compatible Web Crypto polyfill if necessary.

---

## Licence

AGPL-3.0-only — see the [pkgs monorepo licence](https://github.com/ewanc26/pkgs/blob/main/LICENSE).
