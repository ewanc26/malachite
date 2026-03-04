# @ewanc26/tid

Zero-dependency [AT Protocol](https://atproto.com/) TID (Timestamp Identifier) generation for Node.js and browsers.

TIDs are the record keys used throughout ATProto / Bluesky — 13-character, lexicographically sortable, monotonic identifiers derived from a microsecond timestamp and a random clock ID.

## Why this package?

Other TID implementations require native bindings (`node-gyp`, Python) or pull in large dependency trees. This package is **pure JavaScript** with **no runtime dependencies**, and runs anywhere the [Web Crypto API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Crypto_API) is available — Node.js 20+, Deno, Bun, and all modern browsers.

## Install

```sh
npm install @ewanc26/tid
# or
pnpm add @ewanc26/tid
```

## Usage

### Generate a TID for a historical timestamp

Pass an ISO 8601 string or a `Date` object. The clock is monotonic — if records arrive out of order, the timestamp is bumped forward so every call produces a strictly increasing TID within the same JS context.

```ts
import { generateTID } from '@malachite/tid';

// From an ISO string (e.g. a Last.fm scrobble timestamp)
const tid = generateTID('2023-11-01T12:00:00Z');

// From a Date object
const tid2 = generateTID(new Date('2024-03-15T09:30:00Z'));
```

### Generate a TID for right now

```ts
import { generateNextTID } from '@malachite/tid';

const tid = generateNextTID();
```

### Validate a TID

```ts
import { validateTid } from '@malachite/tid';

validateTid('3jzfcijpj2z2a');  // true
validateTid('not-a-tid');       // false
```

### Decode a TID

```ts
import { decodeTid } from '@malachite/tid';

const { timestampUs, clockId, date } = decodeTid('3jzfcijpj2z2a');
// timestampUs — microseconds since Unix epoch
// clockId     — random 0–31 clock identifier
// date        — equivalent JavaScript Date (millisecond precision)
```

### Compare / sort TIDs

Because the AT Protocol base-32 alphabet is ordered by timestamp, lexicographic string comparison is correct. `compareTids` returns `-1 | 0 | 1` for use as a `sort` comparator.

```ts
import { compareTids } from '@malachite/tid';

const tids = ['3jzfcijpj2z2a', '3jzfabc000022', '3jzfzzzzzzz2a'];
tids.sort(compareTids);
// → ['3jzfabc000022', '3jzfcijpj2z2a', '3jzfzzzzzzz2a']  (chronological)
```

## API

| Export | Signature | Description |
|--------|-----------|-------------|
| `generateTID` | `(source: string \| Date) => string` | Generate a TID for a historical timestamp |
| `generateNextTID` | `() => string` | Generate a TID for the current wall-clock time |
| `validateTid` | `(tid: string) => boolean` | Returns `true` if the string is a well-formed TID |
| `decodeTid` | `(tid: string) => DecodedTid` | Decode a TID into timestamp, clockId, and Date |
| `compareTids` | `(a: string, b: string) => -1 \| 0 \| 1` | Lexicographic comparator for sorting |
| `resetTidClock` | `() => void` | Reset the monotonic clock (**tests only**) |

### `DecodedTid`

```ts
interface DecodedTid {
  timestampUs: number;  // microseconds since Unix epoch
  clockId: number;      // 0–31
  date: Date;           // millisecond-precision equivalent
}
```

## Spec notes

- TIDs are 13 characters in the AT Protocol base-32 alphabet (`234567abcdefghijklmnopqrstuvwxyz`).
- The first 11 characters encode a microsecond-precision Unix timestamp.
- The last 2 characters encode a random clock ID (0–31) that disambiguates TIDs generated on different machines or in different processes within the same microsecond.
- The clock ID is randomised once at module load time (per JS context).
- The full specification is at <https://atproto.com/specs/tid>.

## License

AGPL-3.0-only — same as [Malachite](https://github.com/ewanc26/malachite).
