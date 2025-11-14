# Last.fm to ATProto Importer

Import your Last.fm listening history to the AT Protocol network using the `fm.teal.alpha.feed.play` lexicon.

## Setup

```bash
npm install
```

## Usage

### Interactive Mode

```bash
node importer.js
```

### With Command Line Arguments

**Full automation:**

```bash
node importer.js -f lastfm.csv -i alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y
```

**Dry run (preview without publishing):**

```bash
node importer.js -f lastfm.csv --dry-run
```

**Custom batch settings:**

```bash
node importer.js -f lastfm.csv -i alice.bsky.social -b 20 -d 3000
```

## Options

- `-h, --help` - Show help message
- `-f, --file <path>` - Path to Last.fm CSV export file
- `-i, --identifier <id>` - ATProto handle or DID
- `-p, --password <pass>` - ATProto app password
- `-b, --batch-size <num>` - Records per batch (default: 10)
- `-d, --batch-delay <ms>` - Delay between batches in ms (default: 2000)
- `-y, --yes` - Skip confirmation prompt
- `-n, --dry-run` - Preview records without publishing

## Getting Your Last.fm Data

1. Go to <https://lastfm.ghan.nl/export/>
2. Request your data export in CSV
3. Download the CSV file when ready
4. Use the CSV file path with this script

## Features

- ✅ Resolves ATProto handles/DIDs using Slingshot
- ✅ Connects to your personal PDS
- ✅ Converts Last.fm scrobbles to `fm.teal.alpha.feed.play` records
- ✅ Follows the official lexicon schema
- ✅ Batch publishing with configurable rate limiting
- ✅ Dry run mode for previewing
- ✅ Progress tracking and error reporting
- ✅ Preserves MusicBrainz IDs when available

## Record Format

Each scrobble is converted according to the `fm.teal.alpha.feed.play` lexicon:

```json
{
  "$type": "fm.teal.alpha.feed.play",
  "trackName": "Paint My Masterpiece",
  "artists": [
    {
      "artistName": "Cjbeards",
      "artistMbId": "c8d4f4bf-1b82-4d4d-9d73-05909faaff89"
    }
  ],
  "releaseName": "Masquerade",
  "releaseMbId": "fdb2397b-78d5-4019-8fad-656d286e4d33",
  "recordingMbId": "3a390ad3-fe56-45f2-a073-bebc45d6bde1",
  "playedTime": "2025-11-13T23:49:36Z",
  "originUrl": "https://www.last.fm/music/Cjbeards/_/Paint+My+Masterpiece",
  "submissionClientAgent": "lastfm-importer/v1.0.0",
  "musicServiceBaseDomain": "last.fm"
}
```

### Required Fields

- `trackName` - The name of the track
- `artists` - Array of artist objects with `artistName` (required) and optional `artistMbId`

### Optional Fields

- `releaseName` - Album name
- `releaseMbId` - MusicBrainz release ID
- `recordingMbId` - MusicBrainz recording ID
- `playedTime` - ISO 8601 datetime
- `originUrl` - Link to the track
- `submissionClientAgent` - Client identifier
- `musicServiceBaseDomain` - Service domain (e.g., "last.fm")

## Lexicon Reference

This importer follows the lexicon defined in `/lexicons/fm.teal.alpha/feed/play.json`.
