# Last.fm to ATProto Importer

Import your Last.fm listening history to the AT Protocol network using the `fm.teal.alpha.feed.play` lexicon.

(Also [on Tangled!](https://tangled.org/@did:plc:ofrbh253gwicbkc5nktqepol/atproto-lastfm-importer))

## Features

- ✅ **Structured Logging**: Color-coded output with debug/verbose modes
- ✅ **Batch Operations**: Uses `com.atproto.repo.applyWrites` for efficient batch publishing (up to 200 records per call)
- ✅ **Spotify Support**: Import from Spotify Extended Streaming History (JSON format)
- ✅ **Combined Import**: Merge Last.fm and Spotify exports, automatically deduplicating overlapping plays
- ✅ **TID-based Record Keys**: Records use timestamp-based identifiers for chronological ordering
- ✅ **Re-Sync Mode**: Check existing Teal records and only import new scrobbles (no duplicates!)
- ✅ **Rate Limiting**: Automatically limits imports to 1K records per day to prevent rate limiting your entire PDS
- ✅ **Multi-Day Imports**: Large imports (>1K records) automatically span multiple days with 24-hour pauses
- ✅ **Resume Support**: Safe to stop (Ctrl+C) and restart - continues from where it left off
- ✅ **Graceful Cancellation**: Press Ctrl+C to stop after the current batch completes
- ✅ **Identity Resolution**: Resolves ATProto handles/DIDs using Slingshot
- ✅ **PDS Auto-Discovery**: Automatically connects to your personal PDS
- ✅ **Dry Run Mode**: Preview records without publishing
- ✅ **Batch Processing**: Configurable batching with rate limit safety
- ✅ **Progress Tracking**: Real-time progress with time estimates
- ✅ **Error Handling**: Continues on errors with detailed reporting
- ✅ **MusicBrainz Support**: Preserves MusicBrainz IDs when available (Last.fm only)
- ✅ **Chronological Ordering**: Processes oldest first (or newest with `-r` flag)

## Important: Rate Limits

⚠️ **CRITICAL**: Bluesky's AppView has rate limits on PDS instances. Exceeding 10K records per day can rate limit your **ENTIRE PDS**, affecting all users on your instance!

This importer automatically:
- Limits imports to **1,000 records per day** (90% of safe limit)
- Calculates optimal batch sizes and delays
- Pauses 24 hours between days for large imports
- Shows clear progress and time estimates

See: [Bluesky Rate Limits Documentation](https://docs.bsky.app/blog/rate-limits-pds-v3)

## Setup

```bash
npm install
npm run build
```

## Usage

### Combined Import Mode

Merge your Last.fm and Spotify listening history into a single, deduplicated import:

```bash
# Preview the merged import
npm start -- -i lastfm.csv --spotify-input spotify-export/ -m combined --dry-run

# Perform the combined import
npm start -- -i lastfm.csv --spotify-input spotify-export/ -m combined -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y
```

Combined mode will:
1. Parse both Last.fm CSV and Spotify JSON exports
2. Normalize track names and artist names for comparison
3. Identify duplicate plays (same track within 5 minutes)
4. Choose the best version of each play:
   - Prefers Last.fm records with MusicBrainz IDs
   - Otherwise prefers Spotify for better metadata quality
5. Merge into a single chronological timeline
6. Show detailed statistics about the merge

This is perfect for:
- Getting complete listening history from both services
- Filling gaps where one service was used more than the other
- Ensuring the best metadata quality for each play
- Avoiding duplicate entries when both services tracked the same play

**Example Output:**
```
📊 Merge Statistics
═══════════════════════════════════════════
Last.fm records:     15,234
Spotify records:     8,567
Total before merge:  23,801

Duplicates removed:  3,421
Last.fm unique:      11,813
Spotify unique:      5,146

Final merged total:  16,959

Date range:
  First: 2015-03-15 10:23:45
  Last:  2025-01-07 14:32:11
═══════════════════════════════════════════
```

### Re-Sync Mode

If you've already imported scrobbles before and want to sync your Last.fm export with Teal without creating duplicates:

```bash
# Preview what will be synced
npm start -- -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -m sync --dry-run

# Perform the sync
npm start -- -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -m sync -y
```

Sync mode will:
1. Fetch all existing play records from your Teal feed
2. Compare them against your Last.fm export
3. Identify gaps (scrobbles in Last.fm that aren't in Teal)
4. Only import the missing records
5. Show detailed statistics about duplicates and new records

This is perfect for:
- Re-running imports with updated Last.fm exports
- Recovering from interrupted imports
- Adding recent scrobbles without duplicating old ones

**Note:** Sync mode requires authentication even in dry-run mode to fetch existing records.

### Remove Duplicates Mode

If you accidentally imported duplicate records, you can clean them up:

```bash
# Preview duplicates (dry run)
npm start -- -m deduplicate -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx --dry-run

# Remove duplicates (keeps first occurrence)
npm start -- -m deduplicate -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx
```

This will:
1. Fetch all existing records from Teal
2. Identify duplicate plays (same track, artist, and timestamp)
3. Keep the first occurrence of each duplicate
4. Delete the rest

### Interactive Mode

The simplest way to use the importer - just run it and follow the prompts:

```bash
npm start
```

### Command Line Mode

For automation or scripting, provide all parameters via flags:

```bash
# Full automation (Last.fm)
npm start -- -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Import from Spotify (single file)
npm start -- -i Streaming_History_Audio_2021-2023_0.json -m spotify -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Import from Spotify (directory with multiple files - recommended)
npm start -- -i '/path/to/Spotify Extended Streaming History' -m spotify -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Combined import (merge Last.fm and Spotify)
npm start -- -i lastfm.csv --spotify-input '/path/to/Spotify Extended Streaming History' -m combined -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Preview without publishing
npm start -- -i lastfm.csv --dry-run

# Preview with verbose debug output
npm start -- -i lastfm.csv --dry-run -v

# Quiet mode (only warnings and errors)
npm start -- -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -q -y

# Custom batch settings (advanced users)
npm start -- -i lastfm.csv -h alice.bsky.social -b 20 -d 3000

# Process newest tracks first
npm start -- -i lastfm.csv -h alice.bsky.social -r -y
```

## Command Line Options

### Authentication
| Option | Short | Description |
|--------|-------|-------------|
| `--handle <handle>` | `-h` | ATProto handle or DID (e.g., alice.bsky.social) |
| `--password <pass>` | `-p` | ATProto app password |

### Input
| Option | Short | Description |
|--------|-------|-------------|
| `--input <path>` | `-i` | Path to Last.fm CSV or Spotify JSON file/directory |
| `--spotify-input <path>` | | Path to Spotify export (for combined mode) |

### Mode
| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--mode <mode>` | `-m` | Import mode | `lastfm` |

**Available modes:**
- `lastfm` - Import Last.fm export only
- `spotify` - Import Spotify export only
- `combined` - Merge Last.fm + Spotify exports
- `sync` - Skip existing records (sync mode)
- `deduplicate` - Remove duplicate records

### Batch Configuration
| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--batch-size <num>` | `-b` | Records per batch | Auto-calculated |
| `--batch-delay <ms>` | `-d` | Delay between batches in ms | 500 (min: 500) |

### Import Options
| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--reverse` | `-r` | Process newest first | false (oldest first) |
| `--yes` | `-y` | Skip confirmation prompts | false |
| `--dry-run` | | Preview without importing | false |

### Output
| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--verbose` | `-v` | Enable debug logging | false |
| `--quiet` | `-q` | Suppress non-essential output | false |
| `--help` | | Show help message | - |

### Legacy Flags (Backwards Compatible)

For backwards compatibility, the following old flags still work:
- `--file` → Use `--input` instead
- `--identifier` → Use `--handle` instead
- `--spotify-file` → Use `--spotify-input` instead
- `--reverse-chronological` → Use `--reverse` instead
- `--spotify` → Use `--mode spotify` instead
- `--combined` → Use `--mode combined` instead
- `--sync` → Use `--mode sync` instead
- `--remove-duplicates` → Use `--mode deduplicate` instead

### Batch Settings

The importer automatically calculates optimal batch settings based on your total record count and rate limits. You generally **don't need** to specify batch settings unless you have specific requirements.

**Automatic behavior:**
- For imports < 1K records: Uses default settings (200 records/batch, 500ms delay)
- For imports > 1K records: Automatically calculates settings to spread across multiple days

**Manual override** (advanced):
- `--batch-size`: Number of records processed per batch (1-200, PDS maximum)
- `--batch-delay`: Milliseconds to wait between batches (min: 500)

⚠️ Lower delays increase speed but risk hitting rate limits. The automatic calculation is recommended.

## Logging and Output

The importer includes a structured logging system with color-coded output:

- **Green (✓)**: Success messages
- **Cyan (→)**: Progress updates
- **Yellow (⚠️)**: Warnings
- **Red (✗)**: Errors
- **Bold Red (🛑)**: Fatal errors

### Verbosity Levels

**Default Mode**: Shows standard operational messages
```bash
npm start -- -i lastfm.csv -h alice.bsky.social -p pass
```

**Verbose Mode** (`-v`): Shows detailed debug information including batch timing, API calls, etc.
```bash
npm start -- -i lastfm.csv -h alice.bsky.social -p pass -v
```

**Quiet Mode** (`-q`): Only shows warnings and errors
```bash
npm start -- -i lastfm.csv -h alice.bsky.social -p pass -q
```

## Getting Your Data

### Last.fm Export

1. Go to <https://lastfm.ghan.nl/export/>
2. Request your data export in CSV format
3. Download the CSV file when ready
4. Use the CSV file path with this script

### Spotify Export

1. Go to your [Spotify Privacy Settings](https://www.spotify.com/account/privacy/)
2. Scroll down to "Download your data" and request your data
3. Select "Extended streaming history" (this can take up to 30 days)
4. When ready, download and extract the ZIP file
5. Use either:
   - A single JSON file: `Streaming_History_Audio_2021-2023_0.json`
   - The entire extracted directory (recommended)

**Note**: Spotify exports include multiple JSON files. The importer automatically:
- Reads all `Streaming_History_Audio_*.json` files in a directory
- Filters out podcasts, audiobooks, and other non-music content
- Combines all music tracks into a single import

## What Gets Imported

Each scrobble (from Last.fm or Spotify) becomes an `fm.teal.alpha.feed.play` record with:

### Required Fields
- **trackName**: The name of the track
- **artists**: Array of artist objects (requires `artistName`, optional `artistMbId` for Last.fm)
- **playedTime**: ISO 8601 timestamp of when you listened
- **submissionClientAgent**: Identifies this importer (`lastfm-importer/v0.6.0`)
- **musicServiceBaseDomain**: Set to `last.fm` or `spotify.com` depending on source

### Optional Fields (when available)
- **releaseName**: Album/release name
- **releaseMbId**: MusicBrainz release ID (Last.fm only)
- **recordingMbId**: MusicBrainz recording/track ID (Last.fm only)
- **originUrl**: Link to the track on Last.fm or Spotify

### Example Records

**Last.fm Record:**
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
  "submissionClientAgent": "lastfm-importer/v0.6.0",
  "musicServiceBaseDomain": "last.fm"
}
```

**Spotify Record:**
```json
{
  "$type": "fm.teal.alpha.feed.play",
  "trackName": "Don't Give Up",
  "artists": [
    {
      "artistName": "Chicane"
    }
  ],
  "releaseName": "Twenty",
  "playedTime": "2021-09-09T10:34:08Z",
  "originUrl": "https://open.spotify.com/track/3gZqDJkMZipOYCRjlHWgOV",
  "submissionClientAgent": "lastfm-importer/v0.6.0",
  "musicServiceBaseDomain": "spotify.com"
}
```

## Processing Order

By default, records are processed **oldest first** (chronological order). This means your earliest scrobbles will appear first in your ATProto feed.

Use the `--reverse` or `-r` flag to process **newest first** instead.

## Multi-Day Imports

For imports exceeding 1,000 records (after applying the 90% safety margin), the importer automatically:

1. **Calculates a schedule**: Splits your import across multiple days
2. **Shows the plan**: Displays which records will be imported each day
3. **Processes Day 1**: Imports the first batch of records
4. **Pauses 24 hours**: Waits a full day before continuing
5. **Repeats**: Continues until all records are imported

**Important notes:**
- You can safely stop (Ctrl+C) and restart the importer
- Progress is preserved - it continues where it left off
- Each day's progress is clearly displayed
- Time estimates account for multi-day duration

Example output for a 5,000 record import:
```
📊 Rate Limiting Information:
   Total records: 5,000
   Daily limit: 900 records/day
   Estimated duration: 6 days
   Batch size: 50 records
   Batch delay: 1920.0s
```

## Dry Run Mode

Preview what will be imported without actually publishing:

```bash
npm start -- -i lastfm.csv --dry-run
```

Dry run shows:
- Total record count
- Rate limiting schedule (if applicable)
- Multi-day import plan (if needed)
- Preview of first 5 records with full details
- MusicBrainz IDs when available

## Error Handling

The importer is designed to be resilient:

- **Network errors**: Records that fail are logged but don't stop the import
- **Invalid data**: Skipped with error messages
- **Authentication issues**: Clear error messages with suggested fixes
- **Rate limit hits**: Automatic adjustment and retry logic
- **Ctrl+C handling**: Gracefully stops after current batch

Failed records are logged but don't prevent the rest of your import from completing.

## Project Structure

```
atproto-lastfm-importer/
├── src/
│   ├── lib/
│   │   ├── auth.ts         # Authentication & identity resolution
│   │   ├── cli.ts          # Command line interface & argument parsing
│   │   ├── csv.ts          # CSV parsing & record conversion
│   │   ├── publisher.ts    # Batch publishing with rate limiting
│   │   ├── spotify.ts      # Spotify JSON parsing
│   │   ├── merge.ts        # Combined import deduplication
│   │   └── sync.ts         # Re-sync mode & duplicate detection
│   ├── utils/
│   │   ├── logger.ts       # Structured logging system (NEW!)
│   │   ├── helpers.ts      # Utility functions (timing, formatting)
│   │   ├── input.ts        # User input handling (prompts, passwords)
│   │   ├── rate-limiter.ts # Rate limiting calculations
│   │   ├── killswitch.ts   # Graceful shutdown handling
│   │   ├── tid.ts          # TID generation from timestamps
│   │   └── ui.ts           # UI elements (spinners, progress bars)
│   ├── config.ts           # Configuration constants
│   └── types.ts            # TypeScript type definitions
├── lexicons/               # fm.teal.alpha lexicon definitions
│   └── fm.teal.alpha/
│       └── feed/
│           └── play.json   # Play record schema
├── package.json
├── tsconfig.json
├── CLI_IMPROVEMENTS.md     # Detailed CLI documentation
└── README.md
```

## Development

```bash
# Type checking
npm run type-check

# Build
npm run build

# Development mode (rebuild + run)
npm run dev

# Clean build artifacts
npm run clean
```

## Technical Details

### Authentication
- Uses Slingshot resolver to discover your PDS from your handle/DID
- Requires an ATProto app password (not your main password)
- Automatically configures the agent for your personal PDS

### Rate Limiting Algorithm
1. Calculates safe daily limit (90% of 1K = 900 records/day)
2. Determines how many days needed for your import
3. Calculates optimal batch size and delay to spread records evenly
4. Enforces minimum 500ms delay between batches
5. Shows clear schedule before starting

### Record Processing
1. Parses input file(s):
   - **Last.fm**: CSV using `csv-parse` library
   - **Spotify**: JSON files (single or multiple in directory)
2. Filters data:
   - **Spotify**: Automatically removes podcasts, audiobooks, and non-music content
3. Converts to `fm.teal.alpha.feed.play` schema
4. Sorts records chronologically (or reverse if `-r` flag)
5. Generates TID-based record keys from `playedTime` for chronological ordering
6. Validates required fields
7. Publishes in batches using `com.atproto.repo.applyWrites` (up to 200 records per call, the PDS maximum)

**Note:** The batch publishing uses `applyWrites` instead of individual `createRecord` calls for dramatically improved performance (up to 20x faster).

### Data Mapping

**Last.fm:**
- **Track info**: Direct mapping from CSV columns
- **Timestamps**: Converts Unix timestamps to ISO 8601
- **MusicBrainz IDs**: Preserved when present in CSV
- **URLs**: Generated from artist/track names
- **Artists**: Wrapped in array format with optional MBID

**Spotify:**
- **Track info**: Extracted from JSON fields
- **Timestamps**: Already in ISO 8601 format (`ts` field)
- **URLs**: Generated from `spotify_track_uri` field
- **Artists**: Extracted from `master_metadata_album_artist_name`
- **Albums**: Extracted from `master_metadata_album_album_name`
- **Filtering**: Non-music content automatically excluded

## Lexicon Reference

This importer follows the official `fm.teal.alpha` lexicon defined in `/lexicons/fm.teal.alpha/feed/play.json`.

The lexicon defines:
- Required and optional field types
- String length constraints
- Array formats
- Timestamp formatting
- URL validation

## Troubleshooting

### "Handle not found"
- Verify your ATProto handle is correct (e.g., `alice.bsky.social`)
- Make sure you're using a valid DID or handle

### "Invalid credentials"
- Use an **app password**, not your main account password
- Generate app passwords in your account settings

### "Rate limit exceeded"
- The importer should prevent this automatically
- If you see this, wait 24 hours before retrying
- Consider reducing batch size or increasing delay

### "Connection refused"
- Check your internet connection
- Verify your PDS is accessible
- Some PDSs may have firewall rules

### Import seems stuck
- Check progress messages - large imports take time
- Multi-day imports pause for 24 hours between days
- You can safely stop (Ctrl+C) and resume later
- Use `--verbose` flag to see detailed progress

### Too much output
- Use `--quiet` flag to suppress non-essential messages
- Only warnings and errors will be shown

### Need more details
- Use `--verbose` flag to see debug-level information
- Shows batch timing, API calls, and detailed progress

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

See `CLI_IMPROVEMENTS.md` for developer documentation on the logging system and CLI structure.

## License

AGPL-3.0-only - See LICENCE file for details

## Credits

- Uses [@atproto/api](https://www.npmjs.com/package/@atproto/api) for ATProto interactions
- CSV parsing via [csv-parse](https://www.npmjs.com/package/csv-parse)
- Identity resolution via [Slingshot](https://slingshot.danner.cloud)
- Follows the `fm.teal.alpha` lexicon standard
- Colored output via [chalk](https://www.npmjs.com/package/chalk)

---

**Note**: This tool is for personal use. Respect the terms of service and rate limits when exporting your data.