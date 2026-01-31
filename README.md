# Malachite

Import your Last.fm and Spotify listening history to the AT Protocol network using the `fm.teal.alpha.feed.play` lexicon.

**Repository:** [malachite](https://github.com/ewanc26/malachite)  
[Also available on Tangled](https://tangled.org/did:plc:ofrbh253gwicbkc5nktqepol/atproto-lastfm-importer)

## ⚠️ Important: Rate Limits

**CRITICAL**: Bluesky's AppView has rate limits on PDS instances. Exceeding 10K records per day can rate limit your **ENTIRE PDS**, affecting all users on your instance.

This importer automatically protects your PDS by:
- Limiting imports to **1,000 records per day** (with 75% safety margin)
- Calculating optimal batch sizes and delays
- Pausing 24 hours between days for large imports
- Providing clear progress tracking and time estimates

For more details, see the [Bluesky Rate Limits Documentation](https://docs.bsky.app/blog/rate-limits-pds-v3).

## What’s with the name?

It used to be called `atproto-lastfm-importer` — generic as fuck. That name told you what it did and nothing about why it mattered, and it sounded like a disposable weekend script. So I renamed it.

At the moment, the repository is still called `atproto-lastfm-importer` on Tangled, but the GitHub link has been updated to `malachite`. I do not know if this can be resolved.

**Malachite** is a greenish-blue copper mineral associated with preservation and transformation. That’s exactly what this tool does: it preserves your scrobbles and transforms them into proper `fm.teal.alpha.feed.play` records on the AT Protocol. The colour match isn’t an accident — malachite sits squarely in the teal/green range, a deliberate nod to the `teal` lexicon it publishes to.

## Quick Start

**Note:** You must build the project first, then run with arguments.

### Interactive Mode (Recommended for First-Time Users)

Just run without any arguments and Malachite will guide you through the process:

```bash
# Install dependencies and build
pnpm install
pnpm build

# Run in interactive mode
pnpm start
```

The interactive mode will:
- Present a menu of available actions
- Prompt for all required information (handle, password, files)
- Ask for optional settings (dry run, verbose logging, etc.)
- Provide helpful descriptions for each option

### Command Line Mode

For automation or if you prefer command-line arguments:

```bash
# Show help
pnpm start --help

# Run with command line arguments
pnpm start -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Alternative: run directly with node
node dist/index.js -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y
```

## Features

### Import Capabilities
- ✅ **Last.fm Import**: Full support for Last.fm CSV exports with MusicBrainz IDs
- ✅ **Spotify Import**: Import Extended Streaming History JSON files
- ✅ **Combined Import**: Merge Last.fm and Spotify exports with intelligent deduplication
- ✅ **Re-Sync Mode**: Import only new scrobbles without creating duplicates
- ✅ **Duplicate Removal**: Clean up accidentally imported duplicate records

### Performance & Safety
- ✅ **Automatic Duplicate Prevention**: Automatically checks Teal and skips records that already exist (no duplicates!)
- ✅ **Input Deduplication**: Removes duplicate entries within the source file before submission
- ✅ **Batch Operations**: Uses `com.atproto.repo.applyWrites` for efficient batch publishing (up to 200 records per call)
- ✅ **Rate Limiting**: Automatic daily limits prevent PDS rate limiting
- ✅ **Multi-Day Imports**: Large imports automatically span multiple days with 24-hour pauses
- ✅ **Resume Support**: Safe to stop (Ctrl+C) and restart - continues from where it left off
- ✅ **Graceful Cancellation**: Press Ctrl+C to stop after the current batch completes

### User Experience
- ✅ **Structured Logging**: Color-coded output with debug/verbose modes
- ✅ **Progress Tracking**: Real-time progress with time estimates
- ✅ **Dry Run Mode**: Preview records without publishing
- ✅ **Interactive Mode**: Simple prompts guide you through the process
- ✅ **Command Line Mode**: Full automation support for scripting

### Technical Features
- ✅ **TID-based Record Keys**: Timestamp-based identifiers for chronological ordering
- ✅ **Identity Resolution**: Resolves ATProto handles/DIDs using Slingshot
- ✅ **PDS Auto-Discovery**: Automatically connects to your personal PDS
- ✅ **MusicBrainz Support**: Preserves MusicBrainz IDs when available (Last.fm)
- ✅ **Chronological Ordering**: Processes oldest first (or newest with `-r` flag)
- ✅ **Error Handling**: Continues on errors with detailed reporting

## Usage Examples

### Combined Import (Last.fm + Spotify)

Merge your Last.fm and Spotify listening history into a single, deduplicated import:

```bash
# Preview the merged import
pnpm start -i lastfm.csv --spotify-input spotify-export/ -m combined --dry-run

# Perform the combined import
pnpm start -i lastfm.csv --spotify-input spotify-export/ -m combined -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y
```

**What combined mode does:**
1. Parses both Last.fm CSV and Spotify JSON exports
2. Normalizes track names and artist names for comparison
3. Identifies duplicate plays (same track within 5 minutes)
4. Chooses the best version of each play (prefers Last.fm with MusicBrainz IDs)
5. Merges into a single chronological timeline
6. Shows detailed statistics about the merge

**Example output:**
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

Sync your Last.fm export with Teal without creating duplicates:

```bash
# Preview what will be synced
pnpm start -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -m sync --dry-run

# Perform the sync
pnpm start -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -m sync -y
```

**Perfect for:**
- Re-running imports with updated Last.fm exports
- Recovering from interrupted imports
- Adding recent scrobbles without duplicating old ones

**Note:** Sync mode requires authentication even in dry-run mode to fetch existing records.

### Remove Duplicates

Clean up accidentally imported duplicate records:

```bash
# Preview duplicates (dry run)
pnpm start -m deduplicate -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx --dry-run

# Remove duplicates (keeps first occurrence)
pnpm start -m deduplicate -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx
```

### Import from Spotify

```bash
# Import single Spotify JSON file
pnpm start -i Streaming_History_Audio_2021-2023_0.json -m spotify -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Import directory with multiple Spotify files (recommended)
pnpm start -i '/path/to/Spotify Extended Streaming History' -m spotify -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y
```

### Import from Last.fm

```bash
# Standard Last.fm import
pnpm start -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Preview without publishing
pnpm start -i lastfm.csv --dry-run

# Process newest tracks first
pnpm start -i lastfm.csv -h alice.bsky.social -r -y

# Verbose debug output
pnpm start -i lastfm.csv --dry-run -v

# Quiet mode (only warnings and errors)
pnpm start -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -q -y
```

### Advanced Options

```bash
# Development mode (verbose + file logging + smaller batches for debugging)
pnpm start -i lastfm.csv --dev --dry-run

# Custom batch settings (advanced users only)
pnpm start -i lastfm.csv -h alice.bsky.social -b 20 -d 3000

# Full automation with all flags
pnpm start -i lastfm.csv -h alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y -q
```

## Command Line Options

**Note:** When importing data (not in deduplicate mode), you must provide `--input`, `--handle`, and `--password`. The `--yes` flag skips confirmation prompts for automation.

### Required Options

| Option | Short | Description | Example |
|--------|-------|-------------|---------|
| `--input <path>` | `-i` | Path to Last.fm CSV or Spotify JSON file/directory | `-i lastfm.csv` |
| `--handle <handle>` | `-h` | ATProto handle or DID | `-h alice.bsky.social` |
| `--password <pass>` | `-p` | ATProto app password | `-p xxxx-xxxx-xxxx-xxxx` |

### Import Mode

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--mode <mode>` | `-m` | Import mode | `lastfm` |

**Available modes:**
- `lastfm` - Import Last.fm export only
- `spotify` - Import Spotify export only  
- `combined` - Merge Last.fm + Spotify exports
- `sync` - Skip existing records (sync mode)
- `deduplicate` - Remove duplicate records

### Additional Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|  
| `--spotify-input <path>` | | Path to Spotify export (for combined mode) | - |
| `--reverse` | `-r` | Process newest first | `false` |
| `--yes` | `-y` | Skip confirmation prompts | `false` |
| `--dry-run` | | Preview without importing | `false` |
| `--verbose` | `-v` | Enable debug logging | `false` |
| `--quiet` | `-q` | Suppress non-essential output | `false` |
| `--dev` | | Development mode (verbose + file logging + smaller batches) | `false` |
| `--batch-size <num>` | `-b` | Records per batch (1-200) | Auto-calculated |
| `--batch-delay <ms>` | `-d` | Delay between batches in ms | `500` (min) |
| `--help` | | Show help message | - |

### Legacy Flags (Backwards Compatible)

These old flags still work but are deprecated:
- `--file` → Use `--input`
- `--identifier` → Use `--handle`
- `--spotify-file` → Use `--spotify-input`
- `--reverse-chronological` → Use `--reverse`
- `--spotify` → Use `--mode spotify`
- `--combined` → Use `--mode combined`
- `--sync` → Use `--mode sync`
- `--remove-duplicates` → Use `--mode deduplicate`

## Getting Your Data

### Last.fm Export

1. Visit [Last.fm Export Tool](https://lastfm.ghan.nl/export/)
2. Request your data export in CSV format
3. Download the CSV file when ready
4. Use the CSV file path with this importer

### Spotify Export

1. Go to [Spotify Privacy Settings](https://www.spotify.com/account/privacy/)
2. Scroll to "Download your data" and request your data
3. Select "Extended streaming history" (can take up to 30 days)
4. When ready, download and extract the ZIP file
5. Use either:
   - A single JSON file: `Streaming_History_Audio_2021-2023_0.json`
   - The entire extracted directory (recommended)

**Note:** The importer automatically:
- Reads all `Streaming_History_Audio_*.json` files in a directory
- Filters out podcasts, audiobooks, and non-music content
- Combines all music tracks into a single import

## Data Format

Each scrobble becomes an `fm.teal.alpha.feed.play` record with:

### Required Fields
- **trackName**: The name of the track
- **artists**: Array of artist objects (requires `artistName`, optional `artistMbId` for Last.fm)
- **playedTime**: ISO 8601 timestamp of when you listened
- **submissionClientAgent**: Identifies this importer (`malachite/v0.7.3`)
- **musicServiceBaseDomain**: Set to `last.fm` or `spotify.com`

### Optional Fields
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
  "submissionClientAgent": "malachite/v0.7.3",
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
  "submissionClientAgent": "malachite/v0.7.3",
  "musicServiceBaseDomain": "spotify.com"
}
```

## How It Works

### Processing Flow
1. **Parses input file(s)**:
   - Last.fm: CSV using `csv-parse` library
   - Spotify: JSON files (single or multiple in directory)
2. **Filters data**:
   - Spotify: Automatically removes podcasts, audiobooks, and non-music content
3. **Converts to schema**: Maps to `fm.teal.alpha.feed.play` format
4. **Deduplicates input**: Removes duplicate entries from the source data (keeps first occurrence)
5. **Checks Teal**: Fetches existing records and skips any that are already imported (prevents duplicates)
6. **Sorts records**: Chronologically (oldest first) or reverse with `-r` flag
7. **Generates TID-based keys**: From `playedTime` for chronological ordering
8. **Validates fields**: Ensures required fields are present
9. **Publishes in batches**: Uses `com.atproto.repo.applyWrites` (up to 200 records per call)

### Automatic Duplicate Prevention

The importer has **two layers of duplicate prevention** to ensure you never import the same record twice:

#### Step 1: Input File Deduplication

Removes duplicates within your source file(s):

**How duplicates are identified:**
- Same track name (case-insensitive)
- Same artist name (case-insensitive)  
- Same timestamp (exact match)

**What happens:**
- First occurrence is kept
- Subsequent duplicates are removed
- Shows message: "No duplicates found in input data" or "Removed X duplicate(s)"

#### Step 2: Teal Comparison (Automatic & Adaptive)

**Automatically checks your existing Teal records** and skips any that are already imported:

**What happens:**
- Fetches all existing records from your Teal feed with **adaptive batch sizing**
- Starts with small batches (25 records) and automatically adjusts based on network performance
- Increases batch size (up to 100) when network is fast
- Decreases batch size (down to 10) when network is slow
- Shows real-time progress with fetch rate (records/second) and current batch size
- Compares against your input file
- Only imports records that don't already exist
- Shows: "Found X record(s) already in Teal (skipping)"

**Example output:**
```
✓ Loaded 10,234 records
ℹ No duplicates found in input data

=== Checking Existing Records ===
ℹ Fetching records from Teal to avoid duplicates...
→ Fetched 1,000 records (125 rec/s, batch: 37, 8.0s)...
📈 Network good: batch size 37 → 55
→ Fetched 2,000 records (140 rec/s, batch: 82, 14.3s)...
📈 Network good: batch size 82 → 100
→ Fetched 3,000 records (155 rec/s, batch: 100, 19.4s)...
...
✓ Found 9,500 existing records in 61.3s (avg 155 rec/s)

=== Identifying New Records ===
ℹ Total: 10,234 records
ℹ Existing: 9,100 already in Teal
ℹ New: 1,134 to import
```

**This means:**
- ✅ Safe to re-run imports with updated exports
- ✅ Won't create duplicates if you run the import twice
- ✅ Only pays for API calls on new records
- ✅ Works automatically - no special mode needed
- ✅ Adapts to your network speed - faster on good connections, stable on slow ones
- ✅ Batch size shown in debug mode (`-v`) for transparency

**Note:** 
- This duplicate prevention happens automatically for all imports (default behavior)
- **Credentials required**: Even `--dry-run` needs `--handle` and `--password` to check Teal
- **Sync mode** (`-m sync`): Now primarily just shows detailed statistics about what's being synced
- **Deduplicate mode** (`-m deduplicate`): Removes duplicates from already-imported Teal records (cleanup tool)

### Rate Limiting Algorithm
1. Calculates safe daily limit (75% of 10K = 7,500 records/day by default)
2. Determines how many days needed for your import
3. Calculates optimal batch size and delay to spread records evenly
4. Enforces minimum delay between batches
5. Shows clear schedule before starting

### Multi-Day Imports

For imports exceeding the daily limit, the importer automatically:
1. **Calculates a schedule**: Splits your import across multiple days
2. **Shows the plan**: Displays which records will be imported each day
3. **Processes Day 1**: Imports the first batch of records
4. **Pauses 24 hours**: Waits a full day before continuing
5. **Repeats**: Continues until all records are imported

**Example output for a 20,000 record import:**
```
📊 Rate Limiting Information:
   Total records: 20,000
   Daily limit: 7,500 records/day
   Estimated duration: 3 days
   Batch size: 200 records
   Batch delay: 11.52s
```

**Important notes:**
- You can safely stop (Ctrl+C) and restart
- Progress is preserved - continues where it left off
- Each day's progress is clearly displayed
- Time estimates account for multi-day duration

## Logging and Output

The importer uses color-coded output for clarity:

- **Green (✓)**: Success messages
- **Cyan (→)**: Progress updates
- **Yellow (⚠️)**: Warnings
- **Red (✗)**: Errors
- **Bold Red (🛑)**: Fatal errors

### Verbosity Levels

**Default Mode**: Standard operational messages
```bash
pnpm start -i lastfm.csv -h alice.bsky.social -p pass
```

**Verbose Mode** (`-v`): Detailed debug information including batch timing and API calls
```bash
pnpm start -i lastfm.csv -h alice.bsky.social -p pass -v
```

**Quiet Mode** (`-q`): Only warnings and errors
```bash
pnpm start -i lastfm.csv -h alice.bsky.social -p pass -q
```

**Development Mode** (`--dev`): Verbose logging + file logging to `~/.malachite/logs/` + smaller batch sizes
```bash
pnpm start -i lastfm.csv --dev --dry-run
```

Development mode is perfect for:
- Debugging import issues with detailed logs
- Testing changes with smaller batches (20 records max)
- Preserving logs for later analysis
- Troubleshooting problems with support

## Error Handling

The importer is designed to be resilient:

- **Network errors**: Failed records are logged but don't stop the import
- **Invalid data**: Skipped with error messages
- **Authentication issues**: Clear error messages with suggested fixes
- **Rate limit hits**: Automatic adjustment and retry logic
- **Ctrl+C handling**: Gracefully stops after current batch

## Troubleshooting

### Authentication Issues

**"Handle not found"**
- Verify your ATProto handle is correct (e.g., `alice.bsky.social`)
- Ensure you're using a valid DID or handle

**"Invalid credentials"**
- Use an **app password**, not your main account password
- Generate app passwords in your account settings

### Performance Issues

**"Rate limit exceeded"**
- The importer should prevent this automatically
- If you see this, wait 24 hours before retrying
- Consider reducing batch size with `-b` flag

**Import seems stuck**
- Check progress messages - large imports take time
- Multi-day imports pause for 24 hours between days
- You can safely stop (Ctrl+C) and resume later
- Use `--verbose` flag to see detailed progress

### Connection Issues

**"Connection refused"**
- Check your internet connection
- Verify your PDS is accessible
- Some PDSs may have firewall rules

### Output Control

**Too much output**
- Use `--quiet` flag to suppress non-essential messages
- Only warnings and errors will be shown

**Need more details**
- Use `--verbose` flag to see debug-level information
- Shows batch timing, API calls, and detailed progress

## Development

```bash
# Type checking
pnpm run type-check

# Build
pnpm run build

# Development mode (rebuild + run)
pnpm run dev

# Run tests
pnpm run test

# Clean build artifacts
pnpm run clean
```

## File Storage

Malachite stores all its data in `~/.malachite/`:

```
~/.malachite/
├── cache/          # Cached Teal records (24-hour TTL)
├── state/          # Import state for resume functionality
├── logs/           # Import logs (when file logging is enabled)
└── credentials.json # Encrypted credentials (optional, machine-specific)
```

This keeps your project directory clean and follows standard Unix conventions.

### Credential Storage

Malachite can optionally save your ATProto credentials for convenient reuse:

**Security Features:**
- ✅ **AES-256-GCM encryption** - Military-grade encryption
- ✅ **Machine-specific** - Credentials are bound to your computer and can't be transferred
- ✅ **Secure key derivation** - Uses PBKDF2 with 100,000 iterations
- ✅ **File permissions** - Credentials file is readable only by you (Unix)

**How It Works:**
1. Interactive mode asks if you want to save credentials after entering them
2. Credentials are encrypted using a key derived from your hostname + username
3. Saved to `~/.malachite/credentials.json`
4. Next time, you'll be prompted to use saved credentials

**Managing Credentials:**
```bash
# Clear saved credentials
pnpm start --clear-credentials

# Or through interactive mode (option 7)
pnpm start
```

**Important Notes:**
- Credentials are machine-specific and won't work if you copy the file to another computer
- This is a convenience feature - you can always enter credentials manually
- If you change your password, you'll need to clear and re-save credentials

## Project Structure

```
malachite/
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
│   │   ├── logger.ts       # Structured logging system
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
└── README.md
```

## Technical Details

### Authentication
- Uses Slingshot resolver to discover your PDS from your handle/DID
- Requires an ATProto app password (not your main password)
- Automatically configures the agent for your personal PDS

### Batch Publishing
- Uses `com.atproto.repo.applyWrites` for efficiency (up to 20x faster than individual calls)
- Batches up to 200 records per API call (PDS maximum)
- Automatically adjusts batch size based on total record count
- Enforces minimum delays between batches for rate limit safety

### Data Mapping

**Last.fm:**
- Direct mapping from CSV columns
- Converts Unix timestamps to ISO 8601
- Preserves MusicBrainz IDs when present
- Generates URLs from artist/track names
- Wraps artists in array format with optional MBID

**Spotify:**
- Extracts data from JSON fields
- Already in ISO 8601 format (`ts` field)
- Generates URLs from `spotify_track_uri`
- Automatically filters non-music content
- Extracts artist and album from metadata fields

### Lexicon Reference

This importer follows the official `fm.teal.alpha` lexicon defined in `/lexicons/fm.teal.alpha/feed/play.json`.

The lexicon defines required and optional field types, string length constraints, array formats, timestamp formatting, and URL validation.

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

AGPL-3.0-only - See LICENCE file for details

## Credits

- Uses [@atproto/api](https://www.npmjs.com/package/@atproto/api) for ATProto interactions
- CSV parsing via [csv-parse](https://www.npmjs.com/package/csv-parse)
- Identity resolution via [Slingshot](https://slingshot.danner.cloud)
- Follows the `fm.teal.alpha` lexicon standard
- Colored output via [chalk](https://www.npmjs.com/package/chalk)
- Progress indicators via [ora](https://www.npmjs.com/package/ora) and [cli-progress](https://www.npmjs.com/package/cli-progress)

---

**Note**: This tool is for personal use. Respect the terms of service and rate limits when importing your data.
