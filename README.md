# Last.fm to ATProto Importer

Import your Last.fm listening history to the AT Protocol network using the `fm.teal.alpha.feed.play` lexicon.

(Also [on Tangled!](https://tangled.org/@did:plc:ofrbh253gwicbkc5nktqepol/atproto-lastfm-importer))

## Features

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
- ✅ **MusicBrainz Support**: Preserves MusicBrainz IDs when available
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

### Interactive Mode

The simplest way to use the importer - just run it and follow the prompts:

```bash
npm start
```

### Command Line Mode

For automation or scripting, provide all parameters via flags:

```bash
# Full automation
npm start -- -f lastfm.csv -i alice.bsky.social -p xxxx-xxxx-xxxx-xxxx -y

# Preview without publishing
npm start -- -f lastfm.csv --dry-run

# Custom batch settings (advanced users)
npm start -- -f lastfm.csv -i alice.bsky.social -b 20 -d 3000

# Process newest tracks first
npm start -- -f lastfm.csv -i alice.bsky.social -r -y
```

## Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--help` | `-h` | Show help message | - |
| `--file <path>` | `-f` | Path to Last.fm CSV export file | (prompted) |
| `--identifier <id>` | `-i` | ATProto handle or DID | (prompted) |
| `--password <pass>` | `-p` | ATProto app password | (prompted) |
| `--batch-size <num>` | `-b` | Records per batch | Auto-calculated |
| `--batch-delay <ms>` | `-d` | Delay between batches in ms | 2000 (min: 1000) |
| `--yes` | `-y` | Skip confirmation prompt | false |
| `--dry-run` | `-n` | Preview without publishing | false |
| `--reverse-chronological` | `-r` | Process newest first | false (oldest first) |

### Batch Settings

The importer automatically calculates optimal batch settings based on your total record count and rate limits. You generally **don't need** to specify batch settings unless you have specific requirements.

**Automatic behavior:**
- For imports < 1K records: Uses default settings (10 records/batch, 2s delay)
- For imports > 1K records: Automatically calculates settings to spread across multiple days

**Manual override** (advanced):
- `--batch-size`: Number of records processed per batch (1-50)
- `--batch-delay`: Milliseconds to wait between batches (min: 1000)

⚠️ Lower delays increase speed but risk hitting rate limits. The automatic calculation is recommended.

## Getting Your Last.fm Data

1. Go to <https://lastfm.ghan.nl/export/>
2. Request your data export in CSV format
3. Download the CSV file when ready
4. Use the CSV file path with this script

## What Gets Imported

Each Last.fm scrobble becomes an `fm.teal.alpha.feed.play` record with:

### Required Fields
- **trackName**: The name of the track
- **artists**: Array of artist objects (requires `artistName`, optional `artistMbId`)
- **playedTime**: ISO 8601 timestamp of when you listened
- **submissionClientAgent**: Identifies this importer (`lastfm-importer/v0.0.2`)
- **musicServiceBaseDomain**: Always set to `last.fm`

### Optional Fields (when available)
- **releaseName**: Album/release name
- **releaseMbId**: MusicBrainz release ID
- **recordingMbId**: MusicBrainz recording/track ID
- **originUrl**: Link to the track on Last.fm

### Example Record

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
  "submissionClientAgent": "lastfm-importer/v0.0.2",
  "musicServiceBaseDomain": "last.fm"
}
```

## Processing Order

By default, records are processed **oldest first** (chronological order). This means your earliest scrobbles will appear first in your ATProto feed.

Use the `--reverse-chronological` or `-r` flag to process **newest first** instead.

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
   Batch size: 10 records
   Batch delay: 9600.0s
```

## Dry Run Mode

Preview what will be imported without actually publishing:

```bash
npm start -- -f lastfm.csv --dry-run
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
│   │   ├── cli.ts          # Command line argument parsing
│   │   ├── csv.ts          # CSV parsing & record conversion
│   │   └── publisher.ts    # Batch publishing with rate limiting
│   ├── utils/
│   │   ├── helpers.ts      # Utility functions (timing, formatting)
│   │   ├── input.ts        # User input handling (prompts, passwords)
│   │   └── rate-limiter.ts # Rate limiting calculations
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
4. Enforces minimum 1 second delay between batches
5. Shows clear schedule before starting

### Record Processing
1. Parses CSV using `csv-parse` library
2. Sorts records chronologically (or reverse if `-r` flag)
3. Converts Last.fm format to `fm.teal.alpha.feed.play` schema
4. Validates required fields
5. Publishes in batches with configurable delays

### Data Mapping
- **Track info**: Direct mapping from CSV columns
- **Timestamps**: Converts Unix timestamps to ISO 8601
- **MusicBrainz IDs**: Preserved when present in CSV
- **URLs**: Generated from artist/track names
- **Artists**: Wrapped in array format with optional MBID

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

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

MIT License - See LICENSE file for details

## Credits

- Uses [@atproto/api](https://www.npmjs.com/package/@atproto/api) for ATProto interactions
- CSV parsing via [csv-parse](https://www.npmjs.com/package/csv-parse)
- Identity resolution via [Slingshot](https://slingshot.danner.cloud)
- Follows the `fm.teal.alpha` lexicon standard

---

**Note**: This tool is for personal use. Respect Last.fm's terms of service and rate limits when exporting your data.
