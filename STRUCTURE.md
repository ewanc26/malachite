# Last.fm to ATProto Importer - Modular Structure

## Project Structure

```plaintext
lastfm-importer/
├── src/
│   ├── index.js              # Main entry point
│   ├── config.js             # Configuration constants
│   ├── lib/                  # Core library modules
│   │   ├── auth.js           # Authentication & login
│   │   ├── cli.js            # CLI argument parsing & help
│   │   ├── csv.js            # CSV parsing & conversion
│   │   └── publisher.js      # Record publishing logic
│   └── utils/                # Utility functions
│       ├── helpers.js        # Helper functions (formatting, batch calculation)
│       ├── input.js          # User input & password masking
│       └── killswitch.js     # Graceful shutdown handling
├── importer.js               # Wrapper for backwards compatibility
└── importer.old.js           # Original monolithic version (backup)
```

## Module Responsibilities

### `/src/config.js`

- Configuration constants
- Batch size calculation parameters
- API endpoints and client information

### `/src/lib/auth.js`

- ATProto authentication
- Identity resolution via Slingshot
- Login error handling

### `/src/lib/cli.js`

- Command-line argument parsing
- Help text display
- Input validation

### `/src/lib/csv.js`

- CSV file parsing
- Record conversion to ATProto format
- Chronological sorting

### `/src/lib/publisher.js`

- Batch publishing with rate limiting
- Dry-run preview mode
- Progress tracking and reporting
- Killswitch integration

### `/src/utils/helpers.js`

- Duration formatting
- Optimal batch size calculation (logarithmic algorithm)
- Generic utility functions

### `/src/utils/input.js`

- Interactive prompts
- Password masking with asterisks
- Backspace support

### `/src/utils/killswitch.js`

- SIGINT handler
- Graceful shutdown state management
- Force-quit on second Ctrl+C

## Benefits of Modular Structure

1. **Maintainability**: Each module has a single responsibility
2. **Testability**: Individual modules can be tested in isolation
3. **Reusability**: Modules can be imported and reused
4. **Readability**: Smaller files are easier to understand
5. **Collaboration**: Multiple developers can work on different modules
6. **Debugging**: Easier to locate and fix issues

## Usage

The wrapper file (`importer.js`) maintains backwards compatibility:

```bash
# Still works exactly as before
node importer.js -f lastfm.csv -i handle.bsky.social

# Or use the modular version directly
node src/index.js -f lastfm.csv -i handle.bsky.social
```

## Algorithm Details

### Batch Size Calculation

Located in `/src/utils/helpers.js`:

```javascript
batchSize = BASE + (log2(records/MIN) * SCALING_FACTOR)
```

- **Time Complexity**: O(n) - each record processed once
- **Space Complexity**: O(b) where b is batch size
- **Rate Limit Strategy**: Token bucket approach
- **Adaptive**: Adjusts based on total records and delay settings

### Processing Order

- Default: Chronological (oldest first)
- Option: `--reverse-chronological` for newest first
- Sorted by `playedTime` field

## Future Improvements

With the modular structure, it's now easier to:

- Add unit tests for each module
- Implement different authentication methods
- Support multiple export formats (JSON, XML)
- Add progress persistence (resume interrupted imports)
- Implement retry logic with exponential backoff
- Add statistics and analytics
- Create a web UI that imports these modules
