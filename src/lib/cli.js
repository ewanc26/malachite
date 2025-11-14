import { parseArgs } from 'node:util';

/**
 * Parse command line arguments
 */
export function parseCommandLineArgs() {
  const options = {
    help: {
      type: 'boolean',
      short: 'h',
      default: false,
    },
    file: {
      type: 'string',
      short: 'f',
    },
    identifier: {
      type: 'string',
      short: 'i',
    },
    password: {
      type: 'string',
      short: 'p',
    },
    'batch-size': {
      type: 'string',
      short: 'b',
    },
    'batch-delay': {
      type: 'string',
      short: 'd',
    },
    yes: {
      type: 'boolean',
      short: 'y',
      default: false,
    },
    'dry-run': {
      type: 'boolean',
      short: 'n',
      default: false,
    },
    'reverse-chronological': {
      type: 'boolean',
      short: 'r',
      default: false,
    },
  };

  try {
    const { values } = parseArgs({ options, allowPositionals: false });
    return values;
  } catch (error) {
    console.error('Error parsing arguments:', error.message);
    showHelp();
    process.exit(1);
  }
}

/**
 * Show help message
 */
export function showHelp() {
  console.log(`
Last.fm to ATProto Importer

Usage: node importer.js [options]

Options:
  -h, --help                     Show this help message
  -f, --file <path>              Path to Last.fm CSV export file
  -i, --identifier <id>          ATProto handle or DID
  -p, --password <pass>          ATProto app password
  -b, --batch-size <num>         Number of records per batch (auto-calculated if not set)
  -d, --batch-delay <ms>         Delay between batches in ms (default: 2000, min: 1000)
  -y, --yes                      Skip confirmation prompt
  -n, --dry-run                  Preview records without publishing
  -r, --reverse-chronological    Process newest first (default: oldest first)

Examples:
  node importer.js -f lastfm.csv -i alice.bsky.social -p xxxx-xxxx-xxxx-xxxx
  node importer.js --file export.csv --identifier alice.bsky.social --yes
  node importer.js -f lastfm.csv --dry-run
  node importer.js  (interactive mode - prompts for all values)

Notes:
  - Batch size uses logarithmic scaling algorithm (O(n) complexity) for optimal throughput
  - Auto-calculated batch size considers both record count and delay settings
  - Records are processed in chronological order (oldest first) by default
  - Minimum batch delay of 1000ms enforced to respect rate limits
  - Rate limiting follows token bucket strategy for safe API usage
`);
}
