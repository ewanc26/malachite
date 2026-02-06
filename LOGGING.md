# Comprehensive Deep Logging Guide

This document describes the deep logging system now integrated throughout Malachite for troubleshooting, monitoring, and understanding program flow.

## Logging Levels

The logger supports 5 levels:

- **DEBUG** (0): Very detailed information, useful for developers/debugging
- **INFO** (1): General informational messages (default)
- **WARN** (2): Warning messages
- **ERROR** (3): Error messages
- **SILENT** (4): No logging output

## Enabling Debug Logging

To see comprehensive debug logs, set the environment variable before running:

```bash
# Enable DEBUG level logging
DEBUG=* pnpm start -i your-file.csv

# Or set it explicitly
NODE_DEBUG_LOGS=1 pnpm start -i your-file.csv
```

Then logs will output to:
- **Console**: All log messages with colors
- **File**: `~/.malachite/logs/import-TIMESTAMP.log` (platform-aware paths)

## What Gets Logged

### Platform Detection & Initialization
- **File**: `src/utils/platform.ts`
- **Logs**:
  - Platform detection (Windows/macOS/Linux)
  - State directory resolution with env var checking
  - Cache and logs directory paths
  - Locale number formatting

**Example**:
```
[platform.ts] getPlatform() detected: macos (node os.platform() = darwin)
[platform.ts] getMalachiteStateDir() on macOS: /Users/ewan/Library/Application Support/malachite
[platform.ts] formatLocaleNumber(1234567) => 1,234,567
```

### Rate Limit Headers Parsing
- **File**: `src/utils/rate-limit-headers.ts`
- **Logs**:
  - Header parsing attempts with all variants (ratelimit-*, x-ratelimit-*, RateLimit-*)
  - Extracted policy windows
  - Rate limit error detection logic

**Example**:
```
[rate-limit-headers.ts] parseRateLimitHeaders() called with headers: ["ratelimit-limit","ratelimit-remaining","ratelimit-reset"]
[rate-limit-headers.ts] Extracted window from policy: 3600s from "5000;w=3600"
[rate-limit-headers.ts] parseRateLimitHeaders() result: limit=5000, remaining=4999, reset=1707213600, window=3600s
[rate-limit-headers.ts] isRateLimitError() detected: status 429
```

### Rate Limiting State Management
- **File**: `src/utils/rate-limiter.ts`
- **Logs**:
  - Batch size/delay calculations with safety margins
  - State file read/write operations
  - Quota pre-decrement operations
  - Wait-for-permit logic with reset windows
  - Estimated import duration breakdowns

**Example**:
```
[rate-limiter.ts] calculateRateLimitedBatches(totalRecords=50000)
[rate-limiter.ts] Daily limit: 2,500 (raw limit=5000, safety margin=0.5)
[rate-limiter.ts] Rate limiting needed: true (50,000 records > 2,500 daily limit)
[rate-limiter.ts] Estimated duration: 20 day(s), spreading 2,500 records/day
[RateLimiter] constructor: stateFile=/Users/ewan/Library/Application Support/malachite/state/rate-limit.json, safety=0.5
[RateLimiter] ensureStateDir() creating: /Users/ewan/Library/Application Support/malachite/state
[RateLimiter] readState() loaded from ...: {"limit":5000,"remaining":2450,"windowSeconds":3600,"updatedAt":1707209823}
[RateLimiter] waitForPermit(30) called, current remaining: 2450/5000
[RateLimiter] waitForPermit() quota available, pre-decrementing 2450 -> 2420
[RateLimiter] persistState() writing to ...: {"limit":5000,"remaining":2420}
```

### Publisher Batch Processing
- **File**: `src/lib/publisher.ts`
- **Logs**:
  - Configuration parameters (batch size, delay, safety margin)
  - Resume state info
  - Per-batch progress with calculations
  - Adaptive adjustment triggers (speedups/slowdowns)
  - Rate limit hit detection and recovery
  - Error details with context
  - Performance statistics (speed, elapsed time, estimates)

**Example**:
```
[publisher.ts] MAX_APPLY_WRITES_OPS=200, POINTS_PER_RECORD=3
[publisher.ts] Safety margin: 0.5, Records per day limit: 5,000
[publisher.ts] Starting batch: index=0, size=10
[publisher.ts] Reserving quota: batch_size=10, points=30 (3 per record)
[publisher.ts] Sending applyWrites request for 10 records to PDS...
[publisher.ts] Batch success: 10/10 records published in 542ms
[publisher.ts] Updating rate limiter from response headers
⚡ Speeding up! Delay: 2000ms → 1600ms
[publisher.ts] Stats - Elapsed: 12s | Speed: 45.3 rec/s | Success: 453/1000 | Remaining: ~12s
[publisher.ts] Rate limit error headers: ["ratelimit-limit","ratelimit-remaining"]
[publisher.ts] Waiting for rate limit reset, requesting 90 points...
[RateLimiter] Rate limited! Quota exhausted (150/5000 remaining). Waiting 3600s for quota reset...
```

## Log Files

By default, logs are written to platform-specific directories:

- **macOS**: `~/Library/Application Support/malachite/logs/`
- **Windows**: `%APPDATA%\malachite\logs\`
- **Linux**: `~/.config/malachite/logs/` (or `$XDG_CONFIG_HOME/malachite/logs/`)

Each import session creates a new file: `import-2025-02-06T15-23-45.log`

Files contain all logs with ANSI color codes stripped for cleaner text files.

## State Persistence Files

Rate limit state is persisted to:

- **macOS**: `~/Library/Application Support/malachite/state/rate-limit.json`
- **Windows**: `%APPDATA%\malachite\state\rate-limit.json`
- **Linux**: `~/.config/malachite/state/rate-limit.json`

Content example:
```json
{
  "limit": 5000,
  "remaining": 2420,
  "windowSeconds": 3600,
  "updatedAt": 1707209823
}
```

This state is read on startup to continue respecting rate limits across sessions.

## Debugging Common Issues

### "Rate limit hit!" appearing frequently

Look for these debug logs:
```
[rate-limit-headers.ts] parseRateLimitHeaders() result: remaining=XXXX
[RateLimiter] Rate limited! Quota exhausted (XXX/YYYY remaining). Waiting...
```

This indicates the server is returning 429 responses. The logs show:
- How many requests remaining before hitting the limit
- How long the wait window is
- Whether the reset is happening

### Batch size/delay not adjusting

Look for:
```
[publisher.ts] Starting batch: index=X, size=Y
[rate-limiter.ts] Final batch parameters: size=X, delay=Yms
```

If these don't change over time, check:
- Whether speed-up conditions are met (5 consecutive successes)
- Whether failure conditions are triggered (3+ consecutive failures)

### Platform path issues

Look for:
```
[platform.ts] getMalachiteStateDir() using APPDATA on Windows: ...
[platform.ts] getMalachiteStateDir() XDG_CONFIG_HOME not set, using default on Linux: ...
```

This confirms which path the application is using. You can override with:
- **Windows**: Set `APPDATA` environment variable
- **Linux**: Set `XDG_CONFIG_HOME` environment variable

## Performance Monitoring

Key metrics to watch in logs:

1. **Speed (records/sec)**:
   ```
   [publisher.ts] Stats - Speed: 45.3 rec/s
   ```

2. **Batch success rate**:
   ```
   [publisher.ts] Batch success: 450/500 records
   ```

3. **Rate limit headroom**:
   ```
   [RateLimiter] Rate limit info: limit=5000, remaining_with_safety=2450
   ```

4. **Estimated time remaining**:
   ```
   [publisher.ts] Stats - Remaining: ~2m 30s
   ```

## Adding More Logging

When debugging specific issues, you can add more granular logging:

```typescript
// In any module:
import { log } from '../utils/logger.js';

// At start of function:
log.debug(`[module-name] functionName() called with args: ${JSON.stringify(args)}`);

// During processing:
log.debug(`[module-name] Processing item ${i}/${total}: ${description}`);

// On completion:
log.debug(`[module-name] Result: ${JSON.stringify(result)}`);
```

All debug logs will appear when `DEBUG=*` is set and be written to both console and file.

## Summary

The comprehensive logging system now provides visibility into:

✅ Platform detection and path resolution  
✅ Rate limit header parsing and error detection  
✅ Stateful rate limit tracking and quota management  
✅ Batch processing progress and adaptive adjustments  
✅ Performance metrics and time estimates  
✅ Error details with full context  
✅ State persistence and resume capabilities  

This enables fast troubleshooting and monitoring of the import process across all operating systems.
