import { describe, it } from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { RateLimiter } from '../utils/rate-limiter.js';
import { getMalachiteStateDir } from '../utils/platform.js';

describe('RateLimiter', () => {
  it('persists state after updateFromHeaders and applies safety margin', () => {
    const rl = new RateLimiter({ safety: 0.5 });
    const now = Math.floor(Date.now() / 1000);
    const headers = {
      'ratelimit-limit': '100',
      'ratelimit-remaining': '80',
      'ratelimit-reset': String(now + 3600),
      'ratelimit-policy': '100;w=3600',
    };
    rl.updateFromHeaders(headers);
    const statePath = path.join(getMalachiteStateDir(), 'state', 'rate-limit.json');
    assert.ok(fs.existsSync(statePath), 'Persisted state file should exist');
    const raw = fs.readFileSync(statePath, 'utf8');
    const o = JSON.parse(raw);
    // safety = 0.5 so remaining should be floored(80 * 0.5) = 40
    assert.strictEqual(o.remaining, Math.floor(80 * 0.5));
    assert.strictEqual(o.limit, 100);
    assert.strictEqual(o.windowSeconds, 3600);
  });

  it('waitForPermit pre-decrements remaining when quota available', async () => {
    const rl = new RateLimiter({ safety: 1.0 });
    const now = Math.floor(Date.now() / 1000);
    const headers = {
      'ratelimit-limit': '10',
      'ratelimit-remaining': '3',
      'ratelimit-reset': String(now + 3600),
      'ratelimit-policy': '10;w=3600',
    };
    rl.updateFromHeaders(headers);
    const statePath = path.join(getMalachiteStateDir(), 'state', 'rate-limit.json');
    const before = JSON.parse(fs.readFileSync(statePath, 'utf8')).remaining;
    await rl.waitForPermit(1);
    const after = JSON.parse(fs.readFileSync(statePath, 'utf8')).remaining;
    assert.strictEqual(after, Math.max(0, before - 1));
  });
});
