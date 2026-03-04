/**
 * In-memory rate limiter — environment-agnostic.
 * Tracks quota from response headers and gates requests to stay within limits.
 */

import { normalizeHeaders } from './rate-limit-headers.js';

interface State {
  limit: number;
  remaining: number;
  resetAt: number; // unix seconds
  windowSeconds: number;
}

export class RateLimiter {
  private state: State | null = null;
  private readonly headroom: number;

  constructor(opts?: { headroom?: number }) {
    this.headroom = opts?.headroom ?? 0.15;
  }

  updateFromHeaders(headers: Record<string, string>): void {
    const h = normalizeHeaders(headers);
    const get = (k: string) => h[k] ?? h[`x-${k}`] ?? '';

    const limit = parseInt(get('ratelimit-limit'), 10);
    const remaining = parseInt(get('ratelimit-remaining'), 10);
    const reset = parseInt(get('ratelimit-reset'), 10);
    const policy = get('ratelimit-policy');

    if (!limit || isNaN(limit) || isNaN(remaining)) return;

    let windowSeconds = 3600;
    const m = /;w=(\d+)/.exec(policy);
    if (m) windowSeconds = parseInt(m[1], 10);

    const now = Math.floor(Date.now() / 1000);
    this.state = {
      limit,
      remaining,
      resetAt: isNaN(reset) ? now + windowSeconds : reset,
      windowSeconds,
    };
  }

  getActualRemaining(): number {
    if (!this.state) return 0;
    if (Math.floor(Date.now() / 1000) >= this.state.resetAt) return this.state.limit;
    return this.state.remaining;
  }

  getServerCapacity(): { limit: number; windowSeconds: number } | null {
    if (!this.state || this.state.limit === 0) return null;
    return { limit: this.state.limit, windowSeconds: this.state.windowSeconds };
  }

  hasServerInfo(): boolean {
    return this.state !== null && this.state.limit > 0;
  }

  /**
   * Called when the server returns a 429. Zeroes remaining so the next
   * `waitForPermit` actually blocks until the window resets.
   */
  handleRateLimitHit(errHeaders?: Record<string, string>): void {
    if (errHeaders && Object.keys(errHeaders).length > 0) {
      this.updateFromHeaders(errHeaders);
    }
    const now = Math.floor(Date.now() / 1000);
    if (this.state) {
      this.state.remaining = 0;
    } else {
      this.state = { limit: 5000, remaining: 0, resetAt: now + 60, windowSeconds: 3600 };
    }
  }

  /**
   * Wait until there is sufficient quota to send `pointsNeeded` points.
   * Polls `isCancelled` every 50 ms so callers can abort mid-wait.
   */
  async waitForPermit(pointsNeeded: number, isCancelled?: () => boolean): Promise<void> {
    if (!this.state) return; // no info yet — let first request probe

    const now = Math.floor(Date.now() / 1000);
    if (now >= this.state.resetAt) {
      this.state.remaining = this.state.limit;
      this.state.resetAt = now + this.state.windowSeconds;
    }

    const headroomPts = Math.floor(this.state.limit * this.headroom);
    const effective = this.state.remaining - headroomPts;

    if (effective < pointsNeeded) {
      const resetMs = Math.max(0, (this.state.resetAt - Math.floor(Date.now() / 1000)) + 1) * 1000;
      const end = Date.now() + resetMs;
      await new Promise<void>((resolve) => {
        const tick = () => {
          if (isCancelled?.() || Date.now() >= end) { resolve(); return; }
          setTimeout(tick, Math.min(50, end - Date.now()));
        };
        tick();
      });
      if (this.state && !isCancelled?.()) {
        this.state.remaining = this.state.limit;
        this.state.resetAt = Math.floor(Date.now() / 1000) + this.state.windowSeconds;
      }
    }

    if (this.state) this.state.remaining = Math.max(0, this.state.remaining - pointsNeeded);
  }
}
