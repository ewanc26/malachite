/**
 * In-memory rate limiter for browser use.
 * Mirrors the interface of src/utils/rate-limiter.ts without any Node.js deps.
 */

interface State {
  limit: number;
  remaining: number;
  resetAt: number; // unix seconds
  windowSeconds: number;
}

export class BrowserRateLimiter {
  private state: State | null = null;
  private readonly headroom: number;

  constructor(opts?: { headroom?: number }) {
    this.headroom = opts?.headroom ?? 0.15;
  }

  updateFromHeaders(headers: Record<string, string>): void {
    const h = (k: string) => headers[k.toLowerCase()] ?? headers[k] ?? '';
    const limitStr = h('ratelimit-limit') || h('x-ratelimit-limit');
    const remainingStr = h('ratelimit-remaining') || h('x-ratelimit-remaining');
    const resetStr = h('ratelimit-reset') || h('x-ratelimit-reset');
    const policy = h('ratelimit-policy') || h('x-ratelimit-policy');

    const limit = parseInt(limitStr, 10);
    const remaining = parseInt(remainingStr, 10);
    const reset = parseInt(resetStr, 10);

    if (!limit || isNaN(limit) || isNaN(remaining)) return;

    let windowSeconds = 3600;
    const m = /;w=(\d+)/.exec(policy);
    if (m) windowSeconds = parseInt(m[1], 10);

    const now = Math.floor(Date.now() / 1000);
    this.state = {
      limit,
      remaining,
      resetAt: isNaN(reset) ? now + windowSeconds : reset,
      windowSeconds
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

  async waitForPermit(pointsNeeded: number): Promise<void> {
    if (!this.state) return; // no state yet — let first request probe

    const now = Math.floor(Date.now() / 1000);
    if (now >= this.state.resetAt) {
      this.state.remaining = this.state.limit;
      this.state.resetAt = now + this.state.windowSeconds;
    }

    const headroomPts = Math.floor(this.state.limit * this.headroom);
    const effective = this.state.remaining - headroomPts;

    if (effective < pointsNeeded) {
      // Wait until the window resets
      const waitMs = Math.max(0, (this.state.resetAt - Math.floor(Date.now() / 1000)) + 1) * 1000;
      await new Promise((r) => setTimeout(r, waitMs));
      if (this.state) {
        this.state.remaining = this.state.limit;
        this.state.resetAt = Math.floor(Date.now() / 1000) + this.state.windowSeconds;
      }
    }

    if (this.state) this.state.remaining = Math.max(0, this.state.remaining - pointsNeeded);
  }
}
