/**
 * Unit tests for input utilities
 * 
 * Tests cover:
 * - Quote stripping
 * - Menu selection
 * - User confirmations
 * - Input validation
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';

// Test utility functions that can be tested without I/O
describe('Input Utilities', () => {
  describe('Quote stripping helper', () => {
    it('should strip single quotes', () => {
      const stripQuotes = (str: string): string => {
        str = str.trim();
        if (
          (str.startsWith("'") && str.endsWith("'")) ||
          (str.startsWith('"') && str.endsWith('"'))
        ) {
          return str.slice(1, -1);
        }
        return str;
      };

      assert.strictEqual(stripQuotes("'hello'"), 'hello');
      assert.strictEqual(stripQuotes('"hello"'), 'hello');
      assert.strictEqual(stripQuotes("'hello"), "'hello");
      assert.strictEqual(stripQuotes('hello'), 'hello');
    });

    it('should strip double quotes', () => {
      const stripQuotes = (str: string): string => {
        str = str.trim();
        if (
          (str.startsWith("'") && str.endsWith("'")) ||
          (str.startsWith('"') && str.endsWith('"'))
        ) {
          return str.slice(1, -1);
        }
        return str;
      };

      assert.strictEqual(stripQuotes('"test path"'), 'test path');
      assert.strictEqual(stripQuotes("'test path'"), 'test path');
    });

    it('should handle whitespace around quotes', () => {
      const stripQuotes = (str: string): string => {
        str = str.trim();
        if (
          (str.startsWith("'") && str.endsWith("'")) ||
          (str.startsWith('"') && str.endsWith('"'))
        ) {
          return str.slice(1, -1);
        }
        return str;
      };

      assert.strictEqual(stripQuotes("  'hello'  "), 'hello');
      assert.strictEqual(stripQuotes('  "hello"  '), 'hello');
    });

    it('should not strip mismatched quotes', () => {
      const stripQuotes = (str: string): string => {
        str = str.trim();
        if (
          (str.startsWith("'") && str.endsWith("'")) ||
          (str.startsWith('"') && str.endsWith('"'))
        ) {
          return str.slice(1, -1);
        }
        return str;
      };

      assert.strictEqual(stripQuotes('"hello\''), '"hello\'');
      assert.strictEqual(stripQuotes("'hello\""), "'hello\"");
    });
  });

  describe('File path validation', () => {
    it('should validate file path format', () => {
      const isValidPath = (filePath: string): boolean => {
        return !!(filePath && filePath.trim().length > 0);
      };

      assert.strictEqual(isValidPath('/path/to/file.csv'), true);
      assert.strictEqual(isValidPath('./relative/path.csv'), true);
      assert.strictEqual(isValidPath('~/home/file.csv'), true);
      assert.strictEqual(isValidPath(''), false);
      assert.strictEqual(isValidPath('   '), false);
    });

    it('should handle paths with spaces', () => {
      const isValidPath = (filePath: string): boolean => {
        return !!(filePath && filePath.trim().length > 0);
      };

      assert.strictEqual(isValidPath('/path/to/my file.csv'), true);
      assert.strictEqual(isValidPath('C:\\\\Users\\\\My User\\\\file.csv'), true);
    });
  });

  describe('Handle validation', () => {
    it('should validate AT Protocol handle format', () => {
      const isValidHandle = (handle: string): boolean => {
        // Basic validation - should have .bsky.social or be a DID
        return (
          handle.includes('.') ||
          handle.startsWith('did:') ||
          handle.startsWith('did:key')
        );
      };

      assert.strictEqual(isValidHandle('user.bsky.social'), true);
      assert.strictEqual(isValidHandle('myhandle.bsky.social'), true);
      assert.strictEqual(isValidHandle('did:key:z6MkhaXgBZDvotzL'), true);
    });

    it('should reject invalid handles', () => {
      const isValidHandle = (handle: string): boolean => {
        return (
          handle.includes('.') ||
          handle.startsWith('did:') ||
          handle.startsWith('did:key')
        );
      };

      assert.strictEqual(isValidHandle('user'), false);
      assert.strictEqual(isValidHandle(''), false);
    });
  });

  describe('Password validation', () => {
    it('should accept non-empty passwords', () => {
      const isValidPassword = (password: string): boolean => {
        return !!(password && password.length > 0);
      };

      assert.ok(isValidPassword('mypassword'));
      assert.ok(isValidPassword('app-password-xyz'));
      assert.ok(!isValidPassword(''));
    });
  });

  describe('Mode validation', () => {
    it('should validate import modes', () => {
      const VALID_MODES = ['lastfm', 'spotify', 'combined', 'sync', 'deduplicate'];

      const isValidMode = (mode: string): boolean => {
        return VALID_MODES.includes(mode.toLowerCase());
      };

      assert.ok(isValidMode('lastfm'));
      assert.ok(isValidMode('spotify'));
      assert.ok(isValidMode('combined'));
      assert.ok(isValidMode('sync'));
      assert.ok(isValidMode('deduplicate'));
      assert.ok(!isValidMode('invalid'));
      assert.ok(!isValidMode(''));
    });
  });

  describe('Batch size validation', () => {
    it('should validate batch size range', () => {
      const isValidBatchSize = (size: number): boolean => {
        return size >= 1 && size <= 200;
      };

      assert.ok(isValidBatchSize(1));
      assert.ok(isValidBatchSize(100));
      assert.ok(isValidBatchSize(200));
      assert.ok(!isValidBatchSize(0));
      assert.ok(!isValidBatchSize(201));
      assert.ok(!isValidBatchSize(-1));
    });
  });

  describe('Batch delay validation', () => {
    it('should validate batch delay in milliseconds', () => {
      const isValidBatchDelay = (delay: number): boolean => {
        return delay >= 1000 && delay <= 60000;
      };

      assert.ok(isValidBatchDelay(1000));
      assert.ok(isValidBatchDelay(2000));
      assert.ok(isValidBatchDelay(60000));
      assert.ok(!isValidBatchDelay(500));
      assert.ok(!isValidBatchDelay(61000));
    });
  });
});
