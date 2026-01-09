/**
 * TID Audit and Reporting Tools
 * 
 * Utilities for auditing TID generation and producing reports
 */

import { validateTid, decodeTidTimestamp, decodeTidClockId, compareTids } from './tid-clock.js';

export interface TidAuditEntry {
  tid: string;
  valid: boolean;
  timestamp?: number;
  clockId?: number;
  date?: Date;
  errors: string[];
}

export interface TidAuditReport {
  totalTids: number;
  validTids: number;
  invalidTids: number;
  duplicates: number;
  monotonic: boolean;
  firstTid?: TidAuditEntry;
  lastTid?: TidAuditEntry;
  entries: TidAuditEntry[];
  errors: string[];
}

/**
 * Audit a list of TIDs
 */
export function auditTids(tids: string[]): TidAuditReport {
  const entries: TidAuditEntry[] = [];
  const seenTids = new Set<string>();
  let duplicates = 0;
  let validCount = 0;
  let invalidCount = 0;
  const globalErrors: string[] = [];

  // Audit each TID
  for (const tid of tids) {
    const entry: TidAuditEntry = {
      tid,
      valid: false,
      errors: [],
    };

    // Check for duplicates
    if (seenTids.has(tid)) {
      duplicates++;
      entry.errors.push('Duplicate TID');
    }
    seenTids.add(tid);

    // Validate format
    const valid = validateTid(tid);
    entry.valid = valid;

    if (valid) {
      validCount++;
      try {
        entry.timestamp = decodeTidTimestamp(tid);
        entry.clockId = decodeTidClockId(tid);
        entry.date = new Date(entry.timestamp / 1000);
      } catch (error) {
        entry.errors.push(`Decode error: ${error}`);
      }
    } else {
      invalidCount++;
      entry.errors.push('Invalid format');
    }

    entries.push(entry);
  }

  // Check monotonicity
  let monotonic = true;
  for (let i = 1; i < tids.length; i++) {
    if (compareTids(tids[i - 1], tids[i]) >= 0) {
      monotonic = false;
      globalErrors.push(`Non-monotonic at index ${i}: ${tids[i - 1]} >= ${tids[i]}`);
    }
  }

  return {
    totalTids: tids.length,
    validTids: validCount,
    invalidTids: invalidCount,
    duplicates,
    monotonic,
    firstTid: entries[0],
    lastTid: entries[entries.length - 1],
    entries,
    errors: globalErrors,
  };
}

/**
 * Format audit report as human-readable text
 */
export function formatAuditReport(report: TidAuditReport): string {
  const lines: string[] = [];

  lines.push('='.repeat(80));
  lines.push('TID AUDIT REPORT');
  lines.push('='.repeat(80));
  lines.push('');

  // Summary
  lines.push('SUMMARY');
  lines.push('-'.repeat(80));
  lines.push(`Total TIDs:      ${report.totalTids.toLocaleString()}`);
  lines.push(`Valid:           ${report.validTids.toLocaleString()} (${((report.validTids / report.totalTids) * 100).toFixed(1)}%)`);
  lines.push(`Invalid:         ${report.invalidTids.toLocaleString()}`);
  lines.push(`Duplicates:      ${report.duplicates.toLocaleString()}`);
  lines.push(`Monotonic:       ${report.monotonic ? '✓ YES' : '✗ NO'}`);
  lines.push('');

  // Time range
  if (report.firstTid && report.firstTid.date) {
    lines.push('TIME RANGE');
    lines.push('-'.repeat(80));
    lines.push(`First TID:       ${report.firstTid.tid}`);
    lines.push(`  Timestamp:     ${report.firstTid.date.toISOString()}`);
    lines.push(`  Clock ID:      ${report.firstTid.clockId}`);
    lines.push('');
    if (report.lastTid && report.lastTid.date) {
      lines.push(`Last TID:        ${report.lastTid.tid}`);
      lines.push(`  Timestamp:     ${report.lastTid.date.toISOString()}`);
      lines.push(`  Clock ID:      ${report.lastTid.clockId}`);
      const durationMs = (report.lastTid.timestamp! - report.firstTid.timestamp!) / 1000;
      const durationDays = durationMs / (1000 * 60 * 60 * 24);
      lines.push(`  Duration:      ${durationDays.toFixed(2)} days`);
      lines.push('');
    }
  }

  // Errors
  if (report.errors.length > 0) {
    lines.push('ERRORS');
    lines.push('-'.repeat(80));
    for (const error of report.errors.slice(0, 10)) {
      lines.push(`  • ${error}`);
    }
    if (report.errors.length > 10) {
      lines.push(`  ... and ${report.errors.length - 10} more errors`);
    }
    lines.push('');
  }

  // Invalid TIDs
  const invalidEntries = report.entries.filter(e => !e.valid || e.errors.length > 0);
  if (invalidEntries.length > 0) {
    lines.push('INVALID/PROBLEM TIDs');
    lines.push('-'.repeat(80));
    for (const entry of invalidEntries.slice(0, 10)) {
      lines.push(`  ${entry.tid}`);
      for (const error of entry.errors) {
        lines.push(`    - ${error}`);
      }
    }
    if (invalidEntries.length > 10) {
      lines.push(`  ... and ${invalidEntries.length - 10} more invalid TIDs`);
    }
    lines.push('');
  }

  lines.push('='.repeat(80));

  return lines.join('\n');
}

/**
 * Format audit report as JSON
 */
export function formatAuditReportJson(report: TidAuditReport): string {
  return JSON.stringify(report, null, 2);
}
