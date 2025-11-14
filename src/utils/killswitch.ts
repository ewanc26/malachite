let cancelled = false;

// Flip the killswitch when the user hits CTRL-C
process.on('SIGINT', () => {
  console.log('\nCaught CTRL-C — stopping import…');
  cancelled = true;
});

/**
 * Manually cancel the import if needed.
 */
export function cancelImport() {
  cancelled = true;
}

/**
 * Check whether the import should stop.
 * Call this inside loops, batch processors, etc.
 */
export function isImportCancelled(): boolean {
  return cancelled;
}
