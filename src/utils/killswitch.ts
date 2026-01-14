let cancelled = false;
let sigintHandlerRegistered = false;

/**
 * Register the SIGINT handler (should be called early in the application)
 */
export function registerKillswitch(): void {
  if (sigintHandlerRegistered) {
    return;
  }

  // Flip the killswitch when the user hits CTRL-C
  process.on('SIGINT', () => {
    if (cancelled) {
      // If already cancelled and user presses Ctrl+C again, force exit
      console.log('\n\nForce quit detected. Exiting immediately...');
      process.exit(1);
    }
    
    console.log('\n\n⚠️  Ctrl+C detected — stopping after current batch completes...');
    console.log('Press Ctrl+C again to force quit immediately.\n');
    cancelled = true;
  });

  sigintHandlerRegistered = true;
}

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
