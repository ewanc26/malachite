// Global state for killswitch
let importCancelled = false;
let gracefulShutdown = false;

/**
 * Setup killswitch handler for graceful shutdown
 */
export function setupKillswitch() {
  process.on('SIGINT', () => {
    if (gracefulShutdown) {
      console.log('\n\n⚠️  Force quit detected. Exiting immediately...');
      process.exit(1);
    }
    
    gracefulShutdown = true;
    importCancelled = true;
    console.log('\n\n🛑 Killswitch activated! Stopping after current batch...');
    console.log('   Press Ctrl+C again to force quit immediately.\n');
  });
}

/**
 * Check if import has been cancelled
 */
export function isImportCancelled() {
  return importCancelled;
}

/**
 * Reset killswitch state (useful for testing)
 */
export function resetKillswitch() {
  importCancelled = false;
  gracefulShutdown = false;
}
