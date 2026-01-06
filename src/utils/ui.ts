/**
 * UI utilities for polished CLI output with spinners, progress bars, and colors
 */
import ora, { Ora } from 'ora';
import cliProgress from 'cli-progress';
import chalk from 'chalk';

// Singleton spinner instance
let currentSpinner: Ora | null = null;

/**
 * Create and start a spinner
 */
export function startSpinner(text: string): Ora {
  stopSpinner(); // Stop any existing spinner
  currentSpinner = ora({
    text,
    color: 'cyan',
    spinner: 'dots'
  }).start();
  return currentSpinner;
}

/**
 * Update spinner text
 */
export function updateSpinner(text: string): void {
  if (currentSpinner) {
    currentSpinner.text = text;
  }
}

/**
 * Stop and clear spinner with success
 */
export function succeedSpinner(text?: string): void {
  if (currentSpinner) {
    currentSpinner.succeed(text);
    currentSpinner = null;
  }
}

/**
 * Stop and clear spinner with failure
 */
export function failSpinner(text?: string): void {
  if (currentSpinner) {
    currentSpinner.fail(text);
    currentSpinner = null;
  }
}

/**
 * Stop and clear spinner with warning
 */
export function warnSpinner(text?: string): void {
  if (currentSpinner) {
    currentSpinner.warn(text);
    currentSpinner = null;
  }
}

/**
 * Stop and clear spinner without message
 */
export function stopSpinner(): void {
  if (currentSpinner) {
    currentSpinner.stop();
    currentSpinner = null;
  }
}

/**
 * Create a progress bar for batch processing
 */
export function createProgressBar(total: number, title: string) {
  const bar = new cliProgress.SingleBar({
    format: `${chalk.cyan(title)} ${chalk.cyan('│')} {bar} ${chalk.cyan('│')} {percentage}% ${chalk.cyan('│')} {value}/{total} records ${chalk.cyan('│')} {speed} rec/s ${chalk.cyan('│')} ETA: {eta_formatted}`,
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true,
    clearOnComplete: false,
    stopOnComplete: true,
    formatValue: (v: number, options: any, type: string) => {
      switch (type) {
        case 'speed':
          return v.toFixed(1);
        default:
          return cliProgress.Format.ValueFormat(v, options, type);
      }
    }
  }, cliProgress.Presets.shades_classic);

  bar.start(total, 0, { speed: 0 });
  return bar;
}

/**
 * Format success message
 */
export function success(message: string): void {
  console.log(chalk.green('✓'), message);
}

/**
 * Format error message
 */
export function error(message: string): void {
  console.log(chalk.red('✗'), message);
}

/**
 * Format warning message
 */
export function warning(message: string): void {
  console.log(chalk.yellow('⚠'), message);
}

/**
 * Format info message
 */
export function info(message: string): void {
  console.log(chalk.blue('ℹ'), message);
}

/**
 * Format section header
 */
export function header(title: string): void {
  console.log(`\n${chalk.bold.cyan('═══')} ${chalk.bold.white(title)} ${chalk.bold.cyan('═══')}\n`);
}

/**
 * Format subsection
 */
export function subheader(title: string): void {
  console.log(chalk.bold.cyan(`\n${title}`));
}

/**
 * Format key-value pair
 */
export function keyValue(key: string, value: string | number, indent: number = 2): void {
  const spaces = ' '.repeat(indent);
  console.log(`${spaces}${chalk.dim(key + ':')} ${chalk.white(value)}`);
}

/**
 * Format a box around text
 */
export function box(lines: string[]): void {
  const maxLength = Math.max(...lines.map(l => l.length));
  const border = '─'.repeat(maxLength + 4);
  
  console.log(chalk.cyan('┌' + border + '┐'));
  lines.forEach(line => {
    const padding = ' '.repeat(maxLength - line.length);
    console.log(chalk.cyan('│  ') + line + padding + chalk.cyan('  │'));
  });
  console.log(chalk.cyan('└' + border + '┘'));
}

/**
 * Clear last N lines (for in-place updates)
 */
export function clearLines(n: number): void {
  for (let i = 0; i < n; i++) {
    process.stdout.write('\x1b[1A\x1b[2K');
  }
}

/**
 * Format duration in a nice way
 */
export function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  } else if (minutes > 0) {
    const secs = seconds % 60;
    return `${minutes}m ${secs}s`;
  } else {
    return `${seconds}s`;
  }
}

/**
 * Create a status line that updates in place
 */
export class StatusLine {
  private lastLength = 0;
  
  update(text: string): void {
    // Clear previous line
    process.stdout.write('\r' + ' '.repeat(this.lastLength) + '\r');
    
    // Write new text
    process.stdout.write(text);
    this.lastLength = text.length;
  }
  
  clear(): void {
    process.stdout.write('\r' + ' '.repeat(this.lastLength) + '\r');
    this.lastLength = 0;
  }
  
  finish(text?: string): void {
    if (text) {
      this.clear();
      console.log(text);
    } else {
      process.stdout.write('\n');
    }
    this.lastLength = 0;
  }
}

/**
 * Create a table
 */
export function table(headers: string[], rows: string[][]): void {
  const colWidths = headers.map((h, i) => 
    Math.max(h.length, ...rows.map(r => r[i]?.length || 0))
  );
  
  // Header
  const headerRow = headers.map((h, i) => h.padEnd(colWidths[i])).join('  │  ');
  console.log(chalk.cyan(headerRow));
  console.log(chalk.dim('─'.repeat(headerRow.length)));
  
  // Rows
  rows.forEach(row => {
    const formattedRow = row.map((cell, i) => cell.padEnd(colWidths[i])).join('  │  ');
    console.log(formattedRow);
  });
}
