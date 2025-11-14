import * as readline from 'readline';

/**
 * Read user input from command line with proper password masking
 */
export function prompt(question, hideInput = false) {
  return new Promise((resolve) => {
    if (hideInput) {
      // For password input, use raw mode
      const stdin = process.stdin;
      const wasRaw = stdin.isRaw;
      
      // Set raw mode to capture individual keystrokes
      if (stdin.isTTY) {
        stdin.setRawMode(true);
      }
      
      stdin.resume();
      stdin.setEncoding('utf8');
      
      process.stdout.write(question);
      
      let password = '';
      const onData = (char) => {
        char = char.toString();
        
        switch (char) {
          case '\n':
          case '\r':
          case '\u0004': // Ctrl-D
            stdin.removeListener('data', onData);
            if (stdin.isTTY) {
              stdin.setRawMode(wasRaw);
            }
            stdin.pause();
            process.stdout.write('\n');
            resolve(password);
            break;
          case '\u0003': // Ctrl-C
            process.exit(1);
            break;
          case '\u007f': // Backspace
          case '\b': // Backspace
            if (password.length > 0) {
              password = password.slice(0, -1);
              process.stdout.clearLine(0);
              process.stdout.cursorTo(0);
              process.stdout.write(question + '*'.repeat(password.length));
            }
            break;
          default:
            password += char;
            process.stdout.write('*');
            break;
        }
      };
      
      stdin.on('data', onData);
    } else {
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
      });
      
      rl.question(question, (answer) => {
        rl.close();
        resolve(answer);
      });
    }
  });
}
