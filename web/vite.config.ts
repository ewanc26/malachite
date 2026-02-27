import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const webPkg = JSON.parse(readFileSync(resolve('package.json'), 'utf-8'));
const cliPkg = JSON.parse(readFileSync(resolve('../package.json'), 'utf-8'));

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	// Ensure Buffer / global are polyfilled for @atproto/api in the browser
	define: {
		global: 'globalThis',
		__WEB_VERSION__: JSON.stringify(webPkg.version),
		__CLI_VERSION__: JSON.stringify(cliPkg.version)
	},
	optimizeDeps: {
		include: ['@atproto/api', '@atproto/common-web']
	},
	build: {
		target: 'es2022'
	}
});
