import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const webPkg = JSON.parse(readFileSync(resolve('package.json'), 'utf-8'));
const cliPkg = JSON.parse(readFileSync(resolve('../package.json'), 'utf-8'));

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],

	server: {
		host: '127.0.0.1',
		port: 5173,
		fs: {
			allow: [
				resolve('..') // allow workspace root
			]
		}
	},

	define: {
		global: 'globalThis',
		__WEB_VERSION__: JSON.stringify(webPkg.version),
		__CLI_VERSION__: JSON.stringify(cliPkg.version)
	},

	optimizeDeps: {
		include: ['@atproto/api', '@atproto/common-web']
	},

	build: {
		target: 'es2022',
		chunkSizeWarningLimit: 1000
	}
});