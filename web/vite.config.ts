import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const webPkg = JSON.parse(readFileSync(resolve('package.json'), 'utf-8'));
const cliPkg = JSON.parse(readFileSync(resolve('../package.json'), 'utf-8'));

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],

	resolve: {
		alias: {
			// src/core/ files import these packages, but they live in web/node_modules.
			// Without explicit aliases, Rollup resolves bare specifiers relative to the
			// importing file (../src/core/) and never finds web/node_modules on Vercel.
			'@ipld/car':      resolve('node_modules/@ipld/car'),
			'@ipld/dag-cbor': resolve('node_modules/@ipld/dag-cbor'),
			'multiformats':   resolve('node_modules/multiformats'),
		},
	},

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