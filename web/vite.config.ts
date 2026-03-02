import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const webPkg = JSON.parse(readFileSync(resolve('package.json'), 'utf-8'));
const cliPkg = JSON.parse(readFileSync(resolve('../package.json'), 'utf-8'));

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	// Pin dev server to 127.0.0.1 — the OAuth loopback redirect_uri in
	// src/lib/core/oauth.ts must use the same origin (RFC 8252 §7.3).
	server: {
		host: '127.0.0.1',
		port: 5173,
	},
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
		target: 'es2022',
		chunkSizeWarningLimit: 1000 // @atproto/oauth-client-browser is large but unsplittable
	}
});
