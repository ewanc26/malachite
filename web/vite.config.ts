import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	// Ensure Buffer / global are polyfilled for @atproto/api in the browser
	define: {
		global: 'globalThis'
	},
	optimizeDeps: {
		include: ['@atproto/api', '@atproto/common-web']
	},
	build: {
		target: 'es2022'
	}
});
