<script lang="ts">
	import './layout.css';
	let { children } = $props();

	const webVersion: string = __WEB_VERSION__;
	const cliVersion: string = __CLI_VERSION__;
</script>

<svelte:head>
	<link
		rel="preconnect"
		href="https://fonts.googleapis.com"
	/>
	<link
		rel="preconnect"
		href="https://fonts.gstatic.com"
		crossorigin="anonymous"
	/>
	<link
		href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"
		rel="stylesheet"
	/>
</svelte:head>

<header>
	<span class="wordmark">malachite</span>
	<div class="version-strip">
	<span>web v{webVersion}</span>
	<span class="sep">–</span>
	<span>cli v{cliVersion}</span>
	</div>
</header>

{@render children()}

<style>
	header {
		position: sticky;
		top: 0;
		z-index: 10;
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 0.6rem 1.5rem;
		background: var(--bg);
		border-bottom: 1px solid var(--border);
	}
	.wordmark {
		font-size: 0.85rem;
		font-weight: 600;
		font-family: 'JetBrains Mono', monospace;
		color: var(--accent);
		letter-spacing: -0.02em;
	}
	.version-strip {
		font-size: 0.7rem;
		font-family: 'JetBrains Mono', monospace;
		color: var(--muted);
		letter-spacing: 0.03em;
		user-select: none;
	}
	.sep {
		margin: 0 0.4rem;
		color: var(--muted);
	}
</style>
