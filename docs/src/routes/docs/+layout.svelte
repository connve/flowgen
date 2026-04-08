<script lang="ts">
	import Sidebar from '$lib/components/Sidebar.svelte';

	let { children } = $props();
</script>

<div class="flex min-h-screen">
	<div class="hidden lg:block sticky top-0 h-screen">
		<Sidebar />
	</div>

	<main class="flex-1 min-w-0">
		<div class="navbar bg-base-100 border-b border-base-300 sticky top-0 z-40 lg:hidden">
			<div class="navbar-start">
				<a href="/" class="text-xl font-bold text-primary px-4">flowgen</a>
			</div>
		</div>

		<div class="max-w-3xl mx-auto px-6 py-10">
			<article class="prose">
				{@render children()}
			</article>
		</div>
	</main>
</div>
