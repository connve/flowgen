<script lang="ts">
	import Sidebar from '$lib/components/Sidebar.svelte';
	import { onMount } from 'svelte';

	let { children } = $props();

	onMount(() => {
		// Add copy buttons to all code blocks.
		document.querySelectorAll('pre').forEach((pre) => {
			if (pre.querySelector('.copy-btn')) return;

			const wrapper = document.createElement('div');
			wrapper.style.position = 'relative';
			pre.parentNode?.insertBefore(wrapper, pre);
			wrapper.appendChild(pre);

			const btn = document.createElement('button');
			btn.className = 'copy-btn';
			btn.textContent = 'Copy';
			btn.addEventListener('click', () => {
				const code = pre.querySelector('code');
				if (code) {
					navigator.clipboard.writeText(code.textContent || '');
					btn.textContent = 'Copied';
					setTimeout(() => (btn.textContent = 'Copy'), 2000);
				}
			});
			wrapper.appendChild(btn);
		});
	});
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
