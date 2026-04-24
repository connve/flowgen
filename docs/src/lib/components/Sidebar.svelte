<script lang="ts">
	import { page } from '$app/state';
	import { navigation } from '$lib/nav';
</script>

<nav class="w-64 shrink-0 border-r border-base-300 bg-base-100 overflow-y-auto h-full">
	<div class="p-4">
		<a href="/" class="flex items-center gap-2 mb-6">
			<span class="text-xl font-bold text-primary">flowgen</span>
			<span class="badge badge-sm badge-outline">docs</span>
		</a>

		{#each navigation as section}
			<div class="mb-4">
				<a href={section.items[0].href} class="text-xs font-semibold uppercase tracking-wider text-base-content/50 mb-2 px-2 flex items-center gap-1.5 hover:text-base-content transition-colors">
					{#if section.icon}
						<img src={section.icon} alt="" class="w-4 h-4" />
					{/if}
					{section.title}
				</a>
				<ul class="menu menu-sm p-0">
					{#each section.items as item}
						<li>
							<a
								href={item.href}
								class={page.url.pathname === item.href ? 'active font-medium' : ''}
							>
								{item.title}
							</a>
						</li>
					{/each}
				</ul>
			</div>
		{/each}
	</div>
</nav>
