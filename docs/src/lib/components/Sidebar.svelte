<script lang="ts">
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { navigation } from '$lib/nav';

	function closeDrawer() {
		const toggle = document.getElementById('sidebar-drawer') as HTMLInputElement;
		if (toggle) toggle.checked = false;
	}
</script>

<nav class="w-64 shrink-0 border-r border-base-300 bg-base-100 overflow-y-auto h-full">
	<div class="p-4">
		<a href="{base}/" class="hidden lg:flex items-center gap-2 mb-6">
			<span class="text-xl font-bold text-primary leading-none">flowgen</span>
			<span class="badge badge-outline text-xs px-2 py-0.5">docs</span>
		</a>

		{#each navigation as section}
			<div class="mb-4">
				<a href="{base}{section.items[0].href}" class="text-xs font-semibold uppercase tracking-wider text-base-content/50 mb-2 flex items-center gap-1.5 hover:text-base-content transition-colors">
					{#if section.icon}
						<img src="{base}{section.icon}" alt="" class="w-4 h-4" />
					{/if}
					{section.title}
				</a>
				<ul class="menu menu-sm p-0">
					{#each section.items as item}
						<li>
							<a
								href="{base}{item.href}"
								class={page.url.pathname === `${base}${item.href}` ? 'active font-medium' : ''}
								onclick={closeDrawer}
							>
								{item.title}
							</a>
						</li>
					{/each}
				</ul>
				{#if section.subsections}
					{#each section.subsections as sub}
						<div class="mt-1 ml-2 border-l border-base-300 pl-2">
							<span class="text-[10px] font-medium uppercase tracking-wider text-base-content/35">{sub.title}</span>
							<ul class="menu menu-sm p-0">
								{#each sub.items as item}
									<li>
										<a
											href="{base}{item.href}"
											class={page.url.pathname === `${base}${item.href}` ? 'active font-medium' : ''}
											onclick={closeDrawer}
										>
											{item.title}
										</a>
									</li>
								{/each}
							</ul>
						</div>
					{/each}
				{/if}
			</div>
		{/each}
	</div>
</nav>
