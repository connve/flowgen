<script lang="ts">
	import { afterNavigate } from '$app/navigation';

	interface Heading {
		id: string;
		text: string;
		level: number;
	}

	/** How far below the viewport top a heading counts as reached: the top bar plus a margin. */
	const OFFSET = 96;

	let headings = $state<Heading[]>([]);
	let active = $state<string | undefined>();
	let elements: HTMLElement[] = [];
	let frame = 0;

	function collect() {
		elements = [...document.querySelectorAll<HTMLElement>('article.prose :is(h2, h3)[id]')];
		headings = elements.map((element) => ({
			id: element.id,
			text: element.textContent?.trim() ?? '',
			level: element.tagName === 'H2' ? 2 : 3
		}));
		spy();
	}

	function spy() {
		cancelAnimationFrame(frame);
		frame = requestAnimationFrame(() => {
			const bottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2;
			let reached = elements[0];
			for (const element of elements) {
				if (element.getBoundingClientRect().top <= OFFSET) reached = element;
			}
			active = (bottom ? elements.at(-1) : reached)?.id;
		});
	}

	afterNavigate(collect);
</script>

<svelte:window onscroll={spy} onresize={spy} />

{#if headings.length > 1}
	<nav aria-labelledby="toc-title">
		<h2
			id="toc-title"
			class="mb-2 text-xs font-semibold tracking-wider text-base-content/60 uppercase"
		>
			On this page
		</h2>
		<ul class="space-y-0.5 border-l border-base-200 text-[13px]">
			{#each headings as heading (heading.id)}
				<li>
					<a
						href="#{heading.id}"
						aria-current={heading.id === active ? 'location' : undefined}
						class={[
							'-ml-px block border-l py-1 leading-snug transition-colors',
							heading.level === 3 ? 'pl-6' : 'pl-3',
							heading.id === active
								? 'border-primary font-medium text-base-content'
								: 'border-transparent text-base-content/60 hover:text-base-content'
						]}
					>
						{heading.text}
					</a>
				</li>
			{/each}
		</ul>
	</nav>
{/if}
