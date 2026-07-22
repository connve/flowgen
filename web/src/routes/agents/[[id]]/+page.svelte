<script lang="ts">
	import { onMount, tick } from 'svelte';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import { marked } from 'marked';
	import Prism from 'prismjs';
	import 'prismjs/components/prism-sql';
	import 'prismjs/components/prism-yaml';
	import 'prismjs/components/prism-json';
	import 'prismjs/components/prism-bash';
	import 'prismjs/components/prism-python';
	import 'prismjs/components/prism-typescript';
	import 'prismjs/components/prism-markdown';
	import { apiUrl } from '$lib/api';
	import { formatRelative } from '$lib/time';
	import CopyButton from '$lib/CopyButton.svelte';
	import {
		listConversations,
		getConversation,
		putConversation,
		deleteConversation,
		deriveTitle,
		type ConversationSummary
	} from '$lib/conversations';

	interface ChatMessage {
		role: 'user' | 'assistant';
		content: string;
		at: number;
	}

	function formatTimestamp(ms: number): string {
		return new Date(ms).toLocaleString([], {
			month: 'short',
			day: 'numeric',
			hour: '2-digit',
			minute: '2-digit'
		});
	}

	// Highlight fenced code blocks with Prism when the language is known,
	// matching the ResourceViewer treatment. Falls back to plain escaped
	// text for unknown languages.
	marked.use({
		renderer: {
			code({ text, lang }) {
				const grammar = lang && Prism.languages[lang];
				const body = grammar ? Prism.highlight(text, grammar, lang) : escapeHtml(text);
				return `<pre class="language-${lang ?? 'none'}"><code>${body}</code></pre>`;
			}
		}
	});

	function escapeHtml(s: string): string {
		return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
	}

	function renderMarkdown(text: string): string {
		return marked.parse(text, { async: false }) as string;
	}

	let models = $state<string[]>([]);
	let model = $state<string>('');
	let modelsError = $state<string | null>(null);

	// Models arrive as `<proxy>/<model>`. The proxy prefix is an implementation
	// detail, so hide it — unless more than one proxy is present, where it
	// disambiguates. The `<select>` value stays the full id the gateway needs.
	let multiProxy = $derived(new Set(models.map((m) => m.split('/')[0])).size > 1);
	function modelLabel(id: string): string {
		return multiProxy ? id : (id.split('/').slice(1).join('/') || id);
	}

	let messages = $state<ChatMessage[]>([]);
	let input = $state('');
	let sending = $state(false);
	let error = $state<string | null>(null);
	let scroller = $state<HTMLDivElement | null>(null);

	// Conversation history, served from flowgen's system cache. Summaries only
	// (no message bodies); the active one is fetched in full on demand. The URL
	// is the source of truth for which conversation is open: `/agents/{id}`, or
	// `/agents` for a fresh unsaved chat.
	let conversations = $state<ConversationSummary[]>([]);
	let activeId = $derived(page.params.id ?? null);
	let historyPaneOpen = $state(true);
	let convoSearch = $state('');
	// Id we've already loaded messages for, so the load effect doesn't refetch
	// on unrelated state changes or clobber a chat we just minted an id for.
	let loadedId = $state<string | null>(null);
	// True while fetching a conversation's messages by id, so the chat area
	// shows a spinner instead of an empty/flashing view on a slow load.
	let convoLoading = $state(false);

	// Title shown in the breadcrumb: the active conversation's, or a
	// placeholder for a fresh chat.
	let activeTitle = $derived(
		activeId === null
			? 'New conversation'
			: (conversations.find((c) => c.id === activeId)?.title ?? 'Conversation')
	);

	// Case-insensitive title filter for the history pane; empty search shows all.
	let filteredConversations = $derived(
		convoSearch.trim() === ''
			? conversations
			: conversations.filter((c) =>
					c.title.toLowerCase().includes(convoSearch.trim().toLowerCase())
				)
	);

	async function refreshConversations() {
		try {
			conversations = await listConversations();
		} catch (err) {
			// A history-store hiccup shouldn't break the chat itself.
			console.error('Failed to load conversations', err);
		}
	}

	// Whenever the URL id changes, load that conversation's messages (or clear
	// to a fresh chat for `/agents`). Skips a reload of the id we already show.
	$effect(() => {
		const id = activeId;
		if (id === loadedId) return;
		// While a reply is streaming, `persistConversation` mints an id and
		// navigates to `/agents/{id}` — that URL change must NOT reload messages
		// out from under the live stream. Adopt the id as already-loaded and
		// leave the on-screen messages untouched.
		if (sending) {
			loadedId = id;
			return;
		}
		if (id === null) {
			messages = [];
			error = null;
			loadedId = null;
			resetComposer();
			return;
		}
		loadedId = id;
		convoLoading = true;
		getConversation(id)
			.then((c) => {
				// Guard against a race: the URL may have changed again mid-fetch.
				if (activeId !== c.id) return;
				messages = c.messages.map((m) => ({ ...m }) as ChatMessage);
				error = null;
				resetComposer();
				scrollToBottom();
			})
			.catch((err) => {
				error = err instanceof Error ? err.message : 'Failed to load conversation.';
			})
			.finally(() => {
				if (activeId === id) convoLoading = false;
			});
	});

	function resetComposer() {
		input = '';
		if (textareaEl) textareaEl.style.height = 'auto';
		multiline = false;
	}

	// Switching conversations mid-stream would leave the reply writing into the
	// old messages while `activeId` points elsewhere, cross-saving the turn onto
	// the wrong conversation. Block navigation until the stream finishes.
	function openConversation(id: string) {
		if (sending) return;
		goto(`${base}/agents/${id}`);
	}

	function newConversation() {
		if (sending) return;
		goto(`${base}/agents`);
	}

	// Deletes a stored conversation. If it's the one on screen, navigate back to
	// a fresh chat so the view doesn't keep re-saving a deleted id.
	async function removeConversation(id: string) {
		try {
			await deleteConversation(id);
			if (activeId === id) newConversation();
			await refreshConversations();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to delete conversation.';
		}
	}

	// Persists the current messages, minting an id + title on first save and
	// navigating to `/agents/{id}` so the URL reflects the live conversation.
	// Runs on send and after each reply so history survives a reload.
	async function persistConversation() {
		if (messages.length === 0) return;
		const isNew = activeId === null;
		const id = activeId ?? crypto.randomUUID();
		const title = conversations.find((c) => c.id === id)?.title ?? deriveTitle(messages[0].content);
		try {
			// Adopt the minted id before navigating so the load effect treats it
			// as already-loaded and doesn't refetch (and blank) the live chat.
			if (isNew) loadedId = id;
			await putConversation(
				id,
				title,
				messages.map((m) => ({ role: m.role, content: m.content, at: m.at }))
			);
			if (isNew) goto(`${base}/agents/${id}`);
			await refreshConversations();
		} catch (err) {
			console.error('Failed to persist conversation', err);
		}
	}

	function toggleHistoryPane() {
		historyPaneOpen = !historyPaneOpen;
		localStorage.setItem('flowgen-agents-history-pane', historyPaneOpen ? '1' : '0');
	}

	onMount(async () => {
		refreshConversations();
		const pane = localStorage.getItem('flowgen-agents-history-pane');
		if (pane !== null) historyPaneOpen = pane === '1';

		try {
			const res = await fetch(apiUrl('api/agents/models'));
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const body = await res.json();
			models = (body.data ?? []).map((m: { id: string }) => m.id);
			if (models.length > 0) model = models[0];
			else modelsError = 'No models registered on the gateway.';
		} catch (err) {
			modelsError =
				err instanceof Error ? err.message : 'Failed to load models from the AI gateway.';
		}
	});

	async function scrollToBottom() {
		await tick();
		if (scroller) scroller.scrollTop = scroller.scrollHeight;
	}

	async function send() {
		const text = input.trim();
		if (!text || sending || !model) return;

		error = null;
		// Set `sending` before anything awaits, so the URL-load effect stays
		// out of `messages` for the whole turn (persist's `goto` changes the
		// URL mid-stream; without this the reload races the stream and the
		// bubbles get clobbered/merged).
		sending = true;
		messages.push({ role: 'user', content: text, at: Date.now() });
		input = '';
		if (textareaEl) textareaEl.style.height = 'auto';
		multiline = false;
		// Save as soon as the user sends, so the conversation exists (and shows
		// in history) even before the reply arrives — and survives a reload if
		// the reply never comes.
		await persistConversation();
		// Index of the assistant message we stream tokens into. Mutating
		// through `messages[idx]` keeps the reactive proxy in the array
		// updated — holding an outside reference to the pushed object does not.
		const idx = messages.push({ role: 'assistant', content: '', at: Date.now() }) - 1;
		await scrollToBottom();

		try {
			const res = await fetch(apiUrl('api/agents/chat'), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					model,
					stream: true,
					messages: messages
						.slice(0, -1)
						.map((m) => ({ role: m.role, content: m.content })),
				}),
			});

			if (!res.ok || !res.body) {
				throw new Error(`Gateway responded with HTTP ${res.status}`);
			}

			const reader = res.body.getReader();
			const decoder = new TextDecoder();
			let buffer = '';

			// Parse the OpenAI-style SSE stream: `data: {json}\n\n`, terminated
			// by a `data: [DONE]` sentinel.
			while (true) {
				const { value, done } = await reader.read();
				if (done) break;
				buffer += decoder.decode(value, { stream: true });
				const lines = buffer.split('\n');
				buffer = lines.pop() ?? '';
				for (const line of lines) {
					const trimmed = line.trim();
					if (!trimmed.startsWith('data:')) continue;
					const payload = trimmed.slice(5).trim();
					if (payload === '[DONE]') continue;
					try {
						const chunk = JSON.parse(payload);
						const delta = chunk.choices?.[0]?.delta?.content;
						if (delta) {
							messages[idx].content += delta;
							await scrollToBottom();
						}
					} catch {
						// Ignore keep-alives and non-JSON frames.
					}
				}
			}

			if (messages[idx].content.length === 0) {
				messages[idx].content = '(no content returned)';
			}
			messages[idx].at = Date.now();
			// Save the completed turn so it survives a reload.
			await persistConversation();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Chat request failed.';
			// Drop the empty assistant bubble on failure.
			if (messages[idx]?.content.length === 0) messages.splice(idx, 1);
		} finally {
			sending = false;
			await scrollToBottom();
		}
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			send();
		}
	}

	let textareaEl = $state<HTMLTextAreaElement | null>(null);
	// True once the input wraps past one line — used to drop the send button to
	// the bottom (centred looks off next to a tall box).
	let multiline = $state(false);

	// Grow the textarea with its content up to the CSS max-height, then scroll.
	function autoGrow(el: HTMLTextAreaElement) {
		el.style.height = 'auto';
		el.style.height = `${el.scrollHeight}px`;
		multiline = el.scrollHeight > 44;
	}

	function onInput(e: Event) {
		autoGrow(e.currentTarget as HTMLTextAreaElement);
	}
</script>

<svelte:head>
	<title>Agents | Flowgen</title>
</svelte:head>

<section class="flex h-[calc(100vh-4rem)] min-w-0 overflow-hidden">
	<!-- History pane, mirroring the folders pane in Resources 1:1. -->
	<aside
		class="flex shrink-0 flex-col border-r border-base-300 bg-base-100 transition-[width] duration-200 ease-out {historyPaneOpen
			? 'w-64'
			: 'w-16'}"
	>
		{#if !historyPaneOpen}
			<div class="flex flex-1 flex-col items-center gap-1 py-2">
				<div class="tooltip tooltip-right" data-tip="New">
					<button
						type="button"
						aria-label="New"
						class="flex h-10 w-10 items-center justify-center rounded-md bg-primary text-primary-content transition-colors hover:bg-primary/90 disabled:opacity-50"
						onclick={newConversation}
						disabled={sending}
					>
						<Icon icon="tabler:plus" class="h-5 w-5 shrink-0" />
					</button>
				</div>
				<div class="tooltip tooltip-right" data-tip="Conversations">
					<button
						type="button"
						aria-label="Expand conversations"
						class="relative flex h-10 w-10 items-center justify-center rounded-md bg-base-200 text-primary transition-colors hover:bg-base-200"
						onclick={toggleHistoryPane}
					>
						<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
						<Icon icon="tabler:message-2" class="h-5 w-5 shrink-0" />
					</button>
				</div>
			</div>
		{:else}
			<div class="flex min-h-0 flex-1 flex-col px-3 py-2">
				<!-- Pinned header: New + root stay put; only the list below scrolls. -->
				<button
					type="button"
					class="btn btn-sm btn-primary w-full shrink-0 justify-start gap-1.5"
					onclick={newConversation}
					disabled={sending}
				>
					<Icon icon="tabler:plus" class="h-4 w-4" />
					New
				</button>
				<button
					type="button"
					class="relative mt-2 flex w-full items-center gap-1.5 h-10 shrink-0 rounded-md px-2 text-left text-sm transition-colors {activeId ===
					null
						? 'bg-base-200 font-medium text-primary'
						: 'hover:bg-base-200'}"
					onclick={newConversation}
				>
					{#if activeId === null}
						<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
					{/if}
					<Icon icon="tabler:message-2" class="h-5 w-5 shrink-0 opacity-70" />
					<span>All conversations</span>
					<span class="ml-auto text-xs opacity-50">
						{#if convoSearch.trim()}{filteredConversations.length} of {conversations.length}{:else}{conversations.length}{/if}
					</span>
				</button>
				<ul class="mt-1 min-h-0 flex-1 space-y-0.5 overflow-y-auto text-sm">
					{#each filteredConversations as c (c.id)}
						<li class="group relative">
							<button
								type="button"
								class="relative flex w-full flex-col gap-0.5 rounded-md py-2 pl-2 pr-8 text-left transition-colors {activeId ===
								c.id
									? 'bg-base-200 font-medium text-primary'
									: 'hover:bg-base-200'}"
								onclick={() => openConversation(c.id)}
							>
								{#if activeId === c.id}
									<span
										class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"
									></span>
								{/if}
								<span
									class="tooltip tooltip-right block w-full truncate before:max-w-xs before:whitespace-normal before:break-words"
									data-tip={c.title}>{c.title}</span>
								<span class="text-xs opacity-50">{formatRelative(c.updatedAt)}</span>
							</button>
							<div
								class="tooltip tooltip-left absolute right-1 top-1/2 -translate-y-1/2 opacity-0 transition-opacity group-hover:opacity-100"
								data-tip="Delete"
							>
								<button
									type="button"
									class="btn btn-ghost btn-circle btn-xs"
									aria-label="Delete conversation"
									onclick={() => removeConversation(c.id)}
								>
									<Icon icon="tabler:trash" class="h-5 w-5" />
								</button>
							</div>
						</li>
					{/each}
				</ul>
			</div>
		{/if}
		<div
			class="flex h-12 shrink-0 items-center border-t border-base-200 {historyPaneOpen
				? 'justify-end px-3'
				: 'justify-center'}"
		>
			<div
				class="tooltip {historyPaneOpen ? 'tooltip-top' : 'tooltip-right'}"
				data-tip={historyPaneOpen ? 'Collapse conversations' : 'Expand conversations'}
			>
				<button
					type="button"
					aria-label={historyPaneOpen ? 'Collapse conversations' : 'Expand conversations'}
					class="flex h-10 w-10 items-center justify-center rounded-md text-base-content/70 transition-colors hover:bg-base-200 hover:text-base-content"
					onclick={toggleHistoryPane}
				>
					<Icon
						icon={historyPaneOpen ? 'tabler:chevron-left' : 'tabler:chevron-right'}
						class="h-5 w-5"
					/>
				</button>
			</div>
		</div>
	</aside>

	<div class="flex min-w-0 flex-1 flex-col overflow-hidden">
	<div class="shrink-0 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6">
		<div class="flex flex-wrap items-center gap-2">
			<div class="flex min-w-0 items-center gap-1.5 text-sm">
				<a href="{base}/agents" class="shrink-0 hover:text-primary">Agents</a>
				<span class="shrink-0 opacity-40">/</span>
				<span
					class="tooltip tooltip-bottom truncate before:max-w-xs before:whitespace-normal before:break-words"
					data-tip={activeTitle}>{activeTitle}</span>
			</div>

			<div class="dropdown">
				<button
					type="button"
					tabindex="0"
					class="btn btn-sm border border-base-300 bg-base-100 font-normal hover:bg-base-200"
					disabled={models.length === 0}
				>
					<Icon icon="tabler:brain" class="h-4 w-4 opacity-70" />
					<span>{model ? modelLabel(model) : 'Model'}</span>
					<Icon icon="tabler:chevron-down" class="h-3.5 w-3.5 opacity-60" />
				</button>
				<div
					tabindex="-1"
					class="dropdown-content z-10 mt-1 max-h-72 w-56 overflow-auto rounded-md border border-base-200 bg-base-100 p-1 shadow-lg"
				>
					{#each models as m (m)}
						<button
							type="button"
							class="flex w-full items-center gap-2 rounded px-2 py-1 text-left text-sm hover:bg-base-200"
							onclick={() => (model = m)}
						>
							<span class="inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center text-primary">
								{#if model === m}
									<Icon icon="tabler:check" class="h-3.5 w-3.5" />
								{/if}
							</span>
							<span class="truncate">{modelLabel(m)}</span>
						</button>
					{/each}
				</div>
			</div>

			<div class="flex-1"></div>

			<label
				class="input input-sm flex items-center gap-2 border border-base-300 bg-base-100 outline-none focus-within:border-primary"
			>
				<svg
					class="h-4 w-4 opacity-50"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<circle cx="11" cy="11" r="8" />
					<path d="M21 21l-4.35-4.35" />
				</svg>
				<input type="text" placeholder="Search conversations..." bind:value={convoSearch} />
				{#if convoSearch}
					<div class="tooltip tooltip-left" data-tip="Clear search">
						<button
							type="button"
							class="opacity-50 hover:opacity-100"
							aria-label="Clear search"
							onclick={() => (convoSearch = '')}
						>
							<Icon icon="tabler:x" class="h-6 w-6" />
						</button>
					</div>
				{/if}
			</label>
		</div>
	</div>

	{#snippet composer()}
		<div
			class="flex gap-2 rounded-2xl border border-base-300 bg-base-100 py-1.5 pl-2 pr-1.5 shadow-sm transition-colors focus-within:border-primary {multiline
				? 'items-end'
				: 'items-center'}"
		>
			<textarea
				bind:this={textareaEl}
				class="max-h-48 min-h-[2.5rem] flex-1 resize-none bg-transparent px-2 py-2 text-[0.9375rem] leading-6 outline-none placeholder:text-base-content/40"
				rows="1"
				placeholder="Send a message..."
				bind:value={input}
				onkeydown={onKeydown}
				oninput={onInput}
				disabled={sending || !model}
			></textarea>
			<div class="tooltip tooltip-top shrink-0" data-tip="Send">
				<button
					type="button"
					class="btn btn-primary btn-circle btn-sm flex h-8 min-h-8 w-8 items-center justify-center"
					onclick={send}
					disabled={sending || !model || input.trim().length === 0}
					aria-label="Send"
				>
					<Icon icon="tabler:arrow-up" class="h-5 w-5" />
				</button>
			</div>
		</div>
	{/snippet}

	{#if modelsError}
		<div class="p-6">
			<div class="alert alert-warning" role="alert">
				<span>{modelsError}</span>
			</div>
		</div>
	{:else if convoLoading && messages.length === 0}
		<!-- Loading a conversation by id — spinner instead of a flashing empty state. -->
		<div class="flex flex-1 items-center justify-center">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if messages.length === 0}
		<!-- Empty state: greeting + composer centred, like a landing prompt. -->
		<div class="relative flex flex-1 flex-col items-center justify-center gap-7 overflow-hidden px-6">
			<!-- Connve brand glow: three drifting blurred blobs behind the prompt. -->
			<div class="hero-gradient pointer-events-none absolute inset-0" aria-hidden="true">
				<div class="hero-blob hero-blob-1"></div>
				<div class="hero-blob hero-blob-2"></div>
				<div class="hero-blob hero-blob-3"></div>
			</div>
			<p class="edge-word relative text-3xl font-extrabold tracking-tight">
				What's on your mind today?
			</p>
			<div class="relative w-full max-w-2xl">
				{@render composer()}
			</div>
		</div>
	{:else}
		<!-- Conversation: scrolling messages, composer pinned to the bottom. -->
		<div bind:this={scroller} class="min-h-0 flex-1 overflow-y-auto px-6 py-6">
			<div class="mx-auto flex max-w-3xl flex-col gap-5">
				{#each messages as m, i (i)}
					{@const streaming = sending && m.role === 'assistant' && i === messages.length - 1}
					{@const typing = streaming && m.content.length === 0}
					<div class="group flex flex-col gap-1 {m.role === 'user' ? 'items-end' : 'items-start'}">
						{#if typing}
							<span class="flex gap-1 px-1 py-2" aria-label="Assistant is typing">
								<span class="typing-dot h-1.5 w-1.5 rounded-full bg-base-content/50"></span>
								<span
									class="typing-dot h-1.5 w-1.5 rounded-full bg-base-content/50"
									style="animation-delay: 0.15s"
								></span>
								<span
									class="typing-dot h-1.5 w-1.5 rounded-full bg-base-content/50"
									style="animation-delay: 0.3s"
								></span>
							</span>
						{:else if m.role === 'user'}
							<div
								class="max-w-[85%] whitespace-pre-wrap rounded-2xl bg-primary px-4 py-2.5 text-sm text-primary-content shadow-sm"
							>
								{m.content}
							</div>
						{:else}
							<div
								class="chat-markdown prose prose-sm max-w-[85%] min-w-0 rounded-2xl border border-base-200 bg-base-100/60 px-4 py-2.5 text-sm text-base-content shadow-sm {streaming
									? 'chat-streaming'
									: ''}"
							>
								<!-- eslint-disable-next-line svelte/no-at-html-tags -->
								{@html renderMarkdown(m.content)}
							</div>
						{/if}
						{#if !typing && !streaming}
							<div
								class="flex items-center gap-1 px-1 opacity-0 transition-opacity group-hover:opacity-100 {m.role ===
								'user'
									? 'flex-row-reverse'
									: ''}"
							>
								<CopyButton text={m.content} size="xs" />
								<span class="text-xs tabular-nums text-base-content/40">
									{formatTimestamp(m.at)}
								</span>
							</div>
						{/if}
					</div>
				{/each}
			</div>
		</div>

		{#if error}
			<div class="mx-auto w-full max-w-3xl px-6">
				<div class="alert alert-error alert-sm mb-2" role="alert">
					<span>{error}</span>
				</div>
			</div>
		{/if}

		<div class="shrink-0 px-6 pb-6 pt-2">
			<div class="mx-auto max-w-3xl">
				{@render composer()}
			</div>
		</div>
	{/if}
	</div>
</section>

<style>
	/* Connve brand glow, adapted from connve.com's hero: three radial-gradient
	   blobs blended and blurred, drifting slowly. Only shown in the chat empty
	   state — a spacious, landing-like moment where it fits, unlike the dense
	   data views. */
	.hero-gradient {
		filter: blur(80px);
		opacity: 0.22;
	}
	:global(:root[data-theme='mydark']) .hero-gradient {
		opacity: 0.28;
	}
	.hero-blob {
		position: absolute;
		border-radius: 9999px;
		mix-blend-mode: multiply;
		will-change: transform;
	}
	:global(:root[data-theme='mydark']) .hero-blob {
		mix-blend-mode: screen;
	}
	.hero-blob-1 {
		top: 5%;
		left: 12%;
		width: 30vw;
		height: 30vw;
		background: radial-gradient(circle, #00e168 0%, transparent 70%);
		animation: blob-drift-1 18s ease-in-out infinite;
	}
	.hero-blob-2 {
		top: 25%;
		right: 12%;
		width: 28vw;
		height: 28vw;
		background: radial-gradient(circle, #00b4a6 0%, transparent 70%);
		animation: blob-drift-2 22s ease-in-out infinite;
	}
	.hero-blob-3 {
		bottom: 5%;
		left: 38%;
		width: 26vw;
		height: 26vw;
		background: radial-gradient(circle, #006b55 0%, transparent 75%);
		animation: blob-drift-3 26s ease-in-out infinite;
	}
	@keyframes blob-drift-1 {
		0%,
		100% {
			transform: translate(0, 0) scale(1);
		}
		33% {
			transform: translate(8%, 12%) scale(1.1);
		}
		66% {
			transform: translate(-5%, 8%) scale(0.95);
		}
	}
	@keyframes blob-drift-2 {
		0%,
		100% {
			transform: translate(0, 0) scale(1);
		}
		33% {
			transform: translate(-10%, 5%) scale(0.9);
		}
		66% {
			transform: translate(5%, -8%) scale(1.05);
		}
	}
	@keyframes blob-drift-3 {
		0%,
		100% {
			transform: translate(0, 0) scale(1);
		}
		33% {
			transform: translate(10%, -8%) scale(1.08);
		}
		66% {
			transform: translate(-8%, 5%) scale(0.92);
		}
	}
	/* Accent-gradient text clip, matching connve.com's shimmering headline word. */
	.edge-word {
		background: linear-gradient(120deg, #006b55 0%, #00b4a6 50%, #00e168 100%);
		background-size: 200% 200%;
		-webkit-background-clip: text;
		background-clip: text;
		-webkit-text-fill-color: transparent;
		color: transparent;
		animation: edge-shimmer 6s ease-in-out infinite;
	}
	@keyframes edge-shimmer {
		0%,
		100% {
			background-position: 0% 50%;
		}
		50% {
			background-position: 100% 50%;
		}
	}
	@media (prefers-reduced-motion: reduce) {
		.hero-blob,
		.edge-word {
			animation: none;
		}
	}

	/* Blinking cursor appended after the last content element while streaming,
	   so it reads as the last character rather than a stray bar below. */
	.chat-streaming :global(> :last-child)::after {
		content: '';
		display: inline-block;
		width: 0.4rem;
		height: 1em;
		margin-left: 0.15rem;
		vertical-align: text-bottom;
		background: var(--color-base-content);
		opacity: 0.6;
		animation: chat-cursor-blink 1s step-end infinite;
	}
	@keyframes chat-cursor-blink {
		50% {
			opacity: 0;
		}
	}
	@media (prefers-reduced-motion: reduce) {
		.chat-streaming :global(> :last-child)::after {
			animation: none;
		}
	}

	/* Code blocks in assistant messages: a calm surface with the shared Prism
	   palette, matching the resource viewer rather than prose defaults. */
	/* No border here — the message bubble already frames it, so a border would
	   read as a box-in-a-box. A tinted surface keeps code distinct. */
	.chat-markdown :global(pre) {
		margin: 0.5rem 0;
		padding: 0.875rem 1rem;
		border-radius: 0.6rem;
		background: color-mix(in oklab, var(--color-base-200) 70%, var(--color-base-300));
		overflow-x: auto;
		font-size: 0.8125rem;
		line-height: 1.6;
	}
	.chat-markdown :global(pre:first-child) {
		margin-top: 0;
	}
	.chat-markdown :global(pre:last-child) {
		margin-bottom: 0;
	}
	.chat-markdown :global(pre code) {
		background: none;
		padding: 0;
		font-size: inherit;
		color: var(--color-base-content);
	}
	/* Inline code (not in a pre). */
	.chat-markdown :global(:not(pre) > code) {
		background: var(--color-base-200);
		border: 1px solid var(--color-base-300);
		border-radius: 0.3rem;
		padding: 0.1rem 0.35rem;
		font-size: 0.85em;
		font-weight: 500;
	}
	.chat-markdown :global(p:first-child) {
		margin-top: 0;
	}
	.chat-markdown :global(p:last-child) {
		margin-bottom: 0;
	}
</style>
