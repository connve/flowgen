<script lang="ts">
	import { onDestroy, onMount, tick, untrack } from 'svelte';
	import { renderMarkdown } from '$lib/markdown';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';
	import StateMessage from '$lib/StateMessage.svelte';
	import { apiUrl, errorMessage, failure } from '$lib/api';
	import { m } from '$lib/paraglide/messages.js';
	import { pageTitle } from '$lib/pageTitle';
	import PaneResizer from '$lib/PaneResizer.svelte';
	import { PaneWidth } from '$lib/paneWidth.svelte';
	import { isMobile, PANE_OVER_PAGE } from '$lib/mobile';
	import { formatRelative } from '$lib/time';
	import CopyButton from '$lib/CopyButton.svelte';
	import {
		getConversation,
		setConversationRead,
		startReply,
		stopReply,
		updateConversation,
		deleteConversation,
		type ConversationMessage,
		type ConversationSummary
	} from '$lib/conversations';
	import { agentConversations } from '$lib/agentConversations.svelte';
	import { connectorByName, connectorFor } from '$lib/connectors';
	import { buildTree, type TreeNode } from '$lib/tree';
	import { encodePath, type AgentTool } from '$lib/api';
	import { SvelteSet } from 'svelte/reactivity';
	import {
		formatDuration,
		releaseToolRuns,
		savedRun,
		toolRun,
		watchToolRuns,
		type RunTask,
		type ToolRun
	} from '$lib/toolRuns.svelte';
	import type { ActivityLevel } from '$lib/logRecord';

	// `role` narrows to the two values the UI ever produces; the generated
	// `ConversationMessage.role` is a bare `string` since the schema is shared
	// with any future author, so this stays a local type rather than swapping
	// in the generated one wholesale.
	interface ChatMessage extends Omit<ConversationMessage, 'role'> {
		role: 'user' | 'assistant';
	}

	function formatTimestamp(ms: number): string {
		return new Date(ms).toLocaleString([], {
			month: 'short',
			day: 'numeric',
			hour: '2-digit',
			minute: '2-digit'
		});
	}

	// Pretty-print a tool step's arguments (a JSON string) for display, falling
	// back to the raw string if it's incomplete/unparseable (mid-stream).
	function formatStepArgs(args: string): string {
		try {
			return JSON.stringify(JSON.parse(args), null, 2);
		} catch {
			return args;
		}
	}

	// A tool call's arguments as labelled fields (`campaign_id` → "Campaign id"),
	// or null while they stream or when they aren't an object.
	function stepFields(args: string): Array<{ label: string; value: string }> | null {
		let parsed: unknown;
		try {
			parsed = JSON.parse(args);
		} catch {
			return null;
		}
		if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
		return Object.entries(parsed).map(([key, value]) => {
			const words = key.replace(/[_-]+/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase();
			return {
				label: words.charAt(0).toUpperCase() + words.slice(1),
				value: typeof value === 'string' ? value : JSON.stringify(value)
			};
		});
	}

	// A gateway model entry: the id the gateway routes on, plus optional
	// display metadata the proxy's flow labels provide (agent flows carry a
	// friendly name/description; raw proxies don't).
	interface ModelOption {
		id: string;
		display_name?: string;
		description?: string;
	}

	let models = $state<ModelOption[]>([]);
	let model = $state<string>('');
	let modelsError = $state<string | null>(null);
	let modelsEmpty = $state(false);
	let modelsLoading = $state(true);

	// A proxy is one gateway/agent flow. `display_name`/`description` are
	// proxy-level labels the gateway copies onto every model entry, so they
	// name the group, not the model — a proxy with two models must not render
	// two identical rows. Group models by proxy prefix (`<proxy>/<model>`, or a
	// bare `<proxy>` for a flow that declares no model) so each proxy shows its
	// name + description once, with its models listed beneath.
	interface ProxyGroup {
		proxy: string;
		display_name: string;
		description?: string;
		// The `<model>` tail of each id, or null for a bare-proxy entry the user
		// selects directly. `id` is the full value the gateway routes on.
		models: Array<{ id: string; name: string | null }>;
	}
	let proxyGroups = $derived.by(() => {
		const groups = new Map<string, ProxyGroup>();
		for (const entry of models) {
			const [proxy, ...rest] = entry.id.split('/');
			const name = rest.length > 0 ? rest.join('/') : null;
			let group = groups.get(proxy);
			if (!group) {
				group = {
					proxy,
					display_name: entry.display_name || proxy,
					description: entry.description,
					models: [],
				};
				groups.set(proxy, group);
			}
			group.models.push({ id: entry.id, name });
		}
		return [...groups.values()];
	});

	// A saved model the agent no longer offers falls back to the same agent's
	// first model, or the first one listed, so the conversation can go on.
	$effect(() => {
		if (models.length === 0 || models.some((entry) => entry.id === model)) return;
		const proxy = model.split('/')[0];
		model = (models.find((entry) => entry.id.split('/')[0] === proxy) ?? models[0]).id;
	});

	function modelLabel(id: string): string {
		const opt = models.find((entry) => entry.id === id);
		const proxyKey = id.split('/')[0];
		const name = id.includes('/') ? id.split('/').slice(1).join('/') : null;
		const proxy = opt?.display_name || proxyKey;
		// Append the model only when the proxy exposes more than one, so a
		// single-model agent reads as just its name, not "Agent · model".
		const multiModel = models.filter((entry) => entry.id.split('/')[0] === proxyKey).length > 1;
		return name && multiModel ? `${proxy} · ${name}` : proxy;
	}

	let messages = $state<ChatMessage[]>([]);
	let input = $state('');
	// A reply runs while the last message is pending, whichever pod runs it.
	let sending = $derived(messages.at(-1)?.role === 'assistant' && messages.at(-1)?.pending === true);
	let error = $state<string | null>(null);
	let scroller = $state<HTMLDivElement | null>(null);

	// Conversation history, served from flowgen's system cache. Summaries only
	// (no message bodies); the active one is fetched in full on demand. The URL
	// is the source of truth for which conversation is open: `/agents/{id}`, or
	// `/agents` for a fresh unsaved chat.
	let conversations = $derived(agentConversations.list);
	let activeId = $derived(page.params.id ?? null);
	let historyPaneOpen = $state(true);
	const paneWidth = new PaneWidth('agents');
	// Id we've already loaded messages for, so the load effect doesn't refetch
	// on unrelated state changes or clobber a chat we just minted an id for.
	let loadedId = $state<string | null>(null);
	// True while fetching a conversation's messages by id, so the chat area
	// shows a spinner instead of an empty/flashing view on a slow load.
	let convoLoading = $state(false);
	// The conversation whose messages are on screen, so a reload of it keeps the draft.
	let shownId: string | null = null;

	// Title shown in the breadcrumb: the active conversation's, or a
	// placeholder for a fresh chat.
	let activeTitle = $derived(
		activeId === null
			? m.agents_new_conversation()
			: (conversations.find((c) => c.id === activeId)?.title ?? m.agents_conversation_fallback_title())
	);

	// The folder the history list shows, with its subfolders; null shows all.
	let selectedFolder = $state<string | null>(null);
	let expandedFolders = $state(new Set<string>());
	let folderTree = $derived(
		buildTree<ConversationSummary>(
			conversations.filter((c) => c.folder),
			(c) => `${c.folder}/${c.id}`
		)
	);
	let folders = $derived([...new Set(conversations.flatMap((c) => (c.folder ? [c.folder] : [])))].sort());

	function inFolder(c: ConversationSummary, folder: string | null): boolean {
		if (folder === null) return true;
		return c.folder === folder || (c.folder ?? '').startsWith(folder + '/');
	}

	function toggleFolder(path: string) {
		const next = new Set(expandedFolders);
		if (next.has(path)) next.delete(path);
		else next.add(path);
		expandedFolders = next;
	}

	let filteredConversations = $derived(conversations.filter((c) => inFolder(c, selectedFolder)));

	// `updatedAt` of the conversation on screen, as last loaded.
	let loadedUpdatedAt = $state(0);

	// A reply this page doesn't follow (another visit's, another pod's) or a
	// flow can change the open conversation; reload it when the server has a
	// newer copy.
	$effect(() => {
		const entry = conversations.find((c) => c.id === activeId);
		if (following || convoLoading || !entry || loadedId !== activeId) return;
		if (entry.updatedAt <= loadedUpdatedAt) return;
		loadedId = null;
	});

	// Whenever the URL id changes, load that conversation's messages (or clear
	// to a fresh chat for `/agents`). Skips a reload of the id we already show.
	$effect(() => {
		const id = activeId;
		if (id === loadedId) return;
		// A new conversation moves to `/agents/{id}` once its reply starts; the
		// messages on screen are already the ones that reply writes.
		if (id !== null && id === untrack(() => followingId)) {
			loadedId = id;
			return;
		}
		untrack(() => stopFollowing());
		if (id === null) {
			messages = [];
			error = null;
			loadedId = null;
			shownId = null;
			convoLoading = false;
			resetComposer();
			return;
		}
		loadedId = id;
		loadedUpdatedAt = untrack(() => conversations.find((c) => c.id === id)?.updatedAt ?? 0);
		convoLoading = true;
		getConversation(id)
			.then((c) => {
				// Guard against a race: the URL may have changed again mid-fetch.
				if (activeId !== c.id) return;
				messages = c.messages.map((msg) => ({ ...msg }) as ChatMessage);
				loadedUpdatedAt = c.updatedAt;
				if (c.model) model = c.model;
				if (shownId !== c.id) {
					shownId = c.id;
					error = null;
					resetComposer();
					scrollToBottom();
				}
				if (c.unread) markRead(c.id);
				if (c.messages.at(-1)?.pending) followReply(c.id);
			})
			.catch((err) => {
				if (activeId === id) error = errorMessage(err, m.agents_conversation_load_failed());
			})
			.finally(() => {
				if (activeId === id) convoLoading = false;
			});
	});

	async function markRead(id: string) {
		try {
			await setConversationRead(id, true);
		} catch (err) {
			console.error('Failed to mark the conversation read', err);
		}
	}

	// Leaves the conversation if it's open, so it isn't marked read again at once.
	async function markUnread(id: string) {
		try {
			await setConversationRead(id, false);
			if (activeId === id) newConversation();
		} catch (err) {
			error = errorMessage(err, m.agents_conversation_mark_unread_failed());
		}
	}

	function resetComposer() {
		input = '';
		recalled = null;
		if (textareaEl) textareaEl.style.height = 'auto';
		multiline = false;
	}

	// On mobile the pane covers the chat, so it closes once a conversation is picked.
	function closePaneOnMobile() {
		if (isMobile()) historyPaneOpen = false;
	}

	function openConversation(id: string) {
		closePaneOnMobile();
		goto(`${base}/agents/${id}`);
	}

	function newConversation() {
		closePaneOnMobile();
		goto(`${base}/agents`);
	}

	// The conversation whose title is being edited in the history list.
	let renaming = $state<string | null>(null);
	let renameTitle = $state('');
	// Renaming from the conversation's header edits the breadcrumb title.
	let renameInHeader = $state(false);

	function startRename(c: ConversationSummary, inHeader = false) {
		renameTitle = c.title;
		renameInHeader = inHeader;
		renaming = c.id;
	}

	let activeSummary = $derived(conversations.find((c) => c.id === activeId));

	async function saveRename(id: string) {
		if (renaming !== id) return;
		renaming = null;
		const title = renameTitle.trim();
		if (!title || title === conversations.find((c) => c.id === id)?.title) return;
		try {
			await updateConversation(id, { title });
		} catch (err) {
			error = errorMessage(err, m.agents_conversation_rename_failed());
		}
	}

	// The conversation being filed in a folder, and the folder typed for it.
	let moving = $state<ConversationSummary | null>(null);
	let moveFolder = $state('');

	function startMove(c: ConversationSummary) {
		moveFolder = c.folder ?? selectedFolder ?? '';
		moving = c;
	}

	async function saveMove() {
		const c = moving;
		moving = null;
		if (!c) return;
		const folder = moveFolder
			.split('/')
			.map((segment) => segment.trim())
			.filter(Boolean)
			.join('/');
		try {
			await updateConversation(c.id, { folder });
		} catch (err) {
			error = errorMessage(err, m.agents_conversation_move_failed());
			return;
		}
		selectedFolder = folder || null;
		if (folder) {
			const parents = folder.split('/').map((_, i, all) => all.slice(0, i + 1).join('/'));
			expandedFolders = new Set([...expandedFolders, ...parents]);
		}
	}

	function unreadIn(folder: string | null): number {
		return conversations.filter((c) => c.unread && inFolder(c, folder)).length;
	}

	// The history row whose ⋮ menu is open, placed under its button. Fixed, so
	// the scrolling list doesn't clip it.
	let rowMenu = $state<{ c: ConversationSummary; top: number; right: number } | null>(null);
	let rowMenuTrigger: HTMLElement | null = null;

	function openRowMenu(event: MouseEvent, c: ConversationSummary) {
		event.stopPropagation();
		if (rowMenu?.c.id === c.id) {
			rowMenu = null;
			return;
		}
		rowMenuTrigger = event.currentTarget as HTMLElement;
		const rect = rowMenuTrigger.getBoundingClientRect();
		const menuHeight = 168;
		const top =
			rect.bottom + 4 + menuHeight > window.innerHeight ? rect.top - 4 - menuHeight : rect.bottom + 4;
		rowMenu = { c, top, right: window.innerWidth - rect.right };
	}

	function closeRowMenu() {
		rowMenu = null;
		rowMenuTrigger?.focus();
	}

	function rowAction(action: (c: ConversationSummary) => void) {
		const c = rowMenu?.c;
		closeRowMenu();
		if (c) action(c);
	}

	function focusFirstItem(node: HTMLElement) {
		if (rowMenu) node.querySelector<HTMLElement>('[role="menuitem"]')?.focus();
	}

	function onRowMenuKeydown(event: KeyboardEvent) {
		const items = [...(event.currentTarget as HTMLElement).querySelectorAll<HTMLElement>('[role="menuitem"]')];
		const at = items.indexOf(document.activeElement as HTMLElement);
		switch (event.key) {
			case 'ArrowDown':
				event.preventDefault();
				items[(at + 1) % items.length]?.focus();
				break;
			case 'ArrowUp':
				event.preventDefault();
				items[(at - 1 + items.length) % items.length]?.focus();
				break;
			case 'Escape':
			case 'Tab':
				event.preventDefault();
				event.stopPropagation();
				closeRowMenu();
				break;
		}
	}

	function focusSelect(node: HTMLInputElement) {
		node.focus();
		node.select();
	}

	// The conversation waiting for the user to confirm its deletion.
	let pendingDelete = $state<ConversationSummary | null>(null);
	let cancelDeleteButton = $state<HTMLButtonElement | null>(null);

	// The safe answer has focus, so Enter keeps the conversation.
	$effect(() => {
		cancelDeleteButton?.focus();
	});

	// Deletes a stored conversation. If it's the one on screen, navigate back to
	// a fresh chat so the view doesn't keep re-saving a deleted id.
	async function removeConversation(id: string) {
		pendingDelete = null;
		try {
			await deleteConversation(id);
			if (activeId === id) newConversation();
		} catch (err) {
			error = errorMessage(err, m.agents_conversation_delete_failed());
		}
	}

	function toggleHistoryPane() {
		historyPaneOpen = !historyPaneOpen;
		try {
			localStorage.setItem('flowgen-agents-history-pane', historyPaneOpen ? '1' : '0');
		} catch {
			// Not remembered, but the pane stays as set for this visit.
		}
	}

	// MCP tools flows register, by name: the flow a tool step links to and the
	// connectors shown as its icons.
	let tools = $state<Record<string, AgentTool>>({});

	async function loadTools() {
		try {
			const res = await fetch(apiUrl('api/agents/tools'));
			if (!res.ok) throw await failure(res);
			const found: AgentTool[] = await res.json();
			tools = Object.fromEntries(found.map((tool) => [tool.name, tool]));
		} catch (err) {
			console.error('Failed to load agent tools', err);
		}
	}

	// The flow runs the tool calls started; the last call of a reply still
	// streaming is followed live.
	$effect(() => {
		const runs: Array<{ flow: string; run: string }> = [];
		let live: string | null = null;
		for (const msg of messages) {
			const steps = msg.steps ?? [];
			steps.forEach((step, si) => {
				const flow = tools[step.name]?.flow;
				if (!flow || !step.run || step.tasks?.length) return;
				runs.push({ flow, run: step.run });
				if (msg.pending === true && si === steps.length - 1) live = flow;
			});
		}
		untrack(() => watchToolRuns(runs, live));
	});

	onDestroy(releaseToolRuns);

	// Ticks the running call's timer.
	let now = $state(Date.now());
	let liveStep = $derived(messages.some((msg) => msg.pending === true && (msg.steps?.length ?? 0) > 0));
	$effect(() => {
		if (!liveStep) return;
		now = Date.now();
		const timer = setInterval(() => (now = Date.now()), 1000);
		return () => clearInterval(timer);
	});

	// Runs whose quick steps are shown too.
	const allTasks = new SvelteSet<string>();

	// Runs of more than 5 tasks fold the quick ones (under 20ms, no warning)
	// into one line.
	function visibleTasks(run: ToolRun, all: boolean): { shown: RunTask[]; hidden: number } {
		if (all || run.tasks.length <= 5) return { shown: run.tasks, hidden: 0 };
		const shown = run.tasks.filter((t) => t.level !== 'info' || t.duration_ms >= 20);
		return { shown, hidden: run.tasks.length - shown.length };
	}

	function levelText(level: ActivityLevel): string {
		switch (level) {
			case 'error':
				return 'text-error';
			case 'warn':
				return 'text-attention';
			default:
				return 'text-base-content/80';
		}
	}

	function levelMark(level: ActivityLevel): string {
		switch (level) {
			case 'error':
				return 'text-error';
			case 'warn':
				return 'text-attention';
			default:
				return 'text-primary';
		}
	}

	function levelIcon(level: ActivityLevel): string {
		switch (level) {
			case 'error':
				return 'tabler:x';
			case 'warn':
				return 'tabler:exclamation-mark';
			default:
				return 'tabler:check';
		}
	}

	onMount(async () => {
		try {
			const pane = localStorage.getItem('flowgen-agents-history-pane');
			if (pane !== null) historyPaneOpen = pane === '1';
		} catch {
			// Storage can be unavailable; the pane starts open.
		}
		if (isMobile()) historyPaneOpen = false;
		loadTools();

		try {
			const res = await fetch(apiUrl('api/agents/models'));
			if (!res.ok) throw await failure(res);
			const body = await res.json();
			models = (body.data ?? [])
				// The built-in chat speaks OpenAI (`/chat/completions`); skip proxies
				// that speak another protocol so they can't be picked and then fail.
				.filter((entry: { protocol?: string }) => (entry.protocol ?? 'openai') === 'openai')
				.map((entry: { id: string; display_name?: string; description?: string }) => ({
					id: entry.id,
					display_name: entry.display_name,
					description: entry.description
				}));
			// The conversation-load effect may have already restored a saved
			// model by the time this fetch resolves — don't clobber it.
			if (models.length > 0) {
				if (!model) model = models[0].id;
			} else {
				modelsEmpty = true;
			}
		} catch (err) {
			modelsError = errorMessage(err, m.agents_models_load_failed());
		} finally {
			modelsLoading = false;
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
		const opened = activeId;
		const id = opened ?? crypto.randomUUID();
		const at = messages.length;
		messages.push({ role: 'user', content: text, at: Date.now() });
		messages.push({ role: 'assistant', content: '', at: Date.now(), pending: true });
		resetComposer();
		await scrollToBottom();
		try {
			await startReply(id, text, model);
		} catch (err) {
			if (activeId !== opened) return;
			error = errorMessage(err, m.agents_chat_failed());
			if (messages[at]?.content === text && messages[at + 1]?.pending) messages.splice(at, 2);
			input = text;
			await tick();
			if (textareaEl) autoGrow(textareaEl);
			return;
		}
		if (activeId !== opened) return;
		if (opened === null) {
			shownId = id;
			goto(`${base}/agents/${id}`);
		}
		followReply(id);
	}

	// The server writes the reply; this follows it while it runs on this pod.
	let followingId: string | null = null;
	let following = $state.raw<EventSource | null>(null);

	function stopFollowing() {
		following?.close();
		following = null;
		followingId = null;
	}

	function followReply(id: string) {
		following?.close();
		followingId = id;
		const source = new EventSource(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}/reply`));
		following = source;
		source.addEventListener('message', (e) => {
			if (following !== source) return;
			const reply: ChatMessage = JSON.parse(e.data);
			const last = messages.length - 1;
			if (last >= 0 && messages[last].role === 'assistant') messages[last] = reply;
			else messages.push(reply);
			scrollToBottom();
			if (reply.pending !== true) stopFollowing();
		});
		// The reply runs on another pod, or the connection broke: the pending
		// reply still updates through the conversation stream.
		source.onerror = () => {
			if (following === source) stopFollowing();
		};
	}

	onDestroy(() => following?.close());

	// Stops the reply on the server (Esc or the Stop button), whichever pod
	// runs it; with none running, the stored conversation is reloaded.
	function stopGenerating() {
		const id = followingId ?? activeId;
		if (!id) return;
		stopReply(id)
			.then((stopped) => {
				if (!stopped && !following && activeId === id) loadedId = null;
			})
			.catch((err) => (error = errorMessage(err, m.agents_chat_failed())));
	}

	// Which of the conversation's prompts the composer shows, counted from the
	// first; null while it holds the user's own draft.
	let recalled = $state<number | null>(null);
	let draft = '';

	// Up on the composer's first line and Down on its last walk through the
	// prompts sent in this conversation, like a shell history. With a modifier
	// or while an input method composes, the keys keep their usual meaning.
	function recallPrompt(event: KeyboardEvent): boolean {
		if (event.shiftKey || event.altKey || event.metaKey || event.ctrlKey || event.isComposing) {
			return false;
		}
		const el = event.currentTarget as HTMLTextAreaElement;
		const prompts = messages.filter((msg) => msg.role === 'user').map((msg) => msg.content);
		const from = recalled ?? prompts.length;
		let next: number | null;
		if (event.key === 'ArrowUp' && from > 0 && !el.value.slice(0, el.selectionStart).includes('\n')) {
			next = from - 1;
		} else if (
			event.key === 'ArrowDown' &&
			recalled !== null &&
			!el.value.slice(el.selectionEnd).includes('\n')
		) {
			next = recalled + 1 < prompts.length ? recalled + 1 : null;
		} else {
			return false;
		}
		if (recalled === null) draft = input;
		recalled = next;
		input = next === null ? draft : prompts[next];
		tick().then(() => {
			autoGrow(el);
			el.setSelectionRange(input.length, input.length);
		});
		return true;
	}

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			send();
		} else if (recallPrompt(event)) {
			event.preventDefault();
		} else if (event.key === 'Escape' && !sending && input) {
			// Esc clears a drafted message; while generating, the window-level
			// handler uses Esc to stop instead.
			event.preventDefault();
			resetComposer();
		}
	}

	// Esc stops an in-flight generation, from anywhere on the page.
	function onWindowKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && rowMenu) {
			rowMenu = null;
		} else if (event.key === 'Escape' && sending) {
			event.preventDefault();
			stopGenerating();
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
		recalled = null;
		autoGrow(e.currentTarget as HTMLTextAreaElement);
	}
</script>

<svelte:head>
	<title>{pageTitle(activeId === null ? null : activeTitle, m.nav_agents())}</title>
</svelte:head>

<svelte:window
	onkeydown={onWindowKeydown}
	onclick={() => (rowMenu = null)}
	onresize={() => (rowMenu = null)}
/>

{#snippet unreadCount(count: number)}
	{#if count > 0}
		<span class="ml-auto text-xs font-semibold text-base-content/70">
			<span aria-hidden="true">{count}</span>
			<span class="sr-only">{m.agents_conversations_unread_count({ count })}</span>
		</span>
	{/if}
{/snippet}

<section class="relative flex h-[calc(100vh-4rem)] min-w-0 overflow-hidden">
	{#if historyPaneOpen}
		<div class="absolute inset-0 z-20 bg-black/30 md:hidden" aria-hidden="true" onclick={closePaneOnMobile}></div>
	{/if}
	<!-- History pane, mirroring the folders pane in Resources 1:1. -->
	<aside
		class="relative flex shrink-0 flex-col border-r border-base-300 bg-base-100 {paneWidth.dragging
			? ''
			: 'transition-[width] duration-200 ease-out'} {historyPaneOpen ? PANE_OVER_PAGE : 'w-16'}"
		style:width={historyPaneOpen ? `${paneWidth.width}px` : undefined}
	>
		{#if historyPaneOpen}
			<PaneResizer pane={paneWidth} />
		{/if}
		{#if !historyPaneOpen}
			<div class="flex flex-1 flex-col items-center gap-1 py-2">
				<div class="tooltip tooltip-right" data-tip={m.common_new()}>
					<button
						type="button"
						aria-label={m.agents_new_conversation()}
						class="flex h-10 w-10 items-center justify-center rounded-md bg-primary text-primary-content transition-colors hover:bg-primary/90 disabled:opacity-50"
						onclick={newConversation}
						disabled={sending}
					>
						<Icon icon="tabler:plus" class="h-5 w-5 shrink-0" />
					</button>
				</div>
				<div class="tooltip tooltip-right" data-tip={m.agents_conversations()}>
					<button
						type="button"
						aria-label={m.agents_history_expand()}
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
					{m.common_new()}
				</button>
				<button
					type="button"
					class="relative mt-2 flex w-full items-center gap-1.5 h-10 shrink-0 rounded-md px-2 text-left text-sm transition-colors {selectedFolder ===
					null
						? 'bg-base-200 font-medium text-primary'
						: 'hover:bg-base-200'}"
					onclick={() => (selectedFolder = null)}
				>
					{#if selectedFolder === null}
						<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
					{/if}
					<Icon icon="tabler:message-2" class="h-5 w-5 shrink-0 opacity-70" />
					<span>{m.agents_all_conversations()}</span>
				</button>
				{#if folderTree.length > 0}
					<ul class="mt-0.5 shrink-0 space-y-0.5 text-sm">
						{#snippet folderNodes(nodes: TreeNode<ConversationSummary>[])}
							{#each nodes.filter((node) => node.isFolder) as node (node.fullPath)}
								{@const isOpen = expandedFolders.has(node.fullPath)}
								{@const isSelected = selectedFolder === node.fullPath}
								{@const canExpand = (node.children ?? []).some((child) => child.isFolder)}
								<li>
									<div class="flex items-center gap-0.5">
										{#if canExpand}
											<button
												type="button"
												class="flex h-6 w-6 shrink-0 items-center justify-center rounded hover:bg-base-200"
												aria-label={isOpen ? m.resources_collapse_folder() : m.resources_expand_folder()}
												onclick={() => toggleFolder(node.fullPath)}
											>
												<Icon
													icon={isOpen ? 'tabler:chevron-down' : 'tabler:chevron-right'}
													class="h-3.5 w-3.5 opacity-70"
												/>
											</button>
										{:else}
											<span class="h-6 w-6 shrink-0"></span>
										{/if}
										<button
											type="button"
											class="relative flex h-9 flex-1 items-center gap-1.5 rounded-md px-2 text-left transition-colors {isSelected
												? 'bg-base-200 font-medium text-primary'
												: 'hover:bg-base-200'}"
											style="padding-left: {node.depth * 0.75 + 0.375}rem"
											onclick={() => (selectedFolder = node.fullPath)}
										>
											{#if isSelected}
												<span class="absolute -left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-r bg-primary"></span>
											{/if}
											<Icon
												icon={isOpen && canExpand ? 'tabler:folder-open' : 'tabler:folder'}
												class="h-5 w-5 shrink-0 opacity-70"
											/>
											<span class="truncate">{node.name}</span>
											{@render unreadCount(unreadIn(node.fullPath))}
										</button>
									</div>
									{#if isOpen && canExpand && node.children}
										<ul class="space-y-0.5">
											{@render folderNodes(node.children)}
										</ul>
									{/if}
								</li>
							{/each}
						{/snippet}
						{@render folderNodes(folderTree)}
					</ul>
					<div class="my-1 shrink-0 border-t border-base-200"></div>
				{/if}
				<ul
					class="mt-1 min-h-0 flex-1 space-y-0.5 overflow-y-auto text-sm"
					onscroll={() => (rowMenu = null)}
				>
					{#each filteredConversations as c (c.id)}
						<li class="group relative">
							{#if renaming === c.id && !renameInHeader}
								<input
									type="text"
									class="input input-sm w-full border border-primary bg-base-100 outline-none"
									aria-label={m.agents_conversation_rename()}
									bind:value={renameTitle}
									use:focusSelect
									onkeydown={(e) => {
										if (e.key === 'Enter') saveRename(c.id);
										else if (e.key === 'Escape') {
											e.stopPropagation();
											renaming = null;
										}
									}}
									onblur={() => saveRename(c.id)}
								/>
							{:else}
							<button
								type="button"
								class="relative flex w-full flex-col gap-0.5 rounded-md py-2 pl-2 pr-10 text-left transition-colors {activeId ===
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
								<span class="flex w-full items-center gap-1.5">
									{#if c.unread}
										<span
											class="h-2 w-2 shrink-0 rounded-full bg-primary"
											role="img"
											aria-label={m.agents_conversation_unread()}
										></span>
									{/if}
									<span class="block min-w-0 flex-1 truncate {c.unread ? 'font-semibold' : ''}"
										>{c.title}</span
									>
								</span>
								<span class="text-xs opacity-50">{formatRelative(c.updatedAt)}</span>
							</button>
							<div
								class="absolute right-1 top-1/2 -translate-y-1/2 transition-opacity {rowMenu?.c.id === c.id
									? 'opacity-100'
									: 'opacity-0 group-focus-within:opacity-100 group-hover:opacity-100'}"
							>
								<div class="tooltip tooltip-left" data-tip={m.agents_conversation_actions()}>
									<button
										type="button"
										class="btn btn-sm btn-ghost btn-circle"
										aria-label={m.agents_conversation_actions()}
										aria-haspopup="menu"
										aria-expanded={rowMenu?.c.id === c.id}
										onclick={(e) => openRowMenu(e, c)}
									>
										<Icon icon="tabler:dots-vertical" class="h-5 w-5" />
									</button>
								</div>
							</div>
							{/if}
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
				data-tip={historyPaneOpen ? m.agents_history_collapse() : m.agents_history_expand()}
			>
				<button
					type="button"
					aria-label={historyPaneOpen ? m.agents_history_collapse() : m.agents_history_expand()}
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

	<div class="relative flex min-w-0 flex-1 flex-col overflow-hidden">
	<div
		class="flex shrink-0 items-center gap-2 border-b border-base-200 bg-base-100 px-6 pb-4 pt-6 max-md:flex-wrap max-md:px-4 max-md:pt-4"
	>
		<div class="min-w-0 flex-1 max-md:basis-full">
			<div class="flex min-h-8 items-center gap-1.5 text-sm">
				<a href="{base}/agents" class="shrink-0 text-primary hover:underline">{m.agents_breadcrumb()}</a>
				{#each (activeSummary?.folder ?? '').split('/').filter(Boolean) as segment, i (i)}
					{@const path = activeSummary?.folder?.split('/').slice(0, i + 1).join('/') ?? null}
					<span class="shrink-0 opacity-40">/</span>
					<button
						type="button"
						class="shrink-0 font-mono text-primary hover:underline"
						onclick={() => (selectedFolder = path)}>{segment}</button
					>
				{/each}
			</div>
			{#if activeSummary && renaming === activeSummary.id && renameInHeader}
				{@const id = activeSummary.id}
				<input
					type="text"
					class="input input-sm mt-1 w-full max-w-xl border border-primary bg-base-100 text-lg font-medium outline-none"
					aria-label={m.agents_conversation_rename()}
					bind:value={renameTitle}
					use:focusSelect
					onkeydown={(e) => {
						if (e.key === 'Enter') saveRename(id);
						else if (e.key === 'Escape') {
							e.stopPropagation();
							renaming = null;
						}
					}}
					onblur={() => saveRename(id)}
				/>
			{:else}
				<h1 class="mt-1 truncate text-lg font-medium">{activeTitle}</h1>
			{/if}
		</div>

		<div class="flex shrink-0 items-center gap-2">
			{#if activeSummary}
				{@const c = activeSummary}
				<div class="flex shrink-0 items-center">
					{@render headerAction('tabler:folder', m.agents_conversation_move(), () => startMove(c))}
					{@render headerAction('tabler:pencil', m.agents_conversation_rename(), () =>
						startRename(c, true)
					)}
					{@render headerAction('tabler:mail', m.agents_conversation_mark_unread(), () =>
						markUnread(c.id)
					)}
					{@render headerAction('tabler:trash', m.common_delete(), () => (pendingDelete = c), true)}
				</div>
			{/if}

			{@render agentPicker()}
		</div>
	</div>

	{#snippet headerAction(icon: string, label: string, onclick: () => void, danger = false)}
		<div class="tooltip tooltip-bottom" data-tip={label}>
			<button
				type="button"
				class="btn btn-ghost btn-circle btn-sm {danger ? 'text-error' : ''}"
				aria-label={label}
				{onclick}
			>
				<Icon {icon} class="h-5 w-5" />
			</button>
		</div>
	{/snippet}

	{#snippet agentPicker()}
		<div class="dropdown dropdown-end shrink-0">
			<button
				type="button"
				tabindex="0"
				class="btn btn-sm border border-base-300 bg-base-100 font-normal hover:bg-base-200 focus-visible:outline-primary"
				disabled={models.length === 0}
			>
				<Icon icon="tabler:brain" class="h-4 w-4 opacity-70" />
				<span>{model ? modelLabel(model) : m.agents_model_picker()}</span>
				<Icon icon="tabler:chevron-down" class="h-3.5 w-3.5 opacity-60" />
			</button>
			<div
				tabindex="-1"
				class="dropdown-content z-10 mt-1 max-h-80 w-80 overflow-auto rounded-md border border-base-200 bg-base-100 p-1 shadow-lg"
			>
				{#each proxyGroups as group (group.proxy)}
					{@const single = group.models.length === 1}
					{#if single}
						<!-- One model: the header is itself the selectable row; the model
						     name (if any) rides along as a subtle suffix. -->
						{@const only = group.models[0]}
						<button
							type="button"
							class="flex w-full items-start gap-2 rounded px-2 py-1.5 text-left text-sm hover:bg-base-200"
							onclick={(e) => {
								model = only.id;
								e.currentTarget.blur();
							}}
						>
							<span class="mt-0.5 inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center text-primary">
								{#if model === only.id}
									<Icon icon="tabler:check" class="h-3.5 w-3.5" />
								{/if}
							</span>
							<span class="min-w-0 flex-1">
								<span class="block truncate font-medium">{group.display_name}</span>
								{#if group.description}
									<span class="line-clamp-2 block text-xs opacity-60">{group.description}</span>
								{/if}
								{#if only.name}
									<span class="mt-0.5 block truncate font-mono text-xs opacity-40">{only.name}</span>
								{/if}
							</span>
						</button>
					{:else}
						<!-- Proxy with named models: header names the proxy once, models
						     below are the selectable rows. -->
						<div class="min-w-0 px-2 pb-0.5 pt-1.5">
							<div class="truncate text-sm font-medium">{group.display_name}</div>
							{#if group.description}
								<span class="line-clamp-2 block text-xs opacity-50">{group.description}</span>
							{/if}
						</div>
						{#each group.models as opt (opt.id)}
							<button
								type="button"
								class="flex w-full items-center gap-2 rounded py-1 pl-4 pr-2 text-left text-sm hover:bg-base-200"
								onclick={(e) => {
									model = opt.id;
									e.currentTarget.blur();
								}}
							>
								<span class="inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center text-primary">
									{#if model === opt.id}
										<Icon icon="tabler:check" class="h-3.5 w-3.5" />
									{/if}
								</span>
								<span class="min-w-0 flex-1 truncate font-mono text-xs">{opt.name}</span>
							</button>
						{/each}
					{/if}
				{/each}
			</div>
		</div>
	{/snippet}

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
				placeholder={m.agents_composer_placeholder()}
				bind:value={input}
				onkeydown={onKeydown}
				oninput={onInput}
				disabled={sending || !model}
			></textarea>
			{#if sending}
				<!-- While a turn streams, the send button becomes Stop (also Esc). -->
				<div class="tooltip tooltip-top shrink-0" data-tip={m.agents_stop_tooltip()}>
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle"
						onclick={stopGenerating}
						aria-label={m.agents_stop()}
					>
						<Icon icon="tabler:player-stop-filled" class="h-5 w-5" />
					</button>
				</div>
			{:else}
				<div class="tooltip tooltip-top shrink-0" data-tip={m.agents_send()}>
					<button
						type="button"
						class="btn btn-ghost btn-sm btn-circle text-primary"
						onclick={send}
						disabled={!model || input.trim().length === 0}
						aria-label={m.agents_send()}
					>
						<Icon icon="tabler:arrow-up" class="h-5 w-5" />
					</button>
				</div>
			{/if}
		</div>
	{/snippet}

	{#if modelsLoading}
		<!-- Hold the empty state until we know whether models exist, so the chat
		     greeting doesn't flash before switching to "no models". -->
		<div class="flex flex-1 items-center justify-center">
			<span class="loading loading-spinner loading-lg text-primary"></span>
		</div>
	{:else if modelsError}
		<div class="flex flex-1 items-center justify-center">
			<StateMessage
				tone="oops"
				title={m.agents_models_error_title()}
				message={modelsError}
			/>
		</div>
	{:else if modelsEmpty}
		<div class="flex flex-1 items-center justify-center">
			<StateMessage
				tone="notice"
				title={m.agents_models_empty_title()}
				message={m.agents_models_empty_message()}
			/>
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
				{m.agents_greeting()}
			</p>
			<div class="relative w-full max-w-2xl">
				{@render composer()}
			</div>
		</div>
	{:else}
		<!-- Conversation: scrolling messages, composer pinned to the bottom. -->
		<div
			bind:this={scroller}
			class="min-h-0 flex-1 overflow-y-auto px-6 pb-10 pt-6 [mask-image:linear-gradient(to_bottom,transparent,black_1.5rem,black_calc(100%-2.5rem),transparent)]"
		>
			<div class="mx-auto flex max-w-4xl flex-col gap-5">
				{#each messages as msg, i (i)}
					{@const streaming = msg.role === 'assistant' && msg.pending === true}
					{@const steps = msg.steps ?? []}
					{@const hasBody = msg.content.length > 0 || steps.length > 0}
					{@const bareStatus = msg.role === 'assistant' && streaming && !hasBody}
					<div class="group flex flex-col gap-1 {msg.role === 'user' ? 'items-end' : 'items-start'}">
						{#if bareStatus}
							<!-- Streaming, nothing arrived yet: the shimmering status line
							     alone stands in for a bubble. -->
							<span
								class="chat-status chat-status-live flex items-center gap-1.5 px-1 py-2 text-sm font-medium"
							>
								<Icon icon="tabler:activity" mode="mask" class="chat-status-pulse h-4 w-4" />
								{m.agents_working()}
							</span>
						{:else if msg.role === 'user'}
							<div
								class="max-w-[85%] whitespace-pre-wrap rounded-2xl bg-primary px-4 py-2.5 text-sm text-primary-content shadow-sm"
							>
								{msg.content}
							</div>
						{:else}
							<div
								class="chat-markdown prose prose-sm max-w-[85%] min-w-0 rounded-2xl border border-base-200 bg-base-100/60 px-4 py-2.5 text-sm text-base-content shadow-sm"
							>
								<!-- eslint-disable-next-line svelte/no-at-html-tags -->
								{@html renderMarkdown(msg.content)}
								{#if msg.stopped}
									<p class="not-prose mt-2 flex items-center gap-1.5 text-xs text-base-content/50">
										<Icon icon="tabler:player-stop" class="h-3.5 w-3.5 shrink-0" />
										<span>{m.agents_reply_stopped()}</span>
									</p>
								{:else if !streaming && msg.content.length === 0 && steps.length === 0 && !msg.error}
									<p class="text-base-content/50">{m.agents_reply_empty()}</p>
								{/if}
								{#if msg.error}
									<p class="not-prose mt-2 flex items-start gap-1.5 text-xs text-error">
										<Icon icon="tabler:alert-triangle" class="mt-0.5 h-3.5 w-3.5 shrink-0" />
										<span>{msg.error}</span>
									</p>
								{/if}
								{#if steps.length > 0}
									<!-- Tool calls as status lines: `✱ <name>`, expandable to
									     its arguments. The last one shimmers while streaming. -->
									<div class="chat-tools mt-3 space-y-0.5 border-t border-base-200 pt-2 text-xs">
										{#each steps as step, si (si)}
											{@const live = streaming && si === steps.length - 1}
											{@const tool = tools[step.name]}
											{@const reached = tool?.connectors ?? []}
											{@const run = savedRun(step) ?? (tool ? toolRun(step.run) : null)}
											<details class="chat-step">
												<summary
													class="chat-status -mx-1 flex cursor-pointer list-none items-center gap-2 rounded px-1 py-0.5 font-mono outline-none focus-visible:outline-2 focus-visible:outline-primary {live
														? ''
														: 'text-base-content/50 hover:text-base-content/80'}"
												>
													<Icon
														icon={tool ? 'tabler:binary-tree' : 'tabler:tool'}
														mode="mask"
														class="h-3 w-3 shrink-0 {live ? 'chat-status-pulse' : 'opacity-60'}"
													/>
													{#if tool}
														<span class="tooltip tooltip-top flex min-w-0" data-tip={m.agents_tool_open_flow()}>
															<a
																href="{base}/flows/{encodePath(tool.flow)}"
																class="[overflow-wrap:anywhere] hover:underline {live ? 'chat-status-live' : ''}"
																>{step.name}{live ? '…' : ''}</a
															>
														</span>
													{:else}
														<span class="min-w-0 [overflow-wrap:anywhere] {live ? 'chat-status-live' : ''}"
															>{step.name || m.agents_tool_step_fallback()}{live ? '…' : ''}</span
														>
													{/if}
													{#if reached.length > 0}
														<span
															class="ml-1 flex shrink-0 items-center gap-1 {run ? 'chat-step-connectors' : ''}"
														>
															{#each reached.slice(0, 3) as name (name)}
																{@const connector = connectorByName(name)}
																<span class="tooltip tooltip-top" data-tip={connector.label}>
																	<img src={connector.iconUrl} alt={connector.label} class="h-3.5 w-3.5" />
																</span>
															{/each}
															{#if reached.length > 3}
																<span class="font-sans text-[10px] text-base-content/50"
																	>{m.agents_tool_more_connectors({ count: reached.length - 3 })}</span
																>
															{/if}
														</span>
													{/if}
													{#if run && live}
														<span
															class="ml-auto flex min-w-0 shrink items-center gap-2 font-sans text-[11px] tabular-nums text-base-content/50"
														>
															<span class="truncate">{run.last}</span>
															<span class="shrink-0">{formatDuration(now - run.started_ms)}</span>
														</span>
													{:else if run}
														{@const level = run.failed ? 'error' : run.warned ? 'warn' : 'info'}
														<span
															class="ml-auto flex shrink-0 items-center gap-1.5 font-sans text-[11px] tabular-nums {run.failed
																? 'text-error'
																: run.warned
																	? 'text-attention'
																	: 'text-base-content/50'}"
														>
															{#if run.failed}
																<span class="max-sm:hidden">{m.agents_tool_failed_at({ task: run.failed })} ·</span>
															{/if}
															<span class="shrink-0">{formatDuration(run.ended_ms - run.started_ms)}</span>
															<span
																class="tooltip tooltip-top inline-flex shrink-0"
																data-tip={run.failed ? m.agents_tool_failed() : m.agents_tool_succeeded()}
															>
																<Icon icon={levelIcon(level)} class="h-3.5 w-3.5 {levelMark(level)}" />
															</span>
														</span>
													{/if}
													{#if step.arguments || reached.length > 3 || run}
														<Icon
															icon="tabler:chevron-right"
															class="chat-step-chevron h-3 w-3 shrink-0 opacity-40"
														/>
													{/if}
												</summary>
												{#if reached.length > 3 && !run}
													<div class="mt-1 ml-5 flex flex-wrap gap-x-3 gap-y-1 font-sans text-[11px] text-base-content/60">
														{#each reached as name (name)}
															{@const connector = connectorByName(name)}
															<span class="flex items-center gap-1">
																<img src={connector.iconUrl} alt="" class="h-3.5 w-3.5" />
																{connector.label}
															</span>
														{/each}
													</div>
												{/if}
												{#if step.arguments}
													{@const fields = stepFields(step.arguments)}
													{#if fields}
														<div class="not-prose mt-1 ml-5 font-sans text-[11px] leading-5">
															<div class="text-[10px] font-medium uppercase tracking-wide text-base-content/40">
																{m.agents_tool_input()}
															</div>
															<dl class="m-0 grid grid-cols-[max-content_1fr] items-baseline gap-x-3">
																{#each fields as field, fi (fi)}
																	<dt class="m-0 font-normal text-base-content/50">{field.label}</dt>
																	<dd class="m-0 break-words text-base-content/80">{field.value}</dd>
																{/each}
															</dl>
														</div>
													{:else}
														<pre
															class="mt-0.5 ml-5 overflow-x-auto whitespace-pre-wrap break-words rounded bg-base-200/50 px-2 py-1 text-[11px] leading-snug text-base-content/60">{formatStepArgs(
																step.arguments
															)}</pre>
													{/if}
												{/if}
												{#if run && step.run}
													{@const runId = step.run}
													{@const { shown, hidden } = visibleTasks(run, allTasks.has(runId))}
													<div class="not-prose mt-1.5 ml-5 font-sans text-[11px] leading-5">
														<div class="text-[10px] font-medium uppercase tracking-wide text-base-content/40">
															{m.agents_tool_steps()}
														</div>
														<dl class="m-0 grid grid-cols-[max-content_max-content_1fr] items-center gap-x-3">
															{#each shown as task (task.task)}
																{@const connector = connectorFor(task.task_type ?? '')}
																<dt class="m-0 flex items-center gap-1.5 font-normal {levelText(task.level)}">
																	<span class="tooltip tooltip-top shrink-0" data-tip={connector.label}>
																		<img src={connector.iconUrl} alt={connector.label} class="h-3.5 w-3.5" />
																	</span>
																	<span
																		>{task.task}{#if task.count > 1}<span class="text-base-content/40">
																				×{task.count}</span
																			>{/if}</span
																	>
																</dt>
																<dd class="m-0 text-right tabular-nums text-base-content/50">
																	{formatDuration(task.duration_ms)}
																</dd>
																<dd class="m-0 flex items-center">
																	<Icon icon={levelIcon(task.level)} class="h-3.5 w-3.5 {levelMark(task.level)}" />
																</dd>
															{/each}
														</dl>
														{#if hidden > 0}
															<button
																type="button"
																class="cursor-pointer text-base-content/40 hover:text-base-content/70"
																onclick={() => allTasks.add(runId)}
															>
																· {m.agents_tool_more_steps({ count: hidden })}
															</button>
														{/if}
													</div>
												{/if}
											</details>
										{/each}
									</div>
								{/if}
								{#if streaming}
									<!-- Text is streaming (no fresh tool call to shimmer): a
									     working status line marks progress instead of a cursor. -->
									<span
										class="chat-status chat-status-live mt-2 flex items-center gap-1.5 text-xs font-medium"
									>
										<Icon icon="tabler:activity" mode="mask" class="chat-status-pulse h-3.5 w-3.5" />
										{m.agents_working()}
									</span>
								{/if}
							</div>
						{/if}
						{#if !streaming && !bareStatus}
							<div
								class="flex items-center gap-1 px-1 opacity-0 transition-opacity group-hover:opacity-100 {msg.role ===
								'user'
									? 'flex-row-reverse'
									: ''}"
							>
								<CopyButton text={msg.content} size="xs" />
								<span class="text-xs tabular-nums text-base-content/40">
									{formatTimestamp(msg.at)}
								</span>
							</div>
						{/if}
					</div>
				{/each}
			</div>
		</div>

		{#if error}
			<div class="mx-auto w-full max-w-4xl px-6">
				<div class="alert alert-error alert-sm mb-2" role="alert">
					<span>{error}</span>
				</div>
			</div>
		{/if}

		<div class="shrink-0 px-6 pb-6">
			<div class="mx-auto max-w-4xl">
				{@render composer()}
			</div>
		</div>
	{/if}

	{#if rowMenu}
		{@const c = rowMenu.c}
		<ul
			class="menu fixed z-50 w-48 rounded-md border border-base-200 bg-base-100 p-1 text-sm shadow-lg"
			style="top: {rowMenu.top}px; right: {rowMenu.right}px"
			role="menu"
			aria-label={m.agents_conversation_actions()}
			tabindex="-1"
			onkeydown={onRowMenuKeydown}
			{@attach focusFirstItem}
		>
			{@render menuItem('tabler:folder', m.agents_conversation_move(), () => rowAction(startMove))}
			{@render menuItem('tabler:pencil', m.agents_conversation_rename(), () => rowAction(startRename))}
			{#if !c.unread}
				{@render menuItem('tabler:mail', m.agents_conversation_mark_unread(), () =>
					rowAction((c) => markUnread(c.id))
				)}
			{/if}
			{@render menuItem('tabler:trash', m.common_delete(), () => rowAction((c) => (pendingDelete = c)), true)}
		</ul>
	{/if}

	{#snippet menuItem(icon: string, label: string, onclick: () => void, danger = false)}
		<li role="none">
			<button type="button" role="menuitem" class={danger ? 'text-error' : ''} {onclick}>
				<Icon {icon} class="h-4 w-4" />
				{label}
			</button>
		</li>
	{/snippet}

	{#if moving}
		<div
			class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
			role="dialog"
			aria-modal="true"
			aria-label={m.agents_conversation_move()}
			tabindex="-1"
			onclick={(e) => {
				if (e.target === e.currentTarget) moving = null;
			}}
			onkeydown={(e) => {
				if (e.key !== 'Escape') return;
				e.stopPropagation();
				moving = null;
			}}
		>
			<form
				class="w-full max-w-sm overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl"
				onsubmit={(e) => {
					e.preventDefault();
					saveMove();
				}}
			>
				<div class="border-b border-base-200 px-4 py-3 text-sm font-medium">
					{m.agents_conversation_move_title()}
				</div>
				<div class="space-y-1 px-4 py-3 text-sm">
					<input
						type="text"
						class="input input-sm w-full border border-base-300 bg-base-100 outline-none focus:border-primary"
						placeholder={m.agents_conversation_move_placeholder()}
						list="agents-folders"
						bind:value={moveFolder}
						use:focusSelect
					/>
					<datalist id="agents-folders">
						{#each folders as folder (folder)}
							<option value={folder}></option>
						{/each}
					</datalist>
					<p class="text-xs opacity-60">{m.agents_conversation_move_hint()}</p>
				</div>
				<div class="flex justify-end gap-1 border-t border-base-200 px-3 py-2">
					<button type="button" class="btn btn-sm border-base-300 bg-base-100" onclick={() => (moving = null)}>
						{m.common_cancel()}
					</button>
					<button type="submit" class="btn btn-primary btn-sm">{m.agents_conversation_move_confirm()}</button>
				</div>
			</form>
		</div>
	{/if}

	{#if pendingDelete}
		{@const target = pendingDelete}
		<div
			class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
			role="dialog"
			aria-modal="true"
			aria-label={m.agents_conversation_delete()}
			tabindex="-1"
			onclick={(e) => {
				if (e.target === e.currentTarget) pendingDelete = null;
			}}
			onkeydown={(e) => {
				if (e.key !== 'Escape') return;
				e.stopPropagation();
				pendingDelete = null;
			}}
		>
			<div class="w-full max-w-sm overflow-hidden rounded-lg border border-base-200 bg-base-100 shadow-xl">
				<div class="border-b border-base-200 px-4 py-3 text-sm font-medium">
					{m.agents_conversation_delete_title()}
				</div>
				<p class="px-4 py-3 text-sm opacity-70">{m.agents_conversation_delete_message()}</p>
				<div class="flex justify-end gap-1 border-t border-base-200 px-3 py-2">
					<button
						type="button"
						class="btn btn-sm border-base-300 bg-base-100"
						bind:this={cancelDeleteButton}
						onclick={() => (pendingDelete = null)}
					>
						{m.common_cancel()}
					</button>
					<button
						type="button"
						class="btn btn-error btn-soft btn-sm"
						onclick={() => removeConversation(target.id)}
					>
						{m.common_delete()}
					</button>
				</div>
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

	/* Live status label: brand-green gradient text, shimmering while a turn is
	   in progress. Reuses the shared `edge-shimmer` keyframes. */
	.chat-status-live {
		background: linear-gradient(120deg, #006b55 0%, #00b4a6 50%, #00e168 100%);
		background-size: 200% 200%;
		-webkit-background-clip: text;
		background-clip: text;
		-webkit-text-fill-color: transparent;
		color: transparent;
		animation: edge-shimmer 2.5s ease-in-out infinite;
	}
	/* The working icon renders in iconify `mode="mask"` (a <span> with a
	   mask), so the same animated gradient shows through the icon shape — it
	   shimmers in lockstep with the text. Also gently pulses. Targeted via
	   :global because it lands on the iconify element. */
	:global(.chat-status-pulse) {
		background: linear-gradient(120deg, #006b55 0%, #00b4a6 50%, #00e168 100%);
		background-size: 200% 200%;
		animation:
			edge-shimmer 2.5s ease-in-out infinite,
			chat-pulse 1.6s ease-in-out infinite;
	}
	@keyframes chat-pulse {
		0%,
		100% {
			opacity: 1;
		}
		50% {
			opacity: 0.45;
		}
	}
	/* Disclosure chevron rotates when its step is expanded. */
	/* Expanded, each flow step shows its own connector. */
	.chat-step[open] .chat-step-connectors {
		display: none;
	}
	.chat-step[open] :global(.chat-step-chevron) {
		transform: rotate(90deg);
	}
	.chat-step :global(.chat-step-chevron) {
		transition: transform 0.15s ease;
	}
	@media (prefers-reduced-motion: reduce) {
		.chat-status-live,
		:global(.chat-status-pulse) {
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
	/* Tables: scroll horizontally inside the bubble rather than overflowing it,
	   with light rules so columns read cleanly. */
	.chat-markdown :global(table) {
		display: block;
		width: 100%;
		max-width: 100%;
		overflow-x: auto;
		border-collapse: collapse;
		font-size: 0.8125rem;
		margin: 0.5rem 0;
	}
	.chat-markdown :global(th),
	.chat-markdown :global(td) {
		border: 1px solid var(--color-base-200);
		padding: 0.3rem 0.55rem;
		text-align: left;
		white-space: nowrap;
		vertical-align: top;
	}
	.chat-markdown :global(th) {
		background: color-mix(in oklab, var(--color-base-200) 45%, transparent);
		font-weight: 600;
	}
	/* Tool step args: reset the prose <pre> treatment so they stay compact. */
	.chat-tools :global(pre) {
		margin: 0.125rem 0 0;
		padding: 0.25rem 0.5rem;
		border-radius: 0.3rem;
		background: color-mix(in oklab, var(--color-base-200) 50%, transparent);
		line-height: 1.4;
	}
</style>
