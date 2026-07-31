// Client for the built-in Agents chat conversation store. History lives in
// flowgen's configured system cache and is served by the admin API
// (`/api/agents/conversations`). Types come from the generated OpenAPI schema
// so they stay in lockstep with the Rust handlers.

import { apiUrl } from '$lib/api';
import type { components } from '$lib/api/generated';

export type ConversationMessage = components['schemas']['ConversationMessage'];
export type Conversation = components['schemas']['Conversation'];
export type ConversationSummary = components['schemas']['ConversationSummary'];
export type ToolStep = components['schemas']['ToolStep'];

// Fetches conversation summaries (no message bodies), newest first.
export async function listConversations(): Promise<ConversationSummary[]> {
	const res = await fetch(apiUrl('api/agents/conversations'));
	if (!res.ok) throw new Error(`HTTP ${res.status}`);
	const body = await res.json();
	return body.conversations ?? [];
}

// Fetches one conversation with its full message history.
export async function getConversation(id: string): Promise<Conversation> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}`));
	if (!res.ok) throw new Error(`HTTP ${res.status}`);
	return res.json();
}

// Creates or overwrites a conversation. The server stamps `updatedAt` and
// refreshes the TTL, so the expiry window counts from the last write.
export async function putConversation(
	id: string,
	title: string,
	messages: ConversationMessage[],
	model?: string
): Promise<void> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}`), {
		method: 'PUT',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ title, messages, model })
	});
	if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

export async function deleteConversation(id: string): Promise<void> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}`), {
		method: 'DELETE'
	});
	if (!res.ok) throw new Error(`HTTP ${res.status}`);
}

// Longest title kept from a first message before it is truncated.
const TITLE_MAX = 60;

// A conversation title taken from its first user message. Whitespace is
// collapsed and the text is cut to at most `TITLE_MAX` characters on a word
// boundary (never mid-word), with an ellipsis when anything was dropped. The
// message may have no sentence structure, so the cut is purely by length.
export function deriveTitle(firstUserMessage: string): string {
	const text = firstUserMessage.trim().replace(/\s+/g, ' ');
	if (!text) return 'New conversation';
	if (text.length <= TITLE_MAX) return text;
	const clipped = text.slice(0, TITLE_MAX);
	// Back up to the last space so we don't end in the middle of a word; fall
	// back to the hard cut if the first word alone already exceeds the limit.
	const lastSpace = clipped.lastIndexOf(' ');
	const head = lastSpace > 0 ? clipped.slice(0, lastSpace) : clipped;
	return `${head}…`;
}
