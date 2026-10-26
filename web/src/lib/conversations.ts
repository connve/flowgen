// Client for the built-in Agents chat conversation store. History lives in
// flowgen's configured system cache and is served by the web API
// (`/api/agents/conversations`); the server writes agent replies into it.
// Types come from the generated OpenAPI schema so they stay in lockstep with
// the Rust handlers.

import { apiUrl, failure } from '$lib/api';
import type { components } from '$lib/api/generated';
import { m } from '$lib/paraglide/messages.js';

export type ConversationMessage = components['schemas']['ConversationMessage'];
export type Conversation = components['schemas']['Conversation'];
export type ConversationSummary = components['schemas']['ConversationSummary'];
export type ToolStep = components['schemas']['ToolStep'];

// Fetches one conversation with its full message history.
export async function getConversation(id: string): Promise<Conversation> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}`));
	if (!res.ok) throw await failure(res);
	return res.json();
}

// Adds the user's message and has the agent reply on the server.
export async function startReply(id: string, content: string, model: string): Promise<void> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}/reply`), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ content, model })
	});
	if (res.status === 409) throw new Error(m.agents_reply_in_progress());
	if (!res.ok) throw await failure(res);
}

// Stops the conversation's reply on whichever pod runs it; false when none is running.
export async function stopReply(id: string): Promise<boolean> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}/reply`), {
		method: 'DELETE'
	});
	if (res.status === 404) return false;
	if (!res.ok) throw await failure(res);
	return true;
}

// Marks a conversation read or unread for the signed-in user only.
export async function setConversationRead(id: string, read: boolean): Promise<void> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}/read`), {
		method: read ? 'PUT' : 'DELETE'
	});
	if (!res.ok) throw await failure(res);
}

// Renames a conversation or files it in one of the user's folders (`a/b`,
// empty for none). Fields left out stay as they are.
export async function updateConversation(
	id: string,
	update: { title?: string; folder?: string }
): Promise<void> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}`), {
		method: 'PATCH',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(update)
	});
	if (!res.ok) throw await failure(res);
}

export async function deleteConversation(id: string): Promise<void> {
	const res = await fetch(apiUrl(`api/agents/conversations/${encodeURIComponent(id)}`), {
		method: 'DELETE'
	});
	if (!res.ok) throw await failure(res);
}
