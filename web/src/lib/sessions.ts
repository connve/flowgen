// Client for the built-in Agents chat session store. History lives in
// flowgen's configured system cache and is served by the web API; the server
// writes agent replies into it.

import * as api from '$lib/api';
import { isStatus } from '$lib/api';
import { m } from '$lib/paraglide/messages.js';

export type { Session, SessionMessage, SessionSummary, ToolStep } from '$lib/api';

const session = (id: string) => ({ path: { id } });

// Fetches one session with its full message history.
export async function getSession(id: string): Promise<api.Session> {
	return (await api.getSession(session(id))).data;
}

// Adds the user's message and has the agent reply on the server.
export async function startReply(id: string, content: string, model: string): Promise<void> {
	try {
		await api.startReply({ ...session(id), body: { content, model } });
	} catch (err) {
		if (isStatus(err, 409)) throw new Error(m.agents_reply_in_progress());
		throw err;
	}
}

// Stops the session's reply on whichever pod runs it; false when none is running.
export async function stopReply(id: string): Promise<boolean> {
	try {
		await api.stopReply(session(id));
		return true;
	} catch (err) {
		if (isStatus(err, 404)) return false;
		throw err;
	}
}

// Marks a session read or unread for the signed-in user only.
export async function setSessionRead(id: string, read: boolean): Promise<void> {
	await (read ? api.markSessionRead(session(id)) : api.markSessionUnread(session(id)));
}

// Renames a session or files it in one of the user's folders (`a/b`,
// empty for none). Fields left out stay as they are.
export async function updateSession(
	id: string,
	update: { title?: string; folder?: string }
): Promise<void> {
	await api.updateSession({ ...session(id), body: update });
}

export async function deleteSession(id: string): Promise<void> {
	await api.deleteSession(session(id));
}
