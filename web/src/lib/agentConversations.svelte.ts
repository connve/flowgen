import { apiUrl } from '$lib/api';
import type { ConversationSummary } from '$lib/conversations';

// The Agents tab's conversations, kept live from the server's stream.
export const agentConversations = $state<{ list: ConversationSummary[]; unread: number }>({
	list: [],
	unread: 0
});

function show(conversations: ConversationSummary[]) {
	agentConversations.list = conversations.toSorted((a, b) => b.updatedAt - a.updatedAt);
	agentConversations.unread = conversations.filter((c) => c.unread).length;
}

// Follows `/api/agents/conversations/stream`: the list on connect, then each
// change. EventSource reconnects by itself and the list comes again.
export function followAgentConversations(): () => void {
	const source = new EventSource(apiUrl('api/agents/conversations/stream'));
	source.addEventListener('conversations', (e) => {
		show(JSON.parse(e.data));
	});
	source.addEventListener('conversation', (e) => {
		const changed: ConversationSummary = JSON.parse(e.data);
		show([...agentConversations.list.filter((c) => c.id !== changed.id), changed]);
	});
	source.addEventListener('removed', (e) => {
		show(agentConversations.list.filter((c) => c.id !== e.data));
	});
	return () => source.close();
}
