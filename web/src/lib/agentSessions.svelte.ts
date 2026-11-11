import type { SessionSummary } from '$lib/sessions';
import { sessionsStream } from '$lib/urls';
import { visibleEventSource } from '$lib/visibleEventSource';

// The Agents tab's sessions, kept live from the server's stream.
export const agentSessions = $state<{ list: SessionSummary[]; unread: number }>({
	list: [],
	unread: 0
});

function show(sessions: SessionSummary[]) {
	agentSessions.list = sessions.toSorted((a, b) => b.updatedAt - a.updatedAt);
	agentSessions.unread = sessions.filter((c) => c.unread).length;
}

// Follows `/api/agents/sessions/stream`: the list on connect, then each
// change. EventSource reconnects by itself and the list comes again.
export function followAgentSessions(): () => void {
	return visibleEventSource(sessionsStream(), (source) => {
		source.addEventListener('sessions', (e) => {
			show(JSON.parse(e.data));
		});
		source.addEventListener('session', (e) => {
			const changed: SessionSummary = JSON.parse(e.data);
			show([...agentSessions.list.filter((c) => c.id !== changed.id), changed]);
		});
		source.addEventListener('removed', (e) => {
			show(agentSessions.list.filter((c) => c.id !== e.data));
		});
	});
}
