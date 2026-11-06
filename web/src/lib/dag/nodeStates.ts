import { createContext } from 'svelte';

export interface NodeState {
	level: 'info' | 'warn' | 'error';
	ts_ms: number;
	duration_ms?: number;
}

/** Each task's latest status, shared from the graph with its nodes. */
export const [getNodeStates, setNodeStates] = createContext<() => Map<string, NodeState>>();
