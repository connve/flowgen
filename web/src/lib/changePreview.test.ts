import { describe, expect, it } from 'vitest';
import { compareFlows } from './changePreview';

const BEFORE = `flow:
  tasks:
    - generate:
        name: tick
        interval: 1m
    - script:
        name: shape
        code: event.data
    - log:
        name: out
`;

const AFTER = `flow:
  tasks:
    - generate:
        name: tick
        interval: 5m
    - script:
        name: shape
        code: event.data
    - http_request:
        name: send
        endpoint: https://example.com
`;

describe('compareFlows', () => {
	it('marks added, removed and changed tasks and leaves unchanged ones unmarked', () => {
		const comparison = compareFlows(BEFORE, AFTER)!;
		const marks = Object.fromEntries(comparison.dag.nodes.map((node) => [node.name, node.mark]));
		expect(marks).toEqual({ tick: 'changed', shape: undefined, send: 'added', out: 'removed' });
		expect([...comparison.tasks.keys()].sort()).toEqual(['out', 'send', 'tick']);
	});

	it('lists the changed settings before and after', () => {
		const comparison = compareFlows(BEFORE, AFTER)!;
		expect(comparison.tasks.get('tick')!.fields).toEqual([{ key: 'interval', before: '1m', after: '5m' }]);
		expect(comparison.tasks.get('send')!.fields).toEqual([
			{ key: 'endpoint', before: undefined, after: 'https://example.com' }
		]);
		expect(comparison.tasks.get('out')!.fields).toEqual([]);
	});

	it('reports a changed task type as a setting', () => {
		const comparison = compareFlows(BEFORE, AFTER.replace('- script:', '- js_script:'))!;
		expect(comparison.tasks.get('shape')).toEqual({
			mark: 'changed',
			fields: [{ key: 'type', before: 'script', after: 'js_script' }]
		});
	});

	it('colors only the wiring of added and removed tasks', () => {
		const comparison = compareFlows(BEFORE, AFTER)!;
		const marks = Object.fromEntries(comparison.dag.edges.map((edge) => [edge.id, edge.mark]));
		expect(marks).toEqual({ 'tick->shape': undefined, 'shape->send': 'added', 'shape->out': 'removed' });
	});

	it('follows depends_on when it rewires a task', () => {
		const rewired = AFTER.replace('endpoint: https://example.com', 'depends_on: [tick]');
		const marks = Object.fromEntries(compareFlows(BEFORE, rewired)!.dag.edges.map((edge) => [edge.id, edge.mark]));
		expect(marks['tick->send']).toBe('added');
		expect(marks['shape->send']).toBeUndefined();
	});

	it('shows reordered tasks as changed in where they take events from, with no old wiring', () => {
		const reordered = `flow:
  tasks:
    - script:
        name: shape
        code: event.data
    - generate:
        name: tick
        interval: 1m
    - log:
        name: out
`;
		const comparison = compareFlows(BEFORE, reordered)!;
		expect(comparison.dag.edges.map((edge) => [edge.id, edge.mark])).toEqual([
			['shape->tick', undefined],
			['tick->out', undefined]
		]);
		expect(comparison.tasks.get('tick')!.fields).toEqual([
			{ key: 'takes_events_from', before: [], after: ['shape'] }
		]);
		expect(comparison.tasks.get('out')!.fields).toEqual([
			{ key: 'takes_events_from', before: ['shape'], after: ['tick'] }
		]);
	});

	it('marks every task of a new file as added, without marking its wiring', () => {
		const comparison = compareFlows(null, AFTER)!;
		expect(comparison.dag.nodes.every((node) => node.mark === 'added')).toBe(true);
		expect(comparison.dag.edges.every((edge) => edge.mark === undefined)).toBe(true);
	});

	it('marks every task and wire of a deleted file as removed', () => {
		const comparison = compareFlows(BEFORE, '')!;
		expect(comparison.dag.nodes.map((node) => node.mark)).toEqual(['removed', 'removed', 'removed']);
		expect(comparison.dag.edges.every((edge) => edge.mark === 'removed')).toBe(true);
	});

	it('returns null when a version does not parse', () => {
		expect(compareFlows('flow: [', AFTER)).toBeNull();
		expect(compareFlows(BEFORE, 'flow: [')).toBeNull();
	});
});
