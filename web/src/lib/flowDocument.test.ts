import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { FlowDocument } from './flowDocument';

const EXAMPLES = join(__dirname, '../../../examples');

function flowFiles(folder: string): string[] {
	return readdirSync(folder).flatMap((entry) => {
		const path = join(folder, entry);
		if (statSync(path).isDirectory()) return entry === 'resources' ? [] : flowFiles(path);
		return /\.ya?ml$/.test(entry) ? [path] : [];
	});
}

const FLOW = `# A comment that stays.
flow:
  tasks:
    - generate:
        name: tick # and this one
        interval: 1m
    - script:
        name: shape
    - log:
        name: out
        depends_on: [shape]
`;

function commentsUnindented(source: string): string {
	return source
		.split('\n')
		.map((line) => (line.trimStart().startsWith('#') ? line.trimStart() : line))
		.join('\n');
}

describe('FlowDocument', () => {
	it('prints every example as it read it, at most indenting a comment differently', () => {
		const changed = flowFiles(EXAMPLES).filter((path) => {
			const source = readFileSync(path, 'utf8');
			const printed = new FlowDocument(source).toString();
			return commentsUnindented(printed) !== commentsUnindented(source);
		});
		expect(changed).toEqual([]);
	});

	it('sets and removes a setting and keeps the comments', () => {
		const document = new FlowDocument(FLOW);
		document.setTaskValue(0, ['interval'], '5m');
		document.setTaskValue(1, ['code'], 'event');
		document.setTaskValue(2, ['level'], undefined);

		const printed = document.toString();
		expect(printed).toContain('# A comment that stays.');
		expect(printed).toContain('name: tick # and this one');
		expect(document.tasks()[0].values.interval).toBe('5m');
		expect(document.tasks()[1].values.code).toBe('event');
	});

	it('adds a task after another', () => {
		const document = new FlowDocument(FLOW);
		const index = document.addTask('convert', 'to_json', 0);

		expect(index).toBe(1);
		expect(document.tasks().map((task) => task.name)).toEqual(['tick', 'to_json', 'shape', 'out']);
	});

	it('renames a task and the tasks that name it', () => {
		const document = new FlowDocument(FLOW);
		document.renameTask(1, 'reshape');

		expect(document.tasks()[1].name).toBe('reshape');
		expect(document.tasks()[2].values.depends_on).toEqual(['reshape']);
	});

	it('removes a task and points the tasks after it at its own parents', () => {
		const document = new FlowDocument(FLOW);
		document.removeTask(1);

		expect(document.tasks().map((task) => task.name)).toEqual(['tick', 'out']);
		expect(document.tasks()[1].values.depends_on).toEqual(['tick']);
	});
});
