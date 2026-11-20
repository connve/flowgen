// Reading the flow JSON Schema for forms: which control a field gets, its
// default, and whether it is required, advanced or hidden.

export type Schema = Record<string, unknown>;

export type Kind =
	| 'string'
	| 'integer'
	| 'number'
	| 'boolean'
	| 'enum'
	| 'duration'
	| 'credentials'
	| 'source'
	| 'list'
	| 'map'
	| 'object'
	| 'yaml';

export interface Field {
	name: string;
	schema: Schema;
	kind: Kind;
	required: boolean;
	advanced: boolean;
	description?: string;
	default?: unknown;
}

export interface Option {
	value: string;
	description?: string;
}

/** `node`, followed through its `$ref`s within `root`. */
export function resolve(root: Schema, node: unknown): Schema {
	let schema = (node ?? {}) as Schema;
	for (let depth = 0; typeof schema.$ref === 'string' && depth < 16; depth++) {
		const target = pointer(root, schema.$ref.replace(/^#/, ''));
		if (!target) break;
		const { $ref: _, ...rest } = schema;
		schema = { ...(target as Schema), ...rest };
	}
	return schema;
}

function pointer(root: Schema, path: string): unknown {
	return path
		.split('/')
		.slice(1)
		.reduce<unknown>(
			(node, part) => (node as Schema | undefined)?.[part.replace(/~1/g, '/').replace(/~0/g, '~')],
			root
		);
}

/** The schema without the `null` an `Option` adds. */
export function unwrapOptional(root: Schema, schema: Schema): Schema {
	const variants = (schema.anyOf ?? schema.oneOf) as Schema[] | undefined;
	if (variants) {
		const rest = variants.filter((variant) => variant.type !== 'null');
		if (rest.length === 1 && rest.length < variants.length) {
			return { ...resolve(root, rest[0]), description: schema.description, default: schema.default };
		}
	}
	if (Array.isArray(schema.type)) {
		const types = schema.type.filter((type) => type !== 'null');
		if (types.length === 1) return { ...schema, type: types[0] };
	}
	return schema;
}

/** The choices of an enum, with their descriptions when documented. */
export function options(schema: Schema): Option[] | null {
	if (Array.isArray(schema.enum)) return schema.enum.map((value) => ({ value: String(value) }));
	const variants = (schema.oneOf ?? schema.anyOf) as Schema[] | undefined;
	if (variants?.length && variants.every((variant) => 'const' in variant || Array.isArray(variant.enum))) {
		return variants.flatMap((variant) =>
			'const' in variant
				? [{ value: String(variant.const), description: variant.description as string | undefined }]
				: (variant.enum as unknown[]).map((value) => ({ value: String(value), description: variant.description as string | undefined }))
		);
	}
	return null;
}

export function kindOf(root: Schema, schema: Schema): Kind {
	if (schema['x-widget'] === 'credentials') return 'credentials';
	if (schema['x-widget'] === 'source') return 'source';
	if (schema.format === 'duration') return 'duration';
	if (options(schema)) return 'enum';
	switch (schema.type) {
		case 'string':
			return 'string';
		case 'integer':
			return 'integer';
		case 'number':
			return 'number';
		case 'boolean':
			return 'boolean';
		case 'array': {
			const items = resolve(root, schema.items);
			return ['string', 'integer', 'number'].includes(items.type as string) ? 'list' : 'yaml';
		}
		case 'object':
			if (schema.properties) return 'object';
			if (schema.additionalProperties && typeof schema.additionalProperties === 'object') {
				const values = resolve(root, schema.additionalProperties);
				return values.type === 'string' ? 'map' : 'yaml';
			}
			return 'yaml';
	}
	return 'yaml';
}

/** The fields of an object schema, required first, then in schema order. */
export function fieldsOf(root: Schema, schema: Schema): Field[] {
	const properties = (schema.properties ?? {}) as Record<string, Schema>;
	const required = new Set((schema.required ?? []) as string[]);
	return Object.entries(properties)
		.filter(([, property]) => !property['x-hidden'])
		.map(([name, property]) => {
			const resolved = unwrapOptional(root, resolve(root, property));
			return {
				name,
				schema: resolved,
				kind: kindOf(root, resolved),
				required: required.has(name),
				advanced: Boolean(property['x-advanced'] ?? resolved['x-advanced']),
				description: (property.description ?? resolved.description) as string | undefined,
				default: property.default ?? resolved.default
			};
		})
		.sort((a, b) => Number(b.required) - Number(a.required));
}

/** The schema of each task type, by its YAML tag. */
export function taskSchemas(root: Schema): Map<string, Schema> {
	const variants = ((resolve(root, pointer(root, '/$defs/TaskType')).oneOf ?? []) as Schema[]);
	const tasks = new Map<string, Schema>();
	for (const variant of variants) {
		for (const [tag, task] of Object.entries((variant.properties ?? {}) as Record<string, Schema>)) {
			tasks.set(tag, resolve(root, task));
		}
	}
	return tasks;
}

/** Whether `value` is what the field takes when it is left out. */
export function isDefault(field: Field, value: unknown): boolean {
	return value === undefined || value === '' || JSON.stringify(value) === JSON.stringify(field.default);
}
