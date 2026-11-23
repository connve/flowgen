export class Remembered<T extends string> {
	#value = $state('');
	readonly #key: string;

	constructor(key: string, values: readonly T[], fallback: T) {
		this.#key = key;
		this.#value = stored(key, values) ?? fallback;
	}

	get value(): T {
		return this.#value as T;
	}

	set value(next: T) {
		this.#value = next;
		try {
			localStorage.setItem(this.#key, next);
		} catch {
			// Not remembered, but the choice still changes for this visit.
		}
	}
}

function stored<T extends string>(key: string, values: readonly T[]): T | null {
	if (typeof localStorage === 'undefined') return null;
	try {
		const value = localStorage.getItem(key);
		return values.find((known) => known === value) ?? null;
	} catch {
		return null;
	}
}
