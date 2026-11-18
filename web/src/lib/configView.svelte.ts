// Whether configs show as fields or YAML, remembered in this browser.

export type ConfigView = 'fields' | 'yaml';

const KEY = 'flowgen-config-view';

function saved(): ConfigView {
	try {
		return localStorage.getItem(KEY) === 'yaml' ? 'yaml' : 'fields';
	} catch {
		return 'fields';
	}
}

class ConfigViewPref {
	#value = $state<ConfigView>(typeof localStorage === 'undefined' ? 'fields' : saved());

	get value(): ConfigView {
		return this.#value;
	}

	set value(next: ConfigView) {
		this.#value = next;
		try {
			localStorage.setItem(KEY, next);
		} catch {
			// Not remembered, but the view still changes for this visit.
		}
	}
}

export const configView = new ConfigViewPref();
