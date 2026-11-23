// Whether configs show as fields or YAML, remembered in this browser.

import { Remembered } from '$lib/remembered.svelte';

export type ConfigView = 'fields' | 'yaml';

export const configView = new Remembered<ConfigView>('flowgen-config-view', ['fields', 'yaml'], 'fields');
