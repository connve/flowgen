import { Remembered } from '$lib/remembered.svelte';

export type ChangeView = 'preview' | 'diff';

export const changeView = new Remembered<ChangeView>('flowgen-change-view', ['preview', 'diff'], 'preview');
