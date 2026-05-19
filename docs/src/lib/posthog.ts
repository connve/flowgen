import { browser } from '$app/environment';
import posthog from 'posthog-js';
import { PUBLIC_POSTHOG_KEY, PUBLIC_POSTHOG_HOST } from '$env/static/public';

let initialized = false;

export function initPosthog(): typeof posthog | null {
	if (!browser) return null;
	if (!PUBLIC_POSTHOG_KEY) return null;
	if (initialized) return posthog;

	posthog.init(PUBLIC_POSTHOG_KEY, {
		api_host: PUBLIC_POSTHOG_HOST || '/ingest',
		ui_host: 'https://eu.posthog.com',
		capture_pageview: true,
		capture_pageleave: true
	});
	initialized = true;
	return posthog;
}

export function capture(event: string, properties?: Record<string, unknown>): void {
	if (!browser) return;
	if (!PUBLIC_POSTHOG_KEY) {
		console.info('[posthog] capture (no key):', event, properties);
		return;
	}
	initPosthog();
	posthog.capture(event, properties);
}
