import { browser } from '$app/environment';
import posthog from 'posthog-js';
import { env } from '$env/dynamic/public';

let initialized = false;

export function initPosthog(): typeof posthog | null {
	if (!browser) return null;
	if (!env.PUBLIC_POSTHOG_KEY) return null;
	if (initialized) return posthog;

	posthog.init(env.PUBLIC_POSTHOG_KEY, {
		api_host: env.PUBLIC_POSTHOG_HOST || '/ingest',
		ui_host: env.PUBLIC_POSTHOG_UI_HOST || 'https://eu.posthog.com',
		capture_pageview: true,
		capture_pageleave: true
	});
	initialized = true;
	return posthog;
}

export function capture(event: string, properties?: Record<string, unknown>): void {
	if (!browser) return;
	if (!env.PUBLIC_POSTHOG_KEY) return;
	initPosthog();
	posthog.capture(event, properties);
}
