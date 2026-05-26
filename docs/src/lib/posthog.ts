import { browser } from '$app/environment';
import posthog from 'posthog-js';
import {
	PUBLIC_POSTHOG_KEY,
	PUBLIC_POSTHOG_HOST,
	PUBLIC_POSTHOG_UI_HOST
} from '$env/static/public';

let initialized = false;

export function initPosthog(): typeof posthog | null {
	if (!browser) return null;
	if (!PUBLIC_POSTHOG_KEY) return null;
	if (initialized) return posthog;

	posthog.init(PUBLIC_POSTHOG_KEY, {
		api_host: PUBLIC_POSTHOG_HOST || '/ingest',
		ui_host: PUBLIC_POSTHOG_UI_HOST || 'https://eu.posthog.com',
		capture_pageview: true,
		capture_pageleave: true
	});
	initialized = true;
	return posthog;
}

export function identify(email: string, properties?: Record<string, unknown>): void {
	if (!browser) return;
	if (!PUBLIC_POSTHOG_KEY) return;
	initPosthog();
	posthog.identify(email, { email, ...properties });
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
