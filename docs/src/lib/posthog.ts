import { browser, dev } from '$app/environment';
import posthog from 'posthog-js';
import {
	PUBLIC_POSTHOG_KEY,
	PUBLIC_POSTHOG_HOST,
	PUBLIC_POSTHOG_UI_HOST
} from '$env/static/public';

const LOCAL_HOSTS = ['localhost', '127.0.0.1', '[::1]'];

let initialized = false;

/** Whether analytics runs: a published build with a key, never in dev or on this machine. */
function enabled(): boolean {
	return browser && !dev && !!PUBLIC_POSTHOG_KEY && !LOCAL_HOSTS.includes(location.hostname);
}

export function initPosthog(): typeof posthog | null {
	if (!enabled()) return null;
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
	if (!initPosthog()) return;
	posthog.identify(email, { email, ...properties });
}

export function capture(event: string, properties?: Record<string, unknown>): void {
	if (!initPosthog()) return;
	posthog.capture(event, properties);
}
