<script lang="ts">
	import { page } from '$app/state';
	import { apiUrl } from '$lib/api';
	import StateMessage from '$lib/StateMessage.svelte';
	import { m } from '$lib/paraglide/messages.js';
	import Icon from '@iconify/svelte';

	// The server sends only these codes, so a crafted link cannot put text here.
	const ERRORS: Record<string, () => string> = {
		access_denied: m.login_error_access_denied,
		provider: m.login_error_provider,
		expired: m.login_error_expired,
		failed: m.login_error_failed
	};

	let error = $derived(page.url.searchParams.get('error'));
	let errorMessage = $derived(
		error == null
			? null
			: (Object.hasOwn(ERRORS, error) ? ERRORS[error] : m.login_error_failed)()
	);
</script>

<svelte:head>
	<title>{m.login_page_title()}</title>
</svelte:head>

<div class="flex min-h-screen flex-col bg-base-100 text-base-content">
	<StateMessage
		tone={errorMessage ? 'oops' : 'notice'}
		title={errorMessage ? m.login_failed_title() : m.login_title()}
		message={errorMessage ?? m.login_message()}
		icon={errorMessage ? 'tabler:lock-x' : 'tabler:lock'}
	>
		<a href={apiUrl('auth/login')} data-sveltekit-reload class="btn btn-primary btn-sm gap-1.5">
			<Icon icon="tabler:login-2" class="h-4 w-4" />
			{m.login_sign_in()}
		</a>
	</StateMessage>
</div>
