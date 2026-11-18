import { base } from '$app/paths';
import type { CreateClientConfig } from './generated/client.gen';

export const createClientConfig: CreateClientConfig = (config) => ({
	...config,
	baseUrl: base,
	throwOnError: true
});
