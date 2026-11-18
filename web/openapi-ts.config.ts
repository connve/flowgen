import { defineConfig } from '@hey-api/openapi-ts';

export default defineConfig({
	input: '../flowgen/api/client/openapi.yaml',
	output: { path: 'src/lib/api/generated', importFileExtension: null },
	plugins: [
		{ name: '@hey-api/client-fetch', runtimeConfigPath: './src/lib/api/config', throwOnError: true },
		'@hey-api/typescript',
		'@hey-api/sdk'
	]
});
