import adapter from '@sveltejs/adapter-static';

// Baked into the build so SvelteKit's router matches the same prefix the
// backend mounts the UI under (`web.path` in flowgen config). Override
// with PUBLIC_BASE at build time when deploying under a different prefix.
const base = process.env.PUBLIC_BASE ?? '/flowgen';

export default {
	kit: {
		adapter: adapter({
			pages: 'build',
			assets: 'build',
			fallback: 'index.html',
			precompress: false,
			strict: true
		}),
		paths: {
			base,
			relative: true
		}
	}
};
