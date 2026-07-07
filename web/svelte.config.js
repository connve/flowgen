import adapter from '@sveltejs/adapter-static';

const base = process.env.PUBLIC_BASE ?? '';

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
