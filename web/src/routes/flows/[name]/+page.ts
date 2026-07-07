// The flow name is a runtime value looked up against the admin API, so
// static prerendering isn't possible. `ssr = false` keeps this a
// client-only route and `prerender = false` opts out of the static build.
export const prerender = false;
export const ssr = false;
