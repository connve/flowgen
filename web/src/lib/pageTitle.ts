// Browser tab titles: most specific first, then the section, then the app,
// e.g. `hello · Agents · Flowgen`. Empty parts are skipped.
export function pageTitle(...parts: Array<string | null | undefined>): string {
	return [...parts.filter((part): part is string => !!part), 'Flowgen'].join(' · ');
}
