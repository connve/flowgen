// Below Tailwind's `md` breakpoint, where side panes open over the page
// instead of beside it and start closed.
export function isMobile(): boolean {
	return typeof window !== 'undefined' && window.matchMedia('(max-width: 767px)').matches;
}

/** Classes that lay an open side pane over the page on mobile. */
export const PANE_OVER_PAGE =
	'max-md:absolute max-md:inset-y-0 max-md:left-0 max-md:z-30 max-md:max-w-[85vw] max-md:shadow-xl';
