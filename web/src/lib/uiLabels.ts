import { labels } from '@connve/ui';
import { m } from '$lib/paraglide/messages.js';

/** Gives the shared components the app's own wording. */
export function setUiLabels() {
	labels.back = m.common_back();
	labels.close = m.common_close();
	labels.copy = m.common_copy();
	labels.copied = m.common_copied();
	labels.loading = m.common_loading();
	labels.resize = m.pane_resize();
	labels.new = m.common_new();
	labels.edit = m.common_edit();
	labels.editReadOnly = m.edit_button_read_only();
	labels.menu = m.nav_menu();
	labels.navigation = m.nav_main();
	labels.navExpand = m.nav_expand();
	labels.navCollapse = m.nav_collapse();
	labels.navBadge = (label, count) => m.nav_badge({ label, count });
	labels.breadcrumb = m.common_breadcrumb();
	labels.theme = m.theme_label();
	labels.themeSystem = m.theme_system();
	labels.themeLight = m.theme_light();
	labels.themeDark = m.theme_dark();
	labels.themeOption = (theme) => m.theme_option({ theme });
	labels.stateNotice = m.state_notice_title();
	labels.stateOops = m.state_oops_title();
}
