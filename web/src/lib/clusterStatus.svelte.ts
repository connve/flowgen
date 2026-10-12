import { apiUrl, errorMessage, failure, type ClusterStatus } from '$lib/api';
import { m } from '$lib/paraglide/messages.js';

const POLL_MS = 15_000;

export const cluster = $state<{ status: ClusterStatus | null; loaded: boolean; error: string | null }>({
	status: null,
	loaded: false,
	error: null,
});

export function unreachableCount(status: ClusterStatus): number {
	return status.pods.filter((pod) => pod.unreachable_reason).length;
}

export function pollClusterStatus(): () => void {
	const load = async () => {
		try {
			const res = await fetch(apiUrl('api/cluster'));
			if (!res.ok) throw await failure(res);
			cluster.status = await res.json();
			cluster.error = null;
		} catch (err) {
			cluster.status = null;
			cluster.error = errorMessage(err, m.pods_load_failed_message());
		} finally {
			cluster.loaded = true;
		}
	};
	void load();
	const timer = setInterval(load, POLL_MS);
	return () => clearInterval(timer);
}
