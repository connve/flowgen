import { apiUrl, type ClusterStatus } from '$lib/api';

const POLL_MS = 15_000;

export const cluster = $state<{ status: ClusterStatus | null; loaded: boolean }>({
	status: null,
	loaded: false,
});

export function unreachableCount(status: ClusterStatus): number {
	return status.pods.filter((pod) => pod.unreachable_reason).length;
}

export function pollClusterStatus(): () => void {
	const load = async () => {
		try {
			const res = await fetch(apiUrl('api/cluster'));
			cluster.status = res.ok ? await res.json() : null;
		} catch {
			cluster.status = null;
		} finally {
			cluster.loaded = true;
		}
	};
	void load();
	const timer = setInterval(load, POLL_MS);
	return () => clearInterval(timer);
}
