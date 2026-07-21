export interface TreeNode<T> {
	name: string;
	fullPath: string;
	depth: number;
	isFolder: boolean;
	children?: TreeNode<T>[];
	leaf?: T;
	fileCount?: number;
}

// Builds a slash-delimited path tree from a flat list. Each item's `path`
// function returns its slash-joined key. Folder-first ordering, alphabetical
// within a level; folders get `fileCount` set to their recursive leaf count.
export function buildTree<T>(items: T[], path: (item: T) => string): TreeNode<T>[] {
	const root: TreeNode<T> = { name: '', fullPath: '', depth: -1, isFolder: true, children: [] };
	for (const item of items) {
		const parts = path(item).split('/');
		let cursor = root;
		for (let i = 0; i < parts.length; i++) {
			const name = parts[i];
			const isLeaf = i === parts.length - 1;
			const fullPath = parts.slice(0, i + 1).join('/');
			let child = cursor.children!.find((c) => c.name === name);
			if (!child) {
				child = {
					name,
					fullPath,
					depth: i,
					isFolder: !isLeaf,
					children: isLeaf ? undefined : [],
					leaf: isLeaf ? item : undefined
				};
				cursor.children!.push(child);
			}
			cursor = child;
		}
	}
	function sortRec(node: TreeNode<T>) {
		if (!node.children) return;
		node.children.sort((a, b) => {
			if (a.isFolder !== b.isFolder) return a.isFolder ? -1 : 1;
			return a.name.localeCompare(b.name);
		});
		for (const c of node.children) sortRec(c);
	}
	sortRec(root);
	function countLeaves(node: TreeNode<T>): number {
		if (!node.isFolder) return 1;
		let n = 0;
		for (const c of node.children ?? []) n += countLeaves(c);
		node.fileCount = n;
		return n;
	}
	countLeaves(root);
	return root.children ?? [];
}
