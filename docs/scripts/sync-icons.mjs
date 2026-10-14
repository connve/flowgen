// Copies flowgen/<crate>/icons/ into static/icons/ before dev/build, served as /icons/<file>.
import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const crates = resolve(here, '../../flowgen');
const dest = resolve(here, '../static/icons');

rmSync(dest, { recursive: true, force: true });
mkdirSync(dest, { recursive: true });
for (const crate of readdirSync(crates)) {
	const icons = join(crates, crate, 'icons');
	if (existsSync(icons)) cpSync(icons, dest, { recursive: true });
}

console.log(`icons synced ${crates}/*/icons → ${dest}`);
