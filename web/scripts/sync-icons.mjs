// Docs owns the icon set. This script mirrors docs/static/icons/ into
// web/static/icons/ before dev/build so both apps share one source.
import { cpSync, mkdirSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const src = resolve(here, '../../docs/static/icons');
const dest = resolve(here, '../static/icons');

rmSync(dest, { recursive: true, force: true });
mkdirSync(dest, { recursive: true });
cpSync(src, dest, { recursive: true });

console.log(`icons synced ${src} → ${dest}`);
