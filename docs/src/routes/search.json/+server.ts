import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { searchIndex } from '$lib/search';

export const prerender = true;

export const GET: RequestHandler = () => json(searchIndex());
