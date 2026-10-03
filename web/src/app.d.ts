/// <reference types="@sveltejs/kit" />

declare global {
	namespace App {
		interface Error {}
		interface Locals {}
		interface PageData {}
		interface PageState {
			flow?: string;
			resource?: string;
		}
		interface Platform {}
	}
}

export {};
