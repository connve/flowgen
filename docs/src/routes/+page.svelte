<script lang="ts">
	import { navigation } from '$lib/nav';
</script>

<div class="min-h-screen bg-base-100">
	<div class="navbar bg-base-100 border-b border-base-300 sticky top-0 z-50">
		<div class="navbar-start">
			<a href="/" class="text-xl font-bold text-primary px-4">flowgen</a>
		</div>
		<div class="navbar-end px-4">
		</div>
	</div>

	<div class="hero min-h-screen bg-base-100">
		<div class="hero-content text-center">
			<div class="max-w-2xl">
				<h1 class="text-5xl font-extrabold leading-tight">
					Data activation
					<br />
					<span class="text-primary">with a blast 💥</span>
				</h1>
				<p class="py-6 text-lg text-base-content/70 max-w-xl mx-auto">
					Open-source. Distributed by design. Define flows — event-driven, scheduled, or streaming — in declarative YAML. Connect systems, transform data, activate it.
				</p>
				<div class="flex gap-3 justify-center">
					<a href="/docs/getting-started/why-flowgen" class="btn btn-primary btn-lg">Documentation</a>
					<a href="/docs/getting-started/quickstart" class="btn btn-outline btn-lg">Quick Start</a>
				</div>
			</div>
		</div>
	</div>

	<div class="py-16 bg-base-200">
		<div class="max-w-5xl mx-auto px-6">
			<div class="grid md:grid-cols-2 lg:grid-cols-3 gap-6">

				<div class="card bg-base-100 shadow-md hover:shadow-lg transition-shadow">
					<div class="card-body">
						<div class="w-10 h-10 bg-primary rounded-lg flex items-center justify-center mb-2">
							<svg class="w-5 h-5 text-primary-content" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
								<path d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
							</svg>
						</div>
						<h3 class="card-title text-lg">Fully open-source</h3>
						<p class="text-sm text-base-content/70">
							MPL-2.0 licensed. Every feature, every connector available to everyone. Run it on your infrastructure. No per-row pricing.
						</p>
					</div>
				</div>

				<div class="card bg-base-100 shadow-md hover:shadow-lg transition-shadow">
					<div class="card-body">
						<div class="w-10 h-10 bg-primary rounded-lg flex items-center justify-center mb-2">
							<svg class="w-5 h-5 text-primary-content" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
								<path d="M4 6h16M4 12h16M4 18h16" />
							</svg>
						</div>
						<h3 class="card-title text-lg">Distributed by design</h3>
						<p class="text-sm text-base-content/70">
							Leader election, multi-node scaling, and distributed caching built into the runtime.
						</p>
					</div>
				</div>

				<div class="card bg-base-100 shadow-md hover:shadow-lg transition-shadow">
					<div class="card-body">
						<div class="w-10 h-10 bg-primary rounded-lg flex items-center justify-center mb-2">
							<svg class="w-5 h-5 text-primary-content" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
								<path d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
							</svg>
						</div>
						<h3 class="card-title text-lg">Real scripting</h3>
						<p class="text-sm text-base-content/70">
							Rhai scripting with distributed cache access, metadata propagation, and resource loading. No proprietary DSL.
						</p>
					</div>
				</div>

				<div class="card bg-base-100 shadow-md hover:shadow-lg transition-shadow">
					<div class="card-body">
						<div class="w-10 h-10 bg-primary rounded-lg flex items-center justify-center mb-2">
							<svg class="w-5 h-5 text-primary-content" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
								<path d="M13 10V3L4 14h7v7l9-11h-7z" />
							</svg>
						</div>
						<h3 class="card-title text-lg">Efficient data formats</h3>
						<p class="text-sm text-base-content/70">
							First-class Arrow RecordBatch and Avro support. Columnar data stays in binary format through the entire flow.
						</p>
					</div>
				</div>

				<div class="card bg-base-100 shadow-md hover:shadow-lg transition-shadow">
					<div class="card-body">
						<div class="w-10 h-10 bg-primary rounded-lg flex items-center justify-center mb-2">
							<svg class="w-5 h-5 text-primary-content" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
								<path d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
							</svg>
						</div>
						<h3 class="card-title text-lg">Declarative YAML</h3>
						<p class="text-sm text-base-content/70">
							Flows defined in YAML. Version-controlled, reviewed in pull requests, deployed as ConfigMaps.
						</p>
					</div>
				</div>

				<div class="card bg-base-100 shadow-md hover:shadow-lg transition-shadow">
					<div class="card-body">
						<div class="w-10 h-10 bg-primary rounded-lg flex items-center justify-center mb-2">
							<svg class="w-5 h-5 text-primary-content" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
								<path d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
							</svg>
						</div>
						<h3 class="card-title text-lg">Written in Rust</h3>
						<p class="text-sm text-base-content/70">
							Memory safety, ultra performance, and a mature package ecosystem.
						</p>
					</div>
				</div>

			</div>
		</div>
	</div>
</div>
