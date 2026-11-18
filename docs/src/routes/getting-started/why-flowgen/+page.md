---
description: Why we built Flowgen, an open-source composable data activation platform. Turn your data into the next best action!
---

# Why Flowgen

Flowgen is a composable data activation platform. Let's break it down.

First, how it started. We worked with many sales and marketing teams who had the same problem: they wanted to activate their data across data warehouses, SaaS apps, internal APIs and channels, in an easy and reliable way, and focus on business logic rather than technical plumbing. But in almost every case they ended up copying data from system A to system B, or spending thousands on reverse ETL tools that were heavy, hard to manage and built around a batch run every so often.

We thought there must be a better way. We looked at open source solutions, and while there were options, none we could pick without compromise: paywalls for basic features, complex architectures with workers and databases, or event streaming as an afterthought.

So we decided to build it :) We chose Rust because it matches our principles: memory safety means fewer bugs, it's fast, and it's very expressive. It was also exciting to do. That's how our journey started in [2024](https://github.com/connve/flowgen/commit/fb26fde93836f4ffe8e52ef1cc5612a1c739c8d8).

Fast forward to now: we just shipped 150+ releases, and we're on a mission to build the best data activation platform out there, because data is the key to every decision and move a business makes!

Here's what we like the most about Flowgen, in no particular order:

- **It's lightweight.** One binary and no database. State sits in an embedded file on one machine, or in a distributed cache when you run many.
- **It runs anywhere.** On a single machine or on Kubernetes. We run it on millions of events across several organizations, and it gets better with every release.
- **Reliability is built in.** Backpressure and automatic retries with jitter, so you don't have to think about them.
- **It's easy to operate.** You configure flows in YAML and put custom logic in embedded scripts.
- **It has a UI.** Show your team what you're building, or debug your flows.
- **It's enterprise ready.** A growing number of connectors to enterprise SaaS, single sign-on, and OpenTelemetry logs and metrics.
- **It's built on Git.** Flows and resources live in a repository, so you manage everything the GitOps way. Trust us, you'll love it (we do).
- **It's free.** MPL-2.0, no enterprise edition, no paywall, and we're committed to keeping it that way. CONNVE, the company behind Flowgen, offers managed versions and services, but the engine is yours to use. If you do, please tell us!

So what's with "composable"? Flowgen doesn't own your data or replace your stack. It sits between the systems you already have, your warehouse, your CRM, your marketing tools, and you compose data flows. A flow can run on a schedule, react to events, serve an HTTP endpoint or become a tool for your agents.

And is it agentic? Everything is agentic these days, isn't it? Well, we made sure we walk the walk:

- **Agents build, people approve.** Flowgen Agent in the UI, or [the coding agent you already use](/docs/flowgen/getting-started/agent-setup), proposes changes to your data flows or resources. Nothing goes live until somebody approves it.
- **MCP server.** Turn any flow, whether a query, an API call or a whole pipeline, into a tool your agents can run, on your rules, with your business context and one place for logs.
- **AI Gateway.** One OpenAI- and Anthropic-compatible endpoint for your apps' model calls. Each request runs through a flow, so you can log, enrich or check it on the way.
- **AI completions.** Call models from any flow: OpenAI, Anthropic, Gemini, Mistral, local models through Ollama, or any OpenAI-compatible endpoint.
- **Agent sessions.** Chat with an agent or run MCP tools from the Flowgen UI, to test or simply to work with the platform.

Try it out and tell us what you think! We can't wait to hear from you.

<div class="mt-8 h-12 w-12 overflow-hidden rounded-full">
  <img src="https://connve.com/jakub-dulny.jpg" alt="Jakub Dulny" class="h-full w-full object-cover" style="object-position: center 15%; transform: scale(2)" />
</div>

Jakub & the CONNVE team
