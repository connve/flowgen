// Shared alias-to-provider routing for both openai_proxy.yaml and
// anthropic_proxy.yaml gateway flows.
//
// Reads the requested `event.data.model` alias and rewrites the
// downstream endpoint, model, and credentials path used by the
// `ai_completion` task. Also forces a default system prompt so
// upstream providers that reject empty content (e.g. Moonshot 400
// "text content is empty") don't fail when the client omits `system`.
//
// Aliases: zai / glm-4.6, kimi / kimi-k2. Unknown aliases fall back
// to kimi and record `routing_fallback` so the miss is visible in
// logs.

let alias = event.data.model;

if alias == "zai" || alias == "glm-4.6" {
    event.data.endpoint = "https://api.z.ai/api/paas/v4";
    event.data.routed_model = "glm-4.6";
    event.data.credentials_path = "/etc/zai/credentials.json";
} else if alias == "kimi" || alias == "kimi-k2" {
    event.data.endpoint = "https://api.moonshot.ai/v1";
    event.data.routed_model = "kimi-k2";
    event.data.credentials_path = "/etc/kimi/credentials.json";
} else {
    event.data.endpoint = "https://api.moonshot.ai/v1";
    event.data.routed_model = "kimi-k2";
    event.data.credentials_path = "/etc/kimi/credentials.json";
    event.data.routing_fallback = alias;
}

// Force a default so the request goes through when the client
// omits `system`.
if type_of(event.data.system_prompt) != "string" || event.data.system_prompt == "" {
    event.data.system_prompt = "You are a helpful assistant.";
}

event
