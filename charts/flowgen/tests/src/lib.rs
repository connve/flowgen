//! Helm chart tests for the flowgen chart.
//!
//! These tests run `helm template` against the local chart and assert on
//! the rendered Kubernetes manifests. They require `helm` to be installed
//! and available on `PATH`.

#![cfg(test)]

use serde::Deserialize;
use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Deserialize)]
struct Manifest {
    kind: String,
    metadata: Option<serde_yaml::Value>,
    spec: Option<serde_yaml::Value>,
    data: Option<serde_yaml::Value>,
}

/// The `config.yaml` the chart renders, as flowgen loads it.
fn app_config(manifests: &[Manifest]) -> flowgen::config::AppConfig {
    let yaml = manifests
        .iter()
        .find(|m| {
            m.kind == "ConfigMap"
                && m.data
                    .as_ref()
                    .is_some_and(|d| d.get("config.yaml").is_some())
        })
        .and_then(|m| m.data.as_ref())
        .and_then(|d| d.get("config.yaml"))
        .and_then(|v| v.as_str())
        .expect("chart should render config.yaml")
        .to_string();
    flowgen::config::AppConfig::load(
        config::File::from_str(&yaml, config::FileFormat::Yaml),
        config::Environment::with_prefix("FLOWGEN_CHART_TEST_UNSET"),
    )
    .expect("flowgen should load the rendered config")
}

fn env_value(container: &serde_yaml::Value, name: &str) -> Option<String> {
    container
        .get("env")
        .and_then(|e| e.as_sequence())?
        .iter()
        .find(|e| e.get("name").and_then(|n| n.as_str()) == Some(name))
        .and_then(|e| e.get("value"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

#[test]
fn the_default_config_loads_in_flowgen() {
    let config = app_config(&render(""));
    assert!(config.flows.cache.is_some_and(|cache| cache.enabled));
    assert!(!config.flows.system);
}

#[test]
fn system_flows_run_from_flowgen_with_their_variables_and_credentials() {
    let manifests = render(
        r#"flowgen:
  systemFlows:
    enabled: true
    repository: https://git.example.com/a.git
    workspacePath: workspace/
    releases: registry.example.com/a
    gitCredentials:
      secretName: git
    registryCredentials:
      secretName: registry
"#,
    );
    assert!(app_config(&manifests).flows.system);

    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);
    let container = container(pod);
    for (name, value) in [
        ("FLOWGEN_REPOSITORY", "https://git.example.com/a.git"),
        ("FLOWGEN_WORKSPACE_PATH", "workspace/"),
        ("FLOWGEN_RELEASES", "registry.example.com/a"),
        (
            "FLOWGEN_GIT_CREDENTIALS",
            "/etc/flowgen/system-flows/git/git.json",
        ),
        (
            "FLOWGEN_REGISTRY_CREDENTIALS",
            "/etc/flowgen/system-flows/registry/registry.json",
        ),
    ] {
        assert_eq!(env_value(container, name).as_deref(), Some(value), "{name}");
    }
    let secrets: Vec<&str> = pod
        .get("volumes")
        .and_then(|v| v.as_sequence())
        .expect("pod should have volumes")
        .iter()
        .filter_map(|v| v.get("secret")?.get("secretName")?.as_str())
        .collect();
    assert!(
        secrets.contains(&"git") && secrets.contains(&"registry"),
        "{secrets:?}"
    );
}

#[test]
fn system_flows_require_a_repository_and_releases() {
    for (missing, values) in [
        ("repository", "releases: registry.example.com/a"),
        ("releases", "repository: https://git.example.com/a.git"),
    ] {
        let output = helm_template(&format!(
            "flowgen:\n  systemFlows:\n    enabled: true\n    {values}\n"
        ));
        assert!(!output.status.success(), "{missing}");
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("flowgen.systemFlows.{missing} is required")),
            "{missing}"
        );
    }
}

fn config_checksum(values: &str) -> String {
    let spec = find_deployment(&render(values))
        .spec
        .expect("Deployment should have a spec");
    spec["template"]["metadata"]["annotations"]["checksum/config"]
        .as_str()
        .expect("pod should have a checksum/config annotation")
        .to_string()
}

#[test]
fn a_settings_change_changes_the_config_checksum() {
    assert_ne!(
        config_checksum(""),
        config_checksum("flowgen:\n  config:\n    settings:\n      health:\n        port: 9091\n")
    );
}

#[test]
fn health_disabled_renders_without_probes_or_port() {
    let manifests = render(
        r#"flowgen:
  config:
    settings:
      health:
        enabled: false
"#,
    );
    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));
    for probe in ["startupProbe", "readinessProbe", "livenessProbe"] {
        assert!(container.get(probe).is_none(), "{probe}");
    }
    assert!(!container["ports"]
        .as_sequence()
        .expect("container should declare ports")
        .iter()
        .any(|p| p["name"] == "health"));
}

fn render(values: &str) -> Vec<Manifest> {
    let output = helm_template(values);
    assert!(
        output.status.success(),
        "helm template failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("helm output is utf-8");
    let mut manifests = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(&stdout) {
        let value = serde_yaml::Value::deserialize(doc).expect("rendered doc is valid YAML");
        if value.is_null() {
            continue;
        }
        let manifest: Manifest =
            serde_yaml::from_value(value).expect("rendered manifest matches schema");
        manifests.push(manifest);
    }
    manifests
}

fn helm_template(values: &str) -> std::process::Output {
    let chart_dir = env!("CARGO_MANIFEST_DIR");
    let chart_path = std::path::Path::new(chart_dir)
        .parent()
        .expect("tests dir has a parent");

    let mut child = Command::new("helm")
        .args(["template", "flowgen"])
        .arg(chart_path)
        .args(["--values", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("helm should be installed and executable");

    child
        .stdin
        .as_mut()
        .expect("helm stdin piped")
        .write_all(values.as_bytes())
        .expect("write values to helm stdin");

    child.wait_with_output().expect("helm template should run")
}

fn find_deployment(manifests: &[Manifest]) -> Manifest {
    manifests
        .iter()
        .find(|m| m.kind == "Deployment")
        .expect("chart should render a Deployment")
        .clone()
}

// podTemplate.merge / podTemplate.patch land in spec.template.spec, not spec.
fn pod_spec(deployment: &Manifest) -> &serde_yaml::Value {
    deployment
        .spec
        .as_ref()
        .expect("Deployment should have a spec")
        .get("template")
        .expect("Deployment spec should have a template")
        .get("spec")
        .expect("pod template should have a spec")
}

#[test]
fn listed_credentials_are_mounted_together_read_only() {
    let manifests = render("flowgen:\n  credentials: [a, \"no\"]\n");
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);
    let container = &pod["containers"][0];
    let volume = pod["volumes"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|volume| volume["name"] == "flowgen-credentials")
        .expect("a credentials volume");
    let mount = container["volumeMounts"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|mount| mount["name"] == "flowgen-credentials")
        .expect("a credentials mount");
    let sources = volume["projected"]["sources"].as_sequence().unwrap();
    let secrets: Vec<&str> = sources
        .iter()
        .map(|source| source["secret"]["name"].as_str().unwrap())
        .collect();

    assert_eq!(secrets, ["a", "no"]);
    assert!(sources
        .iter()
        .all(|source| source["secret"]["optional"] == true));
    assert_eq!(mount["readOnly"], true);
    assert_eq!(
        env_value(container, "FLOWGEN_CREDENTIALS").as_deref(),
        mount["mountPath"].as_str()
    );
    let none = render("");
    let pod = pod_spec(&find_deployment(&none)).clone();
    assert!(!pod["volumes"]
        .as_sequence()
        .unwrap()
        .iter()
        .any(|volume| volume["name"] == "flowgen-credentials"));
}

#[test]
fn extra_volumes_are_added_to_the_pod_as_they_are() {
    let manifests = render(
        "flowgen:\n  extraVolumes:\n    - name: workspace\n      persistentVolumeClaim:\n        claimName: data\n  extraVolumeMounts:\n    - name: workspace\n      mountPath: /workspace\n",
    );
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);
    let volume = pod["volumes"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|volume| volume["name"] == "workspace")
        .expect("the extra volume");
    let mount = pod["containers"][0]["volumeMounts"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|mount| mount["name"] == "workspace")
        .expect("the extra mount");

    assert_eq!(volume["persistentVolumeClaim"]["claimName"], "data");
    assert_eq!(mount["mountPath"], "/workspace");
}

#[test]
fn default_deployment_renders_without_host_network() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);

    assert!(
        pod.get("hostNetwork").is_none(),
        "default pod spec should not contain hostNetwork, got: {pod:?}"
    );
}

#[test]
fn merge_injects_host_network_and_dns_policy() {
    let manifests = render(
        r#"flowgen:
  podTemplate:
    merge:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
"#,
    );
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);

    let host_network = pod
        .get("hostNetwork")
        .and_then(|v| v.as_bool())
        .expect("hostNetwork should be a boolean");
    assert!(host_network, "hostNetwork should be true");

    let dns_policy = pod
        .get("dnsPolicy")
        .and_then(|v| v.as_str())
        .expect("dnsPolicy should be a string");
    assert_eq!(dns_policy, "ClusterFirstWithHostNet");
}

fn service_named(manifests: &[Manifest], suffix: &str) -> Option<Manifest> {
    manifests
        .iter()
        .find(|m| {
            m.kind == "Service"
                && m.metadata
                    .as_ref()
                    .and_then(|md| md.get("name"))
                    .and_then(|n| n.as_str())
                    .is_some_and(|n| n.ends_with(suffix))
        })
        .cloned()
}

fn service_port(svc: &Manifest) -> u64 {
    svc.spec
        .as_ref()
        .and_then(|s| s.get("ports"))
        .and_then(|p| p.as_sequence())
        .and_then(|seq| seq.first())
        .and_then(|p| p.get("port"))
        .and_then(|v| v.as_u64())
        .expect("service port should be numeric")
}

#[test]
fn ai_gateway_service_renders_when_configured() {
    let manifests = render(
        r#"flowgen:
  config:
    settings:
      ai_gateway:
        enabled: true
        port: 3002
"#,
    );
    let svc = service_named(&manifests, "-ai-gateway").expect("ai-gateway Service should render");
    assert_eq!(service_port(&svc), 3002);
}

#[test]
fn ai_gateway_service_omitted_by_default() {
    let manifests = render("");
    assert!(service_named(&manifests, "-ai-gateway").is_none());
}

#[test]
fn web_service_renders_when_configured() {
    let manifests = render(
        r#"flowgen:
  config:
    settings:
      web:
        enabled: true
        port: 8080
  services:
    web:
      type: LoadBalancer
"#,
    );
    let svc = service_named(&manifests, "-web").expect("web Service should render");
    assert_eq!(service_port(&svc), 8080);
    assert_eq!(
        svc.spec
            .as_ref()
            .and_then(|s| s.get("type"))
            .and_then(|t| t.as_str()),
        Some("LoadBalancer")
    );
}

#[test]
fn web_service_omitted_by_default() {
    let manifests = render("");
    assert!(service_named(&manifests, "-web").is_none());
}

#[test]
fn patch_adds_host_network() {
    let manifests = render(
        r#"flowgen:
  podTemplate:
    patch:
      - op: add
        path: /hostNetwork
        value: true
"#,
    );
    let deployment = find_deployment(&manifests);
    let pod = pod_spec(&deployment);

    let host_network = pod
        .get("hostNetwork")
        .and_then(|v| v.as_bool())
        .expect("hostNetwork should be a boolean");
    assert!(host_network, "hostNetwork should be true after patch");
}

fn container(pod: &serde_yaml::Value) -> &serde_yaml::Value {
    pod.get("containers")
        .and_then(|v| v.as_sequence())
        .and_then(|s| s.first())
        .expect("pod spec should have at least one container")
}

/// Regression: `http_server` is `Option<HttpServerOptions>` in the app
/// config, so removing it from the settings must render a valid chart
/// without the `http` port or its Service.
#[test]
fn http_server_disabled_renders_without_port_or_service() {
    let manifests = render(
        r#"flowgen:
  config:
    settings:
      http_server: null
"#,
    );

    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));
    let ports = container
        .get("ports")
        .and_then(|v| v.as_sequence())
        .expect("container should still declare ports (health at minimum)");
    assert!(
        !ports
            .iter()
            .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("http")),
        "container should not declare an `http` port when http_server is null, got: {ports:?}"
    );

    assert!(
        service_named(&manifests, "-http-server").is_none(),
        "http-server Service should not render when http_server is null"
    );
}

#[test]
fn probes_point_to_dedicated_health_port() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));

    let health_port = container
        .get("ports")
        .and_then(|v| v.as_sequence())
        .expect("container should declare ports")
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("health"))
        .expect("container should expose a `health` port");
    assert_eq!(
        health_port.get("containerPort").and_then(|v| v.as_u64()),
        Some(8081),
        "health port should default to 8081"
    );

    let readiness = container
        .get("readinessProbe")
        .and_then(|p| p.get("httpGet"))
        .expect("container should have a readinessProbe httpGet");
    assert_eq!(
        readiness.get("path").and_then(|v| v.as_str()),
        Some("/readyz")
    );
    assert_eq!(
        readiness.get("port").and_then(|v| v.as_str()),
        Some("health")
    );

    let liveness = container
        .get("livenessProbe")
        .and_then(|p| p.get("httpGet"))
        .expect("container should have a livenessProbe httpGet");
    assert_eq!(
        liveness.get("path").and_then(|v| v.as_str()),
        Some("/livez")
    );
    assert_eq!(
        liveness.get("port").and_then(|v| v.as_str()),
        Some("health")
    );
}

/// Every probe sets `timeoutSeconds`, and a startupProbe exists.
///
/// Leaving `timeoutSeconds` unset is the bug worth catching: Kubernetes
/// defaults it to 1s, so a worker merely busy enough to delay the handler
/// gets restarted, losing in-flight work and shifting load onto replicas
/// that then time out too. What the values should be is a deployment
/// decision — this only asserts they were made deliberately.
#[test]
fn probes_set_their_timeout_explicitly() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let container = container(pod_spec(&deployment));

    for probe in ["startupProbe", "readinessProbe", "livenessProbe"] {
        let value = container
            .get(probe)
            .unwrap_or_else(|| panic!("container should have a {probe}"));
        assert!(
            value
                .get("timeoutSeconds")
                .and_then(|v| v.as_u64())
                .is_some(),
            "{probe} must set timeoutSeconds; unset means Kubernetes' 1s default"
        );
    }
}

fn network_policy(manifests: &[Manifest]) -> Option<Manifest> {
    manifests
        .iter()
        .find(|m| m.kind == "NetworkPolicy")
        .cloned()
}

fn rule_ports(rule: &serde_yaml::Value) -> Vec<u64> {
    rule.get("ports")
        .and_then(|p| p.as_sequence())
        .expect("ingress rule should list ports")
        .iter()
        .map(|p| {
            p.get("port")
                .and_then(|v| v.as_u64())
                .expect("port should be numeric")
        })
        .collect()
}

#[test]
fn cluster_port_declared_without_network_policy_by_default() {
    let manifests = render("");
    let deployment = find_deployment(&manifests);
    let cluster_port = container(pod_spec(&deployment))
        .get("ports")
        .and_then(|v| v.as_sequence())
        .expect("container should declare ports")
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("cluster"))
        .and_then(|p| p.get("containerPort"))
        .and_then(|v| v.as_u64());

    assert_eq!(cluster_port, Some(8082));
    assert!(network_policy(&manifests).is_none());
}

#[test]
fn cluster_network_policy_limits_cluster_port_to_flowgen_pods() {
    let manifests = render(
        r#"flowgen:
  config:
    settings:
      web:
        enabled: true
        port: 8080
  cluster:
    networkPolicy: true
"#,
    );
    let deployment = find_deployment(&manifests);
    let cluster_port = container(pod_spec(&deployment))
        .get("ports")
        .and_then(|v| v.as_sequence())
        .expect("container should declare ports")
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some("cluster"))
        .and_then(|p| p.get("containerPort"))
        .and_then(|v| v.as_u64());
    let policy = network_policy(&manifests).expect("NetworkPolicy should render");
    let rules = policy
        .spec
        .as_ref()
        .and_then(|s| s.get("ingress"))
        .and_then(|i| i.as_sequence())
        .expect("NetworkPolicy should have ingress rules")
        .clone();

    assert_eq!(cluster_port, Some(8082));
    assert_eq!(rules.len(), 2);
    assert_eq!(rule_ports(&rules[0]), vec![8082]);
    assert!(rules[0].get("from").is_some());
    assert_eq!(rule_ports(&rules[1]), vec![3000, 8080, 8081]);
    assert!(rules[1].get("from").is_none());
}

#[test]
fn cluster_port_is_the_telemetry_backend_port() {
    let manifests = render(
        r#"flowgen:
  config:
    settings:
      telemetry:
        enabled: true
        backend:
          type: memory
          port: 9092
  cluster:
    networkPolicy: true
"#,
    );
    let deployment = find_deployment(&manifests);
    let cluster_port = container(pod_spec(&deployment))["ports"]
        .as_sequence()
        .expect("container should declare ports")
        .iter()
        .find(|p| p["name"] == "cluster")
        .and_then(|p| p["containerPort"].as_u64());
    let policy = network_policy(&manifests).expect("NetworkPolicy should render");
    let spec = policy.spec.expect("NetworkPolicy should have a spec");

    assert_eq!(cluster_port, Some(9092));
    assert_eq!(rule_ports(&spec["ingress"][0]), vec![9092]);
}
