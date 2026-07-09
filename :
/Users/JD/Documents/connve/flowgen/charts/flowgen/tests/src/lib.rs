//! Helm chart tests for the flowgen chart.
//!
//! These tests run `helm template` against the local chart and assert on
//! the rendered Kubernetes manifests. They require `helm` to be installed
//! and available on `PATH`.

use serde::Deserialize;
use std::process::Command;

/// Top-level Helm template output. Each document is a Kubernetes manifest.
#[derive(Debug, Deserialize, Clone)]
struct Manifest {
    api_version: String,
    kind: String,
    metadata: Option<Metadata>,
    spec: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct Metadata {
    name: String,
}

/// Renders the chart with the given values and returns all manifests.
fn render(_values: &str) -> Vec<Manifest> {
    let output = Command::new("helm")
        .args([
            "template",
            "flowgen",
            "charts/flowgen",
            "--values",
            "-",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("helm should be installed and executable")
        .wait_with_output()
        .expect("helm template should run");

    assert!(
        output.status.success(),
        "helm template failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let docs = serde_yaml::Deserializer::from_str(&stdout);
    let mut manifests = Vec::new();
    for doc in docs {
        let manifest: Manifest = serde_yaml::Value::deserialize(doc)
            .and_then(serde_yaml::from_value)
            .expect("rendered manifest should be valid YAML");
        manifests.push(manifest);
    }
    manifests
}

fn find_deployment(manifests: &[Manifest]) -> Manifest {
    manifests
        .iter()
        .find(|m| m.kind == "Deployment")
        .expect("chart should render a Deployment")
        .clone()
}

#[test]
fn default_deployment_renders_without_host_network() {
    let manifests = render("flowgen:\n");
    let deployment = find_deployment(&manifests);
    let spec = deployment.spec.expect("Deployment should have a spec");

    assert!(
        spec.get("hostNetwork").is_none(),
        "default pod spec should not contain hostNetwork"
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
    let spec = deployment
        .spec
        .expect("Deployment should have a spec")
        .as_mapping()
        .expect("pod spec should be a mapping")
        .clone();

    let host_network = spec
        .get(&serde_yaml::Value::String("hostNetwork".to_string()))
        .and_then(|v| v.as_bool())
        .expect("hostNetwork should be a boolean");
    assert!(host_network, "hostNetwork should be true");

    let dns_policy = spec
        .get(&serde_yaml::Value::String("dnsPolicy".to_string()))
        .and_then(|v| v.as_str())
        .expect("dnsPolicy should be a string");
    assert_eq!(dns_policy, "ClusterFirstWithHostNet");
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
    let spec = deployment
        .spec
        .expect("Deployment should have a spec")
        .as_mapping()
        .expect("pod spec should be a mapping")
        .clone();

    let host_network = spec
        .get(&serde_yaml::Value::String("hostNetwork".to_string()))
        .and_then(|v| v.as_bool())
        .expect("hostNetwork should be a boolean");
    assert!(host_network, "hostNetwork should be true after patch");
}
