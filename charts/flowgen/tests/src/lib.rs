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
    spec: Option<serde_yaml::Value>,
}

fn render(values: &str) -> Vec<Manifest> {
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

    let output = child.wait_with_output().expect("helm template should run");

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
