use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spec_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("openapi.yaml");
    let spec_content = fs::read_to_string(&spec_path)?;
    let spec: openapiv3::OpenAPI = serde_yaml::from_str(&spec_content)?;

    let mut generator = progenitor::Generator::default();
    let tokens = generator.generate_tokens(&spec)?;
    let content = tokens.to_string();

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    fs::write(out_dir.join("generated.rs"), content)?;

    println!("cargo:rerun-if-changed=openapi.yaml");

    Ok(())
}
