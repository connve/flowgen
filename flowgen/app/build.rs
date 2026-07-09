use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let web_dir = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("web");
    let build_dir = web_dir.join("build");
    let package_json = web_dir.join("package.json");
    let lock = web_dir.join("package-lock.json");
    let src_dir = web_dir.join("src");
    let static_dir = web_dir.join("static");

    println!("cargo:rerun-if-changed={}", package_json.display());
    println!("cargo:rerun-if-changed={}", lock.display());
    println!("cargo:rerun-if-changed={}", src_dir.display());
    println!("cargo:rerun-if-changed={}", static_dir.display());
    println!("cargo:rerun-if-env-changed=FLOWGEN_SKIP_WEB_BUILD");

    if std::env::var_os("FLOWGEN_SKIP_WEB_BUILD").is_some() {
        std::fs::create_dir_all(&build_dir).expect("create empty web build dir");
        return;
    }

    let npm = which_npm();
    let node_modules = web_dir.join("node_modules");
    if !node_modules.exists() {
        run(&npm, &["ci"], &web_dir, "npm ci");
    }
    run(&npm, &["run", "build"], &web_dir, "npm run build");

    if !build_dir.exists() {
        panic!(
            "web build did not produce {}; check svelte adapter-static output",
            build_dir.display()
        );
    }
}

fn which_npm() -> String {
    std::env::var("NPM").unwrap_or_else(|_| "npm".to_string())
}

fn run(bin: &str, args: &[&str], cwd: &PathBuf, label: &str) {
    let status = Command::new(bin)
        .args(args)
        .current_dir(cwd)
        .status()
        .unwrap_or_else(|e| {
            panic!(
                "failed to spawn `{bin} {}` in {}: {e}. Install Node.js/npm, or set FLOWGEN_SKIP_WEB_BUILD=1 to skip the embedded UI build.",
                args.join(" "),
                cwd.display()
            )
        });
    if !status.success() {
        panic!("{label} failed with status {status}");
    }
}
