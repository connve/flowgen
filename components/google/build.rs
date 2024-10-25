/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Google gRPC APIs.
    // https://github.com/googleapis/googleapis
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_well_known_types(true)
        .compile(
            &["proto/googleapis/google/storage/v2/storage.proto"],
            &["proto/googleapis"],
        )?;
    Ok(())
}
