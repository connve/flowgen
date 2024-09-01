#!/bin/bash

set -e -x
apt update -qq
apt-get -qq install pkg-config libssl-dev protobuf-compiler
apt-get install git
rustup component add rustfmt
cd components/google/proto && git clone https://github.com/googleapis/googleapis
cd components/salesforce/proto && git clone https://github.com/forcedotcom/pub-sub-api
cargo build --verbose
cargo test --verbose
cargo fmt --all
