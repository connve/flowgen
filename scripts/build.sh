#!/bin/bash

set -e -x
apt update -qq
apt-get -qq install pkg-config libssl-dev protobuf-compiler
apt-get install git
rustup component add rustfmt
git clone https://github.com/googleapis/googleapis components/google/proto
git clone https://github.com/forcedotcom/pub-sub-api components/salesforce/proto
cargo build --verbose
cargo test --verbose
cargo fmt --all
