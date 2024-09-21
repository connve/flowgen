#!/bin/bash

set -e -x
apt update -qq
apt-get -qq install pkg-config libssl-dev protobuf-compiler
apt-get -qq install git
rustup component add rustfmt
rm -r components/google/proto && git clone https://github.com/googleapis/googleapis components/google/proto/googleapis
rm -r components/salesforce/proto && git clone https://github.com/forcedotcom/pub-sub-api components/salesforce/proto/pubsub
cargo build --verbose
cargo test --verbose
cargo fmt --all
