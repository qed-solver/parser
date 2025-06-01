#!/bin/bash

# Build qed-prover with Rust nightly

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly
source $HOME/.cargo/env

git clone https://github.com/qed-solver/prover.git qed-prover
cd qed-prover
cargo +nightly build --release