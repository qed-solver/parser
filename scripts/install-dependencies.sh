#!/bin/bash

# Install required dependencies for qed-prover

sudo apt-get update
sudo apt-get install -y jq z3

# Install cvc5 - try package manager first, otherwise build from source
sudo apt-get install -y cvc5 || (
    sudo apt-get install -y cmake libgmp-dev &&
    git clone --depth 1 https://github.com/cvc5/cvc5.git &&
    cd cvc5 &&
    ./configure.sh --auto-download &&
    cd build &&
    make -j$(nproc) &&
    sudo make install
)