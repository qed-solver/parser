{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    cvc5-src = {
      flake = false;
      url = "github:cvc5/cvc5";
    };
  };

  outputs = { self, nixpkgs, flake-utils, cvc5-src }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        cvc5-pname = "cvc5-1.0.5";
        cvc5-java = pkgs.stdenv.mkDerivation {
          name = cvc5-pname;
          src = cvc5-src;

          nativeBuildInputs = with pkgs; [ pkg-config cmake flex ];

          buildInputs = with pkgs; [
            cadical.dev
            symfpu
            gmp
            gtest
            libantlr3c
            antlr3_4
            boost
            jdk
            (python3.withPackages (ps: with ps; [ pyparsing toml ]))
          ];

          cmakeFlags = [
            "-DBUILD_BINDINGS_JAVA=ON"
            "-DBUILD_SHARED_LIBS=1"
            "-DCMAKE_BUILD_TYPE=Production"
            "-DANTLR3_JAR=${pkgs.antlr3_4}/lib/antlr/antlr-3.4-complete.jar"
          ];

          preConfigure = ''
            patchShebangs ./src/
          '';

        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cvc5-java
            jdk
            jetbrains.idea-community
          ];
          CVC5_JAVA="${cvc5-java}/share/java/${cvc5-pname}.jar";
        };
      });
}
