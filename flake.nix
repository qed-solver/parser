{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        cosette-parser = pkgs.stdenv.mkDerivation {
          name = "cosette-parser";
          src = pkgs.fetchurl {
            url = "https://github.com/cosette-solver/cosette-parser/releases/download/experimental-1e56f42c2208844a980afeff3a25ee4a7010b980/cosette-parser-1.0-SNAPSHOT-jar-with-dependencies.jar";
            sha256 = "sha256-Iut8Apo8PVumjv7VckK8UGzMOcmGzyax3x2hO8NC7vM=";
          };
          buildInputs = with pkgs; [ jre ];
          nativeBuildInputs = with pkgs; [ makeWrapper ];
          buildCommand = ''
          jar=$out/share/java/cosette-parser.jar
          install -Dm444 $src $jar
          makeWrapper ${pkgs.jre}/bin/java $out/bin/cosette-parser --add-flags "--enable-preview -jar $jar"
          '';
        };
      in {
        defaultPackage = cosette-parser;
        devShell = pkgs.mkShell {
          inputsFrom = [ cosette-parser ];
        };
      }
    );
}
