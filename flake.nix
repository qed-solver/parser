{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    parser-release = {
      flake = false;
      url = "https://github.com/cosette-solver/cosette-parser/releases/download/latest/cosette-parser-1.0-SNAPSHOT-jar-with-dependencies.jar";
    };
  };

  outputs = { self, nixpkgs, flake-utils, parser-release }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        parser = pkgs.stdenv.mkDerivation {
          name = "cosette-parser";
          src = parser-release;
          buildInputs = with pkgs; [ jre ];
          nativeBuildInputs = with pkgs; [ makeWrapper ];
          buildCommand = ''
            jar=$out/share/java/cosette-parser.jar
            install -Dm444 $src $jar
            makeWrapper ${pkgs.jre}/bin/java $out/bin/cosette-parser --add-flags "--enable-preview -jar $jar"
          '';
        };
      in
      {
        packages.default = parser;
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            jdk
            jetbrains.idea-community
          ];
        };
      });
}
