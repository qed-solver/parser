{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    parser-release = {
      flake = false;
      url = "https://github.com/qed-solver/parser/releases/download/latest/qed-parser-1.0-SNAPSHOT-jar-with-dependencies.jar";
    };
  };

  outputs = { self, nixpkgs, flake-utils, parser-release }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        parser = pkgs.stdenv.mkDerivation {
          name = "qed-parser";
          src = parser-release;
          buildInputs = with pkgs; [ jre ];
          nativeBuildInputs = with pkgs; [ makeWrapper ];
          buildCommand = ''
            jar=$out/share/java/qed-parser.jar
            install -Dm444 $src $jar
            makeWrapper ${pkgs.jre}/bin/java $out/bin/qed-parser --add-flags "--enable-preview --add-opens=java.base/java.lang.reflect=ALL-UNNAMED -jar $jar"
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
