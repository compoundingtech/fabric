{
  description = "fabric - local socket facade for iroh-backed cross-machine transports";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      # Systems we support (mirrors pty's flake).
      supportedSystems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];

      # Helper to create outputs for each system.
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;

      # Get pkgs for a given system.
      pkgsFor = system: nixpkgs.legacyPackages.${system};

      # The version is the crate's, so it can never go stale here.
      version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package.version;
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = pkgsFor system;

          fabric = pkgs.rustPlatform.buildRustPackage {
            pname = "fabric";
            inherit version;

            src = ./.;

            # All dependencies resolve from crates.io (no git sources), so the
            # committed Cargo.lock is enough — no vendored-deps hash to maintain.
            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            # build.rs stamps FABRIC_BUILD_SHA from git, and the sandbox has no
            # git, so without help the version reads `0.2.33+unknown` and a
            # machine built from Nix cannot be told from any other build of the
            # same release. build.rs also takes the commit from GITHUB_SHA, which
            # is how CI hands it over, so hand it the flake's own revision. It is
            # absent from a dirty tree, which then still reads `unknown`.
            env = pkgs.lib.optionalAttrs (self ? rev) {
              GITHUB_SHA = self.rev;
            };

            # The test suite includes integration tests that dial real iroh over
            # the network, which the sandboxed build cannot reach. The library
            # unit tests are exercised in CI; skip the build-time check here.
            doCheck = false;

            meta = with pkgs.lib; {
              description = "Local socket facade for iroh-backed cross-machine transports";
              homepage = "https://github.com/compoundingtech/fabric";
              mainProgram = "fabric";
              platforms = supportedSystems;
            };
          };
        in
        {
          default = fabric;
          inherit fabric;
        }
      );

      # `nix develop` — a Rust toolchain for hacking on fabric.
      devShells = forAllSystems (system:
        let pkgs = pkgsFor system;
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              rustc
              rustfmt
              clippy
              rust-analyzer
            ];
          };
        }
      );
    };
}
