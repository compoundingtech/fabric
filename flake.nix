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
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = pkgsFor system;

          fabric = pkgs.rustPlatform.buildRustPackage {
            pname = "fabric";
            version = "0.2.0";

            src = ./.;

            # All dependencies resolve from crates.io (no git sources), so the
            # committed Cargo.lock is enough — no vendored-deps hash to maintain.
            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            # build.rs stamps FABRIC_BUILD_SHA from git; the nix build has no git,
            # so it falls back to "unknown" (version reads e.g. `0.2.0+unknown`).

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
