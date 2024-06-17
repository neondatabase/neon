{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    devenv.url = "github:cachix/devenv";
  };

  outputs = inputs @ {
    flake-parts,
    nixpkgs,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      imports = [
        inputs.devenv.flakeModule
      ];
      systems = nixpkgs.lib.systems.flakeExposed;

      perSystem = {
        config,
        self',
        inputs',
        pkgs,
        system,
        lib,
        ...
      }: {
        devenv.shells.default = {
          packages = with pkgs; [
            rustup
            postgresql_16
            protobuf_26
            icu74
            pkg-config
            bison
            flex
            openssl
            libiconv
            readline
            zlib
            curl
          ];

          env.DISABLE_HOMEBREW = "1";

          scripts = {
            neonmake = {
              description = "Build Neon";

              exec =
                if (pkgs.stdenv.isDarwin)
                then "make -j`sysctl -n hw.logicalcpu` -s"
                else "make -j`nproc` -s";
            };
          };
        };
      };
    };
}
