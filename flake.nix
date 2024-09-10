{
  description = "ethereum-validator-watch";
  inputs = {
    devshell = {
      url = "github:numtide/devshell";
      inputs.systems.follows = "systems";
    };
    nixpkgs = { url = "github:NixOS/nixpkgs/nixos-24.05"; };
    systems.url = "github:nix-systems/default";

  };

  outputs = { self, nixpkgs, systems, ... }@inputs:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems) (system:
          f (import nixpkgs {
            inherit system;
            overlays = [ goverlay ];
            config = { allowUnfree = true; };
          }));
      version = "1.22.4";
      goverlay = final: prev: {
        go = prev.go.overrideAttrs (old: {
          inherit version;
          src = final.fetchurl {
            url = "https://go.dev/dl/go${version}.src.tar.gz";
            sha256 = "sha256-/tcgZ45yinyjC6jR3tHKr+J9FgKPqwIyuLqOIgCPt4Q=";
          };
        });
      };

    in {
      devShells = eachSystem (pkgs: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          packages = [
            pkgs.go
            pkgs.gopls
            pkgs.gops
            pkgs.go-tools
            pkgs.tmux
            pkgs.nodejs_20
            pkgs.python39Full
          ];
        };
      });
    };
}

