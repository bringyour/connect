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
            config = { allowUnfree = true; };
          }));

    in {
      devShells = eachSystem (pkgs: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          packages = [
            pkgs.go_1_22
            pkgs.goda
            pkgs.gopls
            pkgs.gops
            pkgs.go-tools
            pkgs.tmux
            pkgs.protobuf
            pkgs.protoc-gen-go
          ];
        };
      });
    };
}

