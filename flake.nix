{
  description = "RedePanda — .NET 9 terminal chat over Redpanda (Kafka-compatible)";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      # rpk ships under the Business Source License, which nixpkgs classifies as
      # unfree. Allow that one package rather than unfree as a whole.
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              config.allowUnfreePredicate =
                pkg: builtins.elem (nixpkgs.lib.getName pkg) [ "redpanda-rpk" ];
            }
          )
        );
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.dotnet-sdk_9
            pkgs.redpanda-client # provides rpk
            pkgs.docker-compose
          ];

          DOTNET_CLI_TELEMETRY_OPTOUT = 1;
          DOTNET_NOLOGO = 1;

          shellHook = ''
            echo "RedePanda dev shell — .NET $(dotnet --version), rpk on PATH"
            echo "  broker:  cd RedePanda-kafka-docker && docker compose --env-file env.local up -d"
            echo "  topics:  rpk topic list --brokers localhost:19092"
            echo "  chat:    cd RedePanda-chat-client && dotnet run -- local --nick alice --topic newChat"
          '';
        };
      });
    };
}
