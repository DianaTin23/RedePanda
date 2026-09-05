{
  description = "RedeTim — .NET 10 terminal chat over Redpanda (Kafka-compatible)";

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
            pkgs.dotnet-sdk_10
            pkgs.redpanda-client # provides rpk
            pkgs.docker-compose
            pkgs.kubectl
            pkgs.kubernetes-helm
            # Validates rendered manifests against the Kubernetes schemas without a cluster.
            # `kubectl apply --dry-run=client` cannot do this: it still needs an API server to
            # resolve resource kinds, so it fails with "connection refused" when none is running.
            pkgs.kubeconform
            # Reads a registry manifest without pulling the image, which is how
            # scripts/check-digests.sh resolves the digests that the Dockerfiles and the chart pin.
            pkgs.skopeo
            # The Claude Code hooks in .claude/hooks/ read the tool payload from stdin as JSON.
            # Not needed to build, test or deploy the project; missing on a bare NixOS host.
            pkgs.jq
          ];

          DOTNET_CLI_TELEMETRY_OPTOUT = 1;
          DOTNET_NOLOGO = 1;

          shellHook = ''
            echo "RedeTim dev shell — .NET $(dotnet --version); rpk, kubectl, helm on PATH"
            echo "  broker:  cd RedeTim-kafka-docker && docker compose --env-file env.local up -d"
            echo "  topics:  rpk topic list -X brokers=localhost:19092"
            echo "  chat:    REDPANDA_BOOTSTRAP_SERVERS=localhost:19092 \\"
            echo "           dotnet run --project src/RedeTim.ChatClient -- --nick alice"
            echo "  build:   ./scripts/build-images.sh"
            echo "  chart:   helm template redetim deploy/helm/redetim -f deploy/releases/<version>.yaml"
          '';
        };
      });
    };
}
