# Build context is the repository root, because the client references RedePanda.Contracts.
#   podman build -f src/RedePanda.ChatClient/Dockerfile -t redepanda-chatclient:dev .
#
# This image exists for two reasons. It is the console client as a container, so the same binary
# runs the same way locally and in the cluster; and it is what the chart's topic Job runs with
# --ensure-topic. That Job used to be a shell script in the Redpanda image -- a second codebase and
# a second configuration source for a task belonging to this application (12-Factor XII).

# Same SDK pin as the backend: rollForward is "latestPatch" against global.json, so a different
# feature band would not satisfy the request. Refresh with scripts/check-digests.sh.
FROM mcr.microsoft.com/dotnet/sdk:10.0.302@sha256:72dd743782f2ae7e5476fd64f6a460045e3998dc862218b80e6944cba79a01b0 AS build
WORKDIR /src

# Restore before the sources so a code change does not invalidate the package layer. The lock
# files travel with their .csproj because --locked-mode reads them during that same restore.
COPY global.json NuGet.config Directory.Build.props Directory.Packages.props ./
COPY src/RedePanda.Contracts/RedePanda.Contracts.csproj src/RedePanda.Contracts/packages.lock.json src/RedePanda.Contracts/
COPY src/RedePanda.ChatClient/RedePanda.ChatClient.csproj src/RedePanda.ChatClient/packages.lock.json src/RedePanda.ChatClient/

RUN dotnet restore src/RedePanda.ChatClient/RedePanda.ChatClient.csproj --locked-mode

COPY src/RedePanda.Contracts/ src/RedePanda.Contracts/
COPY src/RedePanda.ChatClient/ src/RedePanda.ChatClient/
RUN dotnet publish src/RedePanda.ChatClient/RedePanda.ChatClient.csproj \
    -c Release -o /app --no-restore

# runtime rather than aspnet: this is a console application and never binds a port.
#
# Debian-based for the same reason as the backend, and it is not optional here either: this client
# is a Kafka client, Confluent.Kafka carries native librdkafka assets built against glibc, and a
# musl or chiselled base fails at the first AdminClientBuilder.Build() -- at runtime, inside a Helm
# hook, which is the worst place to discover it.
FROM mcr.microsoft.com/dotnet/runtime:10.0@sha256:acad02eb5c4fbf57d15296f9c08d56cd4036e915bdae5b4dd48a06523d452617 AS runtime
WORKDIR /app
COPY --from=build /app .

# Numeric, from the base image: Kubernetes' runAsNonRoot check rejects a named USER.
USER $APP_UID

ENV DOTNET_RUNNING_IN_CONTAINER=true

ENTRYPOINT ["dotnet", "RedePanda.ChatClient.dll"]
