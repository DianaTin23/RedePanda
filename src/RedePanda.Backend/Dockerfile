# Build context is the repository root, because the backend references RedePanda.Contracts.
#   podman build -f src/RedePanda.Backend/Dockerfile -t redepanda-backend:dev .

# Pinned to the same SDK feature band as the Nix dev shell (see flake.nix and global.json):
# rollForward is "latestPatch", so a 10.0.4xx image would not satisfy a 10.0.302 request. The
# digest is what actually resolves; the tag is kept only so a human can read the version.
# Refresh both together with scripts/check-digests.sh.
FROM mcr.microsoft.com/dotnet/sdk:10.0.302@sha256:72dd743782f2ae7e5476fd64f6a460045e3998dc862218b80e6944cba79a01b0 AS build
WORKDIR /src

# Restore before copying the sources so a code change does not invalidate the package layer.
# The root configuration comes first: central package management pins every version in
# Directory.Packages.props, so restore fails outright if it is missing, and NuGet.config keeps
# the feed list from falling back to whatever the build host has configured.
COPY global.json NuGet.config Directory.Build.props Directory.Packages.props ./

# The lock files travel with their .csproj, because --locked-mode below reads them during the
# very same restore. Copying them later, with the sources, would be too late.
COPY src/RedePanda.Contracts/RedePanda.Contracts.csproj src/RedePanda.Contracts/packages.lock.json src/RedePanda.Contracts/
COPY src/RedePanda.Backend/RedePanda.Backend.csproj src/RedePanda.Backend/packages.lock.json src/RedePanda.Backend/

# --locked-mode turns a lock file that no longer matches the project into a build failure
# (NU1004) instead of a silent upgrade. Regenerate with a plain `dotnet restore` on the host
# after changing a version, and commit the result.
RUN dotnet restore src/RedePanda.Backend/RedePanda.Backend.csproj --locked-mode

COPY src/RedePanda.Contracts/ src/RedePanda.Contracts/
COPY src/RedePanda.Backend/ src/RedePanda.Backend/
RUN dotnet publish src/RedePanda.Backend/RedePanda.Backend.csproj \
    -c Release -o /app --no-restore

# Debian-based on purpose. Confluent.Kafka carries native librdkafka assets that are built
# against glibc: an -alpine (musl) base cannot resolve them through the RID graph, and a
# -chiseled base has no reliable /etc/os-release for the loader's platform probe. Either one
# fails at the first ConsumerBuilder.Build(), at runtime rather than at build time.
#
# The tag stays at the 10.0 major stream here rather than an exact patch: global.json only
# constrains the SDK, and a published net10.0 app runs on any 10.0.x runtime. The digest is
# what pins it; the tag moving upstream cannot affect this build.
FROM mcr.microsoft.com/dotnet/aspnet:10.0@sha256:207cc51496778557731c81ff670333d8ade4a4fec22768fd1be8e78474a84ecf AS runtime
WORKDIR /app
COPY --from=build /app .

# APP_UID is a numeric uid supplied by the base image. Kubernetes' runAsNonRoot check rejects
# images whose USER is a name rather than a number, so this must stay numeric.
USER $APP_UID

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true
EXPOSE 8080

ENTRYPOINT ["dotnet", "RedePanda.Backend.dll"]
