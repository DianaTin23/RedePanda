# Build context is the repository root, because the backend references RedePanda.Contracts.
#   podman build -f src/RedePanda.Backend/Dockerfile -t redepanda-backend:dev .

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Restore before copying the sources so a code change does not invalidate the package layer.
COPY src/RedePanda.Contracts/RedePanda.Contracts.csproj src/RedePanda.Contracts/
COPY src/RedePanda.Backend/RedePanda.Backend.csproj src/RedePanda.Backend/
RUN dotnet restore src/RedePanda.Backend/RedePanda.Backend.csproj

COPY src/RedePanda.Contracts/ src/RedePanda.Contracts/
COPY src/RedePanda.Backend/ src/RedePanda.Backend/
RUN dotnet publish src/RedePanda.Backend/RedePanda.Backend.csproj \
    -c Release -o /app --no-restore

# Debian-based on purpose. Confluent.Kafka carries native librdkafka assets that are built
# against glibc: an -alpine (musl) base cannot resolve them through the RID graph, and a
# -chiseled base has no reliable /etc/os-release for the loader's platform probe. Either one
# fails at the first ConsumerBuilder.Build(), at runtime rather than at build time.
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app .

# APP_UID is a numeric uid supplied by the base image. Kubernetes' runAsNonRoot check rejects
# images whose USER is a name rather than a number, so this must stay numeric.
USER $APP_UID

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true
EXPOSE 8080

ENTRYPOINT ["dotnet", "RedePanda.Backend.dll"]
