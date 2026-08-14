FROM mcr.microsoft.com/dotnet/sdk:10.0.302@sha256:72dd743782f2ae7e5476fd64f6a460045e3998dc862218b80e6944cba79a01b0 AS build
WORKDIR /src

COPY global.json NuGet.config Directory.Build.props Directory.Packages.props ./

COPY src/RedePanda.Contracts/RedePanda.Contracts.csproj src/RedePanda.Contracts/packages.lock.json src/RedePanda.Contracts/
COPY src/RedePanda.Backend/RedePanda.Backend.csproj src/RedePanda.Backend/packages.lock.json src/RedePanda.Backend/

RUN dotnet restore src/RedePanda.Backend/RedePanda.Backend.csproj --locked-mode

COPY src/RedePanda.Contracts/ src/RedePanda.Contracts/
COPY src/RedePanda.Backend/ src/RedePanda.Backend/
RUN dotnet publish src/RedePanda.Backend/RedePanda.Backend.csproj \
    -c Release -o /app --no-restore

# Muss glibc-basiert bleiben: die nativen librdkafka-Assets von Confluent.Kafka laden weder unter
# -alpine (musl) noch unter -chiseled, und zwar erst zur Laufzeit. Siehe docs/build.md.
FROM mcr.microsoft.com/dotnet/aspnet:10.0@sha256:207cc51496778557731c81ff670333d8ade4a4fec22768fd1be8e78474a84ecf AS runtime
WORKDIR /app
COPY --from=build /app .

USER $APP_UID

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true
EXPOSE 8080

ENTRYPOINT ["dotnet", "RedePanda.Backend.dll"]
