FROM mcr.microsoft.com/dotnet/sdk:10.0.302@sha256:72dd743782f2ae7e5476fd64f6a460045e3998dc862218b80e6944cba79a01b0 AS build
WORKDIR /src

COPY global.json NuGet.config Directory.Build.props Directory.Packages.props ./

COPY src/RedeTim.Contracts/RedeTim.Contracts.csproj src/RedeTim.Contracts/packages.lock.json src/RedeTim.Contracts/
COPY src/RedeTim.Backend/RedeTim.Backend.csproj src/RedeTim.Backend/packages.lock.json src/RedeTim.Backend/

RUN dotnet restore src/RedeTim.Backend/RedeTim.Backend.csproj --locked-mode

COPY src/RedeTim.Contracts/ src/RedeTim.Contracts/
COPY src/RedeTim.Backend/ src/RedeTim.Backend/
RUN dotnet publish src/RedeTim.Backend/RedeTim.Backend.csproj \
    -c Release -o /app --no-restore

# Must stay glibc-based: Confluent.Kafka's native librdkafka assets load under neither -alpine
# (musl) nor -chiseled, and they only fail at runtime.
FROM mcr.microsoft.com/dotnet/aspnet:10.0@sha256:207cc51496778557731c81ff670333d8ade4a4fec22768fd1be8e78474a84ecf AS runtime
WORKDIR /app
COPY --from=build /app .

USER $APP_UID

ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_RUNNING_IN_CONTAINER=true
EXPOSE 8080

ENTRYPOINT ["dotnet", "RedeTim.Backend.dll"]
