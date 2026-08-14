FROM mcr.microsoft.com/dotnet/sdk:10.0.302@sha256:72dd743782f2ae7e5476fd64f6a460045e3998dc862218b80e6944cba79a01b0 AS build
WORKDIR /src

COPY global.json NuGet.config Directory.Build.props Directory.Packages.props ./
COPY src/RedePanda.Contracts/RedePanda.Contracts.csproj src/RedePanda.Contracts/packages.lock.json src/RedePanda.Contracts/
COPY src/RedePanda.ChatClient/RedePanda.ChatClient.csproj src/RedePanda.ChatClient/packages.lock.json src/RedePanda.ChatClient/

RUN dotnet restore src/RedePanda.ChatClient/RedePanda.ChatClient.csproj --locked-mode

COPY src/RedePanda.Contracts/ src/RedePanda.Contracts/
COPY src/RedePanda.ChatClient/ src/RedePanda.ChatClient/
RUN dotnet publish src/RedePanda.ChatClient/RedePanda.ChatClient.csproj \
    -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0@sha256:acad02eb5c4fbf57d15296f9c08d56cd4036e915bdae5b4dd48a06523d452617 AS runtime
WORKDIR /app
COPY --from=build /app .

USER $APP_UID

ENV DOTNET_RUNNING_IN_CONTAINER=true

ENTRYPOINT ["dotnet", "RedePanda.ChatClient.dll"]
