FROM mcr.microsoft.com/dotnet/sdk:8.0@sha256:2d7f935b8c7fe032cd3d36b5ce9c82c24413881e6dad1b4fbdf36cf369e4244f AS build
WORKDIR /source

COPY *.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish -c release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0@sha256:a6fc92280fbf2149cd6846d39c5bf7b9b535184e470aa68ef2847b9a02f6b99e
WORKDIR /app
COPY --from=build /app .

ENTRYPOINT ["dotnet", "csharp-npgsql.dll"]
