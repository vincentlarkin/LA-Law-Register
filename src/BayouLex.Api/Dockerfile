FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/BayouLex.Shared/BayouLex.Shared.csproj src/BayouLex.Shared/
COPY src/BayouLex.Api/BayouLex.Api.csproj src/BayouLex.Api/
RUN dotnet restore src/BayouLex.Api/BayouLex.Api.csproj
COPY src/ src/
RUN dotnet publish src/BayouLex.Api/BayouLex.Api.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
ENV ASPNETCORE_URLS=http://+:8080
ENV BAYOULEX_DATASET_PATH=/data/bayoulex-content.sqlite
ENV BAYOULEX_OFFLINE_ROOT=/data/offline
ENV BAYOULEX_PUBLIC_BASE_URL=https://api.ladf.us/bayoulex/v1
EXPOSE 8080
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "BayouLex.Api.dll"]
