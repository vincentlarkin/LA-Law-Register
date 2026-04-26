# BayouLex Quickstart

## Start Here

Use the compendium menu instead of remembering script flags:

```powershell
python scripts\bayoulex_compendium.py
```

From there you can download/build from source, download offline chunks from an
API, run a local API smoke server, and launch the C# Windows client.

The legacy Python GUI/search tools are intentionally not on this refactor
branch. They are preserved on the pushed `legacy` branch.

## How The API Gets Its Data

The API does not scrape websites while users search.

The data flow is:

1. The compendium/downloader workflow fetches Louisiana legal source material
   into local generated files under `out/`.
2. The dataset builder converts that corpus into one canonical SQLite file:
   `data/bayoulex-content.sqlite`.
3. The offline packager optionally compresses and chunks that SQLite file under
   `data/offline/<version>/`.
4. The API container mounts those files read-only and serves search/detail
   responses from SQLite.

So the API serves a prepared dataset. It is not continuously changing the
content in the background.

## Does The API Run Continuously?

Two separate things exist:

- The API server runs continuously as a small read-only web service.
- The data refresh/build workflow runs only when you choose to refresh content,
  or later on a schedule if you want that.

On TrueNAS, the API should run as a custom Docker app. It opens the prepared
SQLite dataset read-only. To update content, build a new dataset on a dev/build
machine, copy it to the NAS data directory, and restart or reload the API
container.

## Local Smoke Test

Start the local API smoke server:

```powershell
python scripts\bayoulex_compendium.py
```

Choose `Run local API smoke server`.

In another terminal, run the Windows app:

```powershell
python scripts\bayoulex_compendium.py
```

Choose `Run C# Windows client`.

In the app, set the API box to:

```text
http://127.0.0.1:5087/bayoulex/v1/
```

Click `Refresh`, then search. The `Download Data for Offline` button downloads
the chunked offline SQLite snapshot when the API has an offline manifest
available.

Opening either of these URLs in a browser should now show JSON instead of a
plain 404:

```text
http://127.0.0.1:5087/
http://127.0.0.1:5087/bayoulex/v1/
```

Direct commands, if you do want them for troubleshooting:

```powershell
$env:BAYOULEX_DATASET_PATH=(Resolve-Path data\bayoulex-content.sqlite).Path
$env:BAYOULEX_OFFLINE_ROOT=(Resolve-Path data\offline).Path
$env:BAYOULEX_PUBLIC_BASE_URL='http://127.0.0.1:5087/bayoulex/v1'
& "$env:USERPROFILE\.dotnet-sdk-10\dotnet.exe" run --project src\BayouLex.Api\BayouLex.Api.csproj --urls http://127.0.0.1:5087
```

Then, in another terminal:

```powershell
& "$env:USERPROFILE\.dotnet-sdk-10\dotnet.exe" run --project src\BayouLex.Client.Windows\BayouLex.Client.Windows.csproj
```

## What Goes On The NAS

For `api.ladf.us`, the NAS needs only the deployed API container plus the
prepared data files. The source download/build process should happen on a dev or
build machine so the NAS stays simple.

- Cloudflare Tunnel sends `api.ladf.us` traffic to NGINX.
- NGINX proxies only the BayouLex API route to the API container.
- The API opens `bayoulex-content.sqlite` read-only.
- Offline downloads are served as fixed chunks with hashes and ETags.
