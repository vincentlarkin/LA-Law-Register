# BayouLex TrueNAS Deployment Notes

## Shape

Traffic should flow:

`Cloudflare Tunnel -> NGINX -> bayoulex-api container -> read-only SQLite dataset`

The API container should not publish a public router port. Keep it reachable only
from NGINX on the Docker network or a private host port.

## Build And Publish Data

Build data on a dev/build machine:

```powershell
python scripts\build_bayoulex_dataset.py --out out --db data\bayoulex-content.sqlite --dataset-version 20260426
python scripts\package_offline_snapshot.py --db data\bayoulex-content.sqlite --dataset-version 20260426
```

Upload these files to the NAS data mount:

- `data/bayoulex-content.sqlite`
- `data/offline/<version>/manifest.json`
- `data/offline/<version>/chunk-*.brpart`

Mount that directory read-only into the API container as `/data`.

## Build API Image

From the repo root, using a machine with the .NET SDK and Docker:

```powershell
docker build -f src\BayouLex.Api\Dockerfile -t bayoulex-api:latest .
```

The root `.dockerignore` intentionally excludes `out/`, `data/`, `build/`, and
`dist/` so the build context stays small.

## NGINX

Use `deploy/nginx/api.ladf.us.conf` as the starting point. If your TrueNAS NGINX
app separates `http{}` and `server{}` includes, put the `limit_req_zone` lines in
the `http{}` include and keep the `server{}` block in the site config.

## Safety Defaults

- No write/admin API endpoints.
- Dataset volume is read-only.
- Offline chunks are rate-limited more aggressively than search/detail.
- Cloudflare Tunnel remains the only public ingress.
