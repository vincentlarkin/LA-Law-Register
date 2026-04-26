# BayouLex API + Fast Windows Client Refactor

## Summary

- Keep all existing content safe: preserve `legacy`, avoid history rewrites, and validate the new dataset before removing generated `out/` files from `main`.
- Use `api.ladf.us` through the existing Cloudflare Tunnel -> NGINX -> BayouLex API container.
- Replace the slow Python GUI path with a C# Windows desktop client that is API-first and supports optional offline SQLite download.
- Normal human operation should go through `python scripts\bayoulex_compendium.py`, not a pile of command-line flags.
- Legacy Python GUI/search tooling is preserved on the pushed `legacy` branch and removed from this API-first branch.

## Data + API

- Build one canonical SQLite content database from the current corpus on a dev/build machine, not the NAS.
- Keep Python downloaders as source ingestion tooling for v1; generated per-document files become build inputs, not committed/runtime files.
- API stack: ASP.NET Core Minimal API in a TrueNAS custom Docker app.
- Public read-only endpoints:
  - `GET /bayoulex/v1/init`
  - `GET /bayoulex/v1/catalog`
  - `GET /bayoulex/v1/search`
  - `GET /bayoulex/v1/documents/{documentKey}`
  - `GET /bayoulex/v1/offline/{version}/manifest`
  - `GET /bayoulex/v1/offline/{version}/chunks/{chunkNumber}`
- Offline package: compressed SQLite snapshot split into `16 MiB` chunks with manifest, per-chunk SHA-256, full-file SHA-256, ETags, resume support, and strict NGINX/API rate limits.

## Windows GUI Replacement

- Use the C# WPF desktop app as the GUI replacement.
- Default mode: API search/detail through `api.ladf.us`.
- Optional offline mode: download/reassemble SQLite snapshot, then search locally without requiring the API.
- UI must never run network, SQLite, full-text formatting, hash verification, or file I/O on the UI thread.
- Use async services with cancellation:
  - cancel stale searches as the user types;
  - debounce live search;
  - load details only when a row is selected;
  - stream offline chunks with progress and resume.
- Use virtualized result/document lists, fixed page sizes, and bounded previews so large documents cannot freeze the UI.
- Regex search is not part of remote v1 because it implies expensive full scans; if kept, make it offline-only and explicitly marked slower.

## Git + Repo Cleanup

- Commit current branding/license work intentionally, including `LICENSE` and `assets/branding/bayoulex-brand.png`.
- Create a refactor branch, for example `codex/api-client-refactor`.
- Add `docs/API_PLAN.md`.
- Add `src/BayouLex.Api`, `src/BayouLex.Client.Windows`, and shared DTO/client code.
- Remove generated corpus from `main` tracking after data parity succeeds:
  - `git rm -r --cached out`
  - keep `out/` ignored.
- Leave `legacy` untouched as the content-preserving snapshot branch.

## NAS + Security

- Use existing Cloudflare Tunnel and NGINX setup.
- NGINX proxies `api.ladf.us` to the API container; API is not directly exposed.
- Keep TrueNAS host changes minimal; if SSH is needed, inspect read-only first and use non-root key-based access.
- NGINX handles TLS edge, request size limits, chunk download throttling, and basic abuse controls.
- API has no write/admin routes and opens SQLite read-only.

## Test Plan

- Data parity: new SQLite includes the current searchable baseline unless exclusions are documented.
- API: init, catalog, search, detail, manifest, chunk fetch, ETag/304, 429 throttling.
- GUI: no hangs during typing, search cancellation works, detail loading is async, huge documents preview without freezing, offline download resumes after interruption.
- Offline: corrupt chunk detection, re-download only bad chunks, local search works after reassembly.
- Deployment smoke: Cloudflare Tunnel -> NGINX -> API -> SQLite responds on `api.ladf.us`.

## Assumptions

- Target desktop app is Windows-first WPF.
- API/default mode is public read-only.
- Offline full-content download is optional and throttled to protect the NAS.
- Implementation uses a .NET SDK on the dev machine; TrueNAS runs only the built container.
