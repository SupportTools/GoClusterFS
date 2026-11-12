# GoClusterFS dashboard

The web dashboard that `goclusterfs-mount` serves at `http://<api-addr>/ui/`. What it shows and how auth works: [docs/API-CONFIG.md](../../docs/API-CONFIG.md#web-dashboard).

## Build

From the repo root:

```bash
make dashboard build
```

`make dashboard` runs `npm ci`, `npm run lint` and `npm run build`, which writes to `internal/webui/build/dist`. `make build` then embeds it in `goclusterfs-mount`. Without it, `/ui/` shows a placeholder page. Use Node 22; CI pins the exact version in `.github/workflows/ci.yml`.

## Develop

Start a mount with its API on `127.0.0.1:8080`, then:

```bash
npm ci
npm run dev
```

Open `http://localhost:5173/ui/`. Vite proxies `/api` and `/health` to the mount. Writes from the dev server work, because the proxy keeps the `Host` header and the API sees a same-origin request. They still need the token, which you enter on the Settings page.

## Packages

`package-lock.json` pins every package, and CI and releases install it with `npm ci`. Company policy: a package version must be at least 48 hours old before anyone installs it. Upgrade packages in their own PR, and only to versions that old.
