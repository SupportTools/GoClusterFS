#!/usr/bin/env bash
# Builds release archives and checksums.txt into dist/.
# VERSION defaults to `git describe`; PLATFORMS defaults to "linux/amd64 linux/arm64".
set -euo pipefail

cd "$(dirname "$0")/.."

version="${VERSION:-$(git describe --tags --always --dirty)}"
commit="$(git rev-parse HEAD)"
date="$(TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd)"
platforms="${PLATFORMS:-linux/amd64 linux/arm64}"
pkg=github.com/supporttools/GoClusterFS/internal/version
ldflags="-s -w -X $pkg.Version=$version -X $pkg.Commit=$commit -X $pkg.Date=$date"
go_license="$(go env GOROOT)/LICENSE"
[ -f "$go_license" ] || go_license="$(go env GOROOT)/../LICENSE" # Homebrew keeps it outside GOROOT

rm -rf dist
mkdir -p dist

for platform in $platforms; do
	goos="${platform%/*}"
	goarch="${platform#*/}"
	name="goclusterfs_${version#v}_${goos}_${goarch}"
	dir="dist/$name"
	mkdir -p "$dir/licenses/go"

	for cmd in mount fsck; do
		CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
			go build -trimpath -ldflags "$ldflags" -o "$dir/goclusterfs-$cmd" "./cmd/$cmd"
	done

	cp LICENSE README.md "$dir/"
	cp "$go_license" "$dir/licenses/go/"
	go version -m "$dir"/goclusterfs-* | awk '$1 == "dep" { print $2 }' | sort -u |
		while read -r mod; do
			mkdir -p "$dir/licenses/$mod"
			cp "$(go list -m -f '{{.Dir}}' "$mod")"/LICENSE* "$dir/licenses/$mod/"
		done

	tar -C dist -czf "dist/$name.tar.gz" "$name"
	rm -rf "$dir"
done

cd dist
if command -v sha256sum >/dev/null; then
	sha256sum goclusterfs_*.tar.gz >checksums.txt
else
	shasum -a 256 goclusterfs_*.tar.gz >checksums.txt
fi
