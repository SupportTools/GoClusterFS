#!/usr/bin/env bash
# Writes bash and zsh completion scripts for the three commands into $1
# (default: completions/), from each command's own flags.
# goclusterfs-mount and goclusterfs-fsck build only on Linux; other hosts skip them.
set -euo pipefail

cd "$(dirname "$0")/.."

out="${1:-completions}"
mkdir -p "$out/bash" "$out/zsh"

cmds="gfsctl"
if [ "$(go env GOHOSTOS)" = linux ]; then
	cmds="goclusterfs-mount goclusterfs-fsck gfsctl"
else
	echo "completions.sh: skipping goclusterfs-mount and goclusterfs-fsck, which build only on Linux" >&2
fi

bin="$(mktemp -d)"
trap 'rm -rf "$bin"' EXIT
for cmd in $cmds; do
	go build -o "$bin/$cmd" "./cmd/$cmd"
	sub="-completion"
	[ "$cmd" = gfsctl ] && sub="completion"
	"$bin/$cmd" "$sub" bash >"$out/bash/$cmd"
	"$bin/$cmd" "$sub" zsh >"$out/zsh/_$cmd"
done
