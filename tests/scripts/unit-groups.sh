#!/usr/bin/env bash
# Splits the unit test packages into CI groups of about equal run time,
# using unit-timings.txt.
#
#   unit-groups.sh packages <groups> <group>   packages for one group, space separated
#   unit-groups.sh check <groups>              fail unless every package is in exactly one group
set -euo pipefail

cd "$(dirname "$0")/../.."
timings=tests/scripts/unit-timings.txt
default_seconds=5

list_packages() {
	local out
	out=$(go list ./internal/...)
	echo "$out"
}

# Longest package first, each into the group with the least time so far.
assign() {
	list_packages |
		awk -v def="$default_seconds" 'NR == FNR { if ($1 !~ /^#/) w[$1] = $2; next } { print ($1 in w ? w[$1] : def), $1 }' "$timings" - |
		sort -k1,1nr -k2,2 |
		awk -v n="$1" '{ m = 1; for (i = 2; i <= n; i++) if (load[i] < load[m]) m = i; load[m] += $1; print m, $2 }'
}

packages() {
	assign "$1" | awk -v g="$2" '$1 == g { print $2 }'
}

check() {
	local all got
	all=$(list_packages | sort)
	if [[ -z "$all" ]]; then
		echo "found no unit test packages"
		exit 1
	fi
	got=$(for g in $(seq 1 "$1"); do packages "$1" "$g"; done | sort)
	if [[ "$all" != "$got" ]]; then
		echo "unit test groups miss or repeat packages (< missing, > repeated):"
		diff <(echo "$all") <(echo "$got") || true
		exit 1
	fi
	echo "each of $(echo "$all" | wc -l | tr -d ' ') unit test packages is in exactly one of $1 groups"
}

case "${1:-}" in
packages) packages "$2" "$3" | paste -sd ' ' - ;;
check) check "$2" ;;
*)
	echo "usage: $0 packages <groups> <group> | check <groups>" >&2
	exit 2
	;;
esac
