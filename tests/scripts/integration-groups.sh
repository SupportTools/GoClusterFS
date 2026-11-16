#!/usr/bin/env bash
# Splits the Go integration tests into CI groups of about equal run time,
# using integration-timings.txt, or the file INTEGRATION_TIMINGS names.
#
#   integration-groups.sh regex <groups> <group>   -run regex for one group
#   integration-groups.sh check <groups>           fail unless every test is in exactly one group
set -euo pipefail

cd "$(dirname "$0")/../.."
pkg=./tests/integration
timings="${INTEGRATION_TIMINGS:-tests/scripts/integration-timings.txt}"
default_seconds=3

list_tests() {
	local out
	out=$(go test -list "${1:-.}" "$pkg")
	grep '^Test' <<<"$out" || true
}

# Longest test first, each into the group with the least time so far.
assign() {
	list_tests |
		awk -v def="$default_seconds" 'NR == FNR { if ($1 !~ /^#/) w[$1] = $2; next } { print ($1 in w ? w[$1] : def), $1 }' "$timings" - |
		sort -k1,1nr -k2,2 |
		awk -v n="$1" '{ m = 1; for (i = 2; i <= n; i++) if (load[i] < load[m]) m = i; load[m] += $1; print m, $2 }'
}

regex() {
	assign "$1" | awk -v g="$2" '$1 == g { names = names sep $2; sep = "|" } END { print "^(" names ")$" }'
}

check() {
	local all got
	all=$(list_tests | sort)
	if [[ -z "$all" ]]; then
		echo "found no integration tests"
		exit 1
	fi
	got=$(for g in $(seq 1 "$1"); do list_tests "$(regex "$1" "$g")"; done | sort)
	if [[ "$all" != "$got" ]]; then
		echo "integration test groups miss or repeat tests (< missing, > repeated):"
		diff <(echo "$all") <(echo "$got") || true
		exit 1
	fi
	echo "each of $(echo "$all" | wc -l | tr -d ' ') integration tests is in exactly one of $1 groups"
}

case "${1:-}" in
regex) regex "$2" "$3" ;;
check) check "$2" ;;
*)
	echo "usage: $0 regex <groups> <group> | check <groups>" >&2
	exit 2
	;;
esac
