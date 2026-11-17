#!/usr/bin/env bash
# Splits the unit tests into CI groups of about equal run time, using unit-timings.txt.
# A package with a shard count after its time there gets that many groups to itself,
# split by test name and balanced with unit-test-timings.txt; the other packages share
# the remaining groups.
#
#   unit-groups.sh packages <groups> <group>   packages for one group, space separated
#   unit-groups.sh run <groups> <group>        -run regex for one group; . runs whole packages
#   unit-groups.sh check <groups>              fail unless every package, and every test of a
#                                              split package, is in exactly one group
set -euo pipefail

cd "$(dirname "$0")/../.."
timings=tests/scripts/unit-timings.txt
test_timings=tests/scripts/unit-test-timings.txt
default_seconds=5
default_test_seconds=1

list_packages() {
	local out
	out=$(go list ./internal/... ./tests/soak ./tests/integration/history)
	echo "$out"
}

# "package shards" for each package to split by test.
split_packages() {
	awk '$1 !~ /^#/ && NF >= 3 { print $1, $3 }' "$timings"
}

list_tests() {
	local out
	out=$(go test -list "${2:-.}" "$1")
	grep -E '^(Test|Example|Fuzz)' <<<"$out" || true
}

# Longest first, each into the shard with the least time so far.
balance() {
	sort -k1,1nr -k2,2 |
		awk -v n="$1" -v first="$2" '{ m = 1; for (i = 2; i <= n; i++) if (load[i] < load[m]) m = i; load[m] += $1; print first + m - 1, $2 }'
}

# One line per assignment: "<group> <package>" or "<group> <package> <test>".
plan() {
	local groups=$1 next=1 pkg shards
	local -a split=()
	while read -r pkg shards; do
		split+=("$pkg")
		list_tests "$pkg" |
			awk -v def="$default_test_seconds" -v pkg="$pkg" 'NR == FNR { if ($1 == pkg) w[$2] = $3; next } { print ($1 in w ? w[$1] : def), $1 }' "$test_timings" - |
			balance "$shards" "$next" |
			awk -v pkg="$pkg" '{ print $1, pkg, $2 }'
		next=$((next + shards))
	done < <(split_packages)
	if ((next > groups)); then
		echo "unit-groups.sh: split packages need $((next - 1)) groups, leaving none of $groups for the rest" >&2
		exit 1
	fi
	list_packages |
		grep -vxF -f <(printf '%s\n' "${split[@]+"${split[@]}"}") |
		awk -v def="$default_seconds" 'NR == FNR { if ($1 !~ /^#/) w[$1] = $2; next } { print ($1 in w ? w[$1] : def), $1 }' "$timings" - |
		balance $((groups - next + 1)) "$next"
}

packages() {
	plan "$1" | awk -v g="$2" '$1 == g && !seen[$2]++ { print $2 }'
}

# The -run regex for group $2 of the plan in $1.
regex() {
	awk -v g="$2" '$1 == g && NF == 3 { names = names sep $3; sep = "|" } END { print (names == "" ? "." : "^(" names ")$") }' <<<"$1"
}

run() {
	regex "$(plan "$1")" "$2"
}

check() {
	local groups=$1 p all got pkg shards
	p=$(plan "$groups")
	all=$(list_packages | sort)
	if [[ -z "$all" ]]; then
		echo "found no unit test packages"
		exit 1
	fi
	got=$(awk 'NF == 2 { print $2 } NF == 3 && !seen[$2]++ { print $2 }' <<<"$p" | sort)
	if [[ "$all" != "$got" ]]; then
		echo "unit test groups miss or repeat packages (< missing, > repeated):"
		diff <(echo "$all") <(echo "$got") || true
		exit 1
	fi
	echo "each of $(wc -l <<<"$all" | tr -d ' ') unit test packages is in exactly one of $groups groups"
	while read -r pkg shards; do
		all=$(list_tests "$pkg" | sort)
		got=$(for g in $(awk -v pkg="$pkg" '$2 == pkg { print $1 }' <<<"$p" | sort -u); do list_tests "$pkg" "$(regex "$p" "$g")"; done | sort)
		if [[ "$all" != "$got" ]]; then
			echo "the $shards groups of $pkg miss or repeat tests (< missing, > repeated):"
			diff <(echo "$all") <(echo "$got") || true
			exit 1
		fi
		echo "each of the $(wc -l <<<"$all" | tr -d ' ') tests of $pkg is in exactly one of its $shards groups"
	done < <(split_packages)
}

case "${1:-}" in
packages) packages "$2" "$3" | paste -sd ' ' - ;;
run) run "$2" "$3" ;;
check) check "$2" ;;
*)
	echo "usage: $0 packages <groups> <group> | run <groups> <group> | check <groups>" >&2
	exit 2
	;;
esac
