#!/usr/bin/env bash
# Prints "<suite><TAB><test>" for each failed test in a log, for the nightly summary.
#
#   failed-tests.sh go <suite> <log>          go test output, with or without -v, fuzzing included
#   failed-tests.sh docker <suite> <results>  tests/docker results.txt
#
# A Go package that failed without a failed test (a build error, a panic in
# TestMain) prints as "<package> (package failed)"; a timeout names the tests
# that were still running.
set -euo pipefail

kind="$1" suite="$2" log="$3"
case "$kind" in
go)
	awk -v suite="$suite" '
		/^[[:space:]]*--- FAIL: / { sub(/^[[:space:]]*--- FAIL: /, ""); print suite "\t" $1; tests++; next }
		/^panic: test timed out/ { timedout = 1; next }
		timedout && /^\t+running tests:/ { running = 1; next }
		running && /^\t\t[^ ]+ \(/ { sub(/^\t\t/, ""); print suite "\t" $1 " (timed out)"; tests++; next }
		running { running = 0 }
		/^FAIL\t/ { pkgs[$2] = 1 }
		END { if (!tests) for (p in pkgs) print suite "\t" p " (package failed)" }
	' "$log" | sort -u
	;;
docker)
	awk -v suite="$suite" '/^FAIL: / { sub(/^FAIL: /, ""); print suite "\t" $0 }' "$log" | sort -u
	;;
*)
	echo "usage: $0 go|docker <suite> <log>" >&2
	exit 2
	;;
esac
