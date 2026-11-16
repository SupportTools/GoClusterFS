#!/usr/bin/env bash
# Prints a markdown coverage summary from Go cover profiles. A package that CI splits
# by test appears in several profiles, so each block counts once, covered if any
# profile ran it.
#
#   coverage-summary.sh <profile>...
set -euo pipefail

awk '
	FNR == 1 { next }
	{ stmts[$1] = $2; hits[$1] += $3 }
	END {
		for (b in stmts) {
			pkg = b
			sub(/\/[^\/]*:.*/, "", pkg)
			n[pkg] += stmts[b]
			total += stmts[b]
			if (hits[b] > 0) {
				c[pkg] += stmts[b]
				covered += stmts[b]
			}
		}
		printf "## Unit test coverage\n\n**Total: %.1f%%**\n\n| Package | Coverage |\n| --- | --- |\n", total ? 100 * covered / total : 0
		fflush()
		for (p in n) printf "| %s | %.1f%% |\n", p, 100 * c[p] / n[p] | "sort"
		close("sort")
	}' "$@"
