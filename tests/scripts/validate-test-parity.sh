#!/bin/bash
# validate-test-parity.sh - Validates test coverage across all environments
#
# This script reads tests/test-manifest.yaml and verifies that each listed
# test function actually exists in the corresponding test file.
#
# Exit codes:
#   0 - All tests validated successfully
#   1 - Critical tests are missing
#   2 - High-priority tests are missing (warning)
#
# Usage:
#   ./tests/scripts/validate-test-parity.sh [--report] [--strict]
#
# Options:
#   --report  Generate detailed coverage report
#   --strict  Fail on any TODO (not just critical)

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MANIFEST="$PROJECT_ROOT/tests/test-manifest.yaml"

# Test file locations
GO_TESTS_DIR="$PROJECT_ROOT/tests/integration"
DOCKER_TESTS="$PROJECT_ROOT/tests/docker/scripts/run-tests.sh"
VM_TESTS="$PROJECT_ROOT/tests/vm/scripts/run-vm-tests.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
total_scenarios=0
critical_missing=0
high_missing=0
medium_missing=0
go_implemented=0
docker_implemented=0
vm_implemented=0
go_todo=0
docker_todo=0
vm_todo=0

# Parse arguments
REPORT_MODE=false
STRICT_MODE=false
for arg in "$@"; do
    case $arg in
        --report)
            REPORT_MODE=true
            ;;
        --strict)
            STRICT_MODE=true
            ;;
    esac
done

# Verify manifest exists
if [[ ! -f "$MANIFEST" ]]; then
    echo -e "${RED}ERROR: Test manifest not found at $MANIFEST${NC}"
    exit 1
fi

# Function to verify a Go test exists
verify_go_test() {
    local spec="$1"  # format: filename::FunctionName

    if [[ "$spec" == "TODO" ]]; then
        return 1
    fi

    local file="${spec%%::*}"
    local func="${spec##*::}"
    local full_path="$GO_TESTS_DIR/$file"

    if [[ ! -f "$full_path" ]]; then
        return 1
    fi

    grep -q "func $func" "$full_path" 2>/dev/null
}

# Function to verify a shell test exists
verify_shell_test() {
    local test_file="$1"
    local spec="$2"  # format: filename::function_name

    if [[ "$spec" == "TODO" ]]; then
        return 1
    fi

    local func="${spec##*::}"

    if [[ ! -f "$test_file" ]]; then
        return 1
    fi

    # Match both "func_name() {" and "function func_name {" styles
    grep -qE "^${func}\(\)|^function ${func}" "$test_file" 2>/dev/null
}

# Function to extract scenarios from YAML-like format
# Uses a simpler approach compatible with mawk
extract_scenarios() {
    local id="" name="" priority="" go="" docker="" vm=""

    while IFS= read -r line; do
        # New scenario starts with "  - id:"
        if [[ "$line" =~ ^"  - id: " ]]; then
            # Print previous scenario if exists
            if [[ -n "$id" ]]; then
                echo "${id}|${name}|${priority}|${go}|${docker}|${vm}"
            fi
            # Reset and extract new id
            id="${line#*: }"
            name="" priority="" go="" docker="" vm=""
        elif [[ "$line" =~ ^"    name: " ]]; then
            name="${line#*: }"
        elif [[ "$line" =~ ^"    priority: " ]]; then
            priority="${line#*: }"
        elif [[ "$line" =~ ^"      go: " ]]; then
            go="${line#*: }"
        elif [[ "$line" =~ ^"      docker: " ]]; then
            docker="${line#*: }"
        elif [[ "$line" =~ ^"      vm: " ]]; then
            vm="${line#*: }"
        fi
    done < "$MANIFEST"

    # Print last scenario
    if [[ -n "$id" ]]; then
        echo "${id}|${name}|${priority}|${go}|${docker}|${vm}"
    fi
}

# Main validation
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}GoClusterFS Test Parity Validation${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Arrays for reporting
declare -a missing_critical=()
declare -a missing_high=()
declare -a missing_medium=()

while IFS='|' read -r id name priority go docker vm; do
    ((total_scenarios++))

    # Skip environment-specific tests
    case "$priority" in
        go_only|vm_only|docker_only)
            continue
            ;;
    esac

    scenario_issues=""

    # Check Go test
    if [[ -n "$go" && "$go" != "TODO" ]]; then
        if verify_go_test "$go"; then
            ((go_implemented++))
        else
            scenario_issues+="go:MISSING "
        fi
    elif [[ "$go" == "TODO" ]]; then
        ((go_todo++))
        scenario_issues+="go:TODO "
    fi

    # Check Docker test
    if [[ -n "$docker" && "$docker" != "TODO" ]]; then
        if verify_shell_test "$DOCKER_TESTS" "$docker"; then
            ((docker_implemented++))
        else
            scenario_issues+="docker:MISSING "
        fi
    elif [[ "$docker" == "TODO" ]]; then
        ((docker_todo++))
        scenario_issues+="docker:TODO "
    fi

    # Check VM test
    if [[ -n "$vm" && "$vm" != "TODO" ]]; then
        if verify_shell_test "$VM_TESTS" "$vm"; then
            ((vm_implemented++))
        else
            scenario_issues+="vm:MISSING "
        fi
    elif [[ "$vm" == "TODO" ]]; then
        ((vm_todo++))
        scenario_issues+="vm:TODO "
    fi

    # Track issues by priority
    if [[ -n "$scenario_issues" ]]; then
        case "$priority" in
            critical)
                ((critical_missing++))
                missing_critical+=("$id: $scenario_issues")
                ;;
            high)
                ((high_missing++))
                missing_high+=("$id: $scenario_issues")
                ;;
            medium)
                ((medium_missing++))
                missing_medium+=("$id: $scenario_issues")
                ;;
        esac
    fi

done < <(extract_scenarios)

# Print results
echo -e "${BLUE}Coverage Summary:${NC}"
echo "  Total scenarios: $total_scenarios"
echo ""
echo -e "  ${GREEN}Go tests:${NC}"
echo "    Implemented: $go_implemented"
echo "    TODO: $go_todo"
echo ""
echo -e "  ${GREEN}Docker tests:${NC}"
echo "    Implemented: $docker_implemented"
echo "    TODO: $docker_todo"
echo ""
echo -e "  ${GREEN}VM tests:${NC}"
echo "    Implemented: $vm_implemented"
echo "    TODO: $vm_todo"
echo ""

# Print missing tests
if [[ ${#missing_critical[@]} -gt 0 ]]; then
    echo -e "${RED}CRITICAL GAPS (must fix):${NC}"
    for issue in "${missing_critical[@]}"; do
        echo "  - $issue"
    done
    echo ""
fi

if [[ ${#missing_high[@]} -gt 0 ]]; then
    echo -e "${YELLOW}HIGH PRIORITY GAPS (should fix):${NC}"
    for issue in "${missing_high[@]}"; do
        echo "  - $issue"
    done
    echo ""
fi

if $REPORT_MODE && [[ ${#missing_medium[@]} -gt 0 ]]; then
    echo -e "${BLUE}MEDIUM PRIORITY GAPS (nice to have):${NC}"
    for issue in "${missing_medium[@]}"; do
        echo "  - $issue"
    done
    echo ""
fi

# Determine exit code
exit_code=0

if [[ $critical_missing -gt 0 ]]; then
    echo -e "${RED}FAIL: $critical_missing critical test(s) missing or have TODOs${NC}"
    exit_code=1
elif [[ $high_missing -gt 0 ]]; then
    echo -e "${YELLOW}WARN: $high_missing high-priority test(s) missing or have TODOs${NC}"
    if $STRICT_MODE; then
        exit_code=2
    fi
else
    echo -e "${GREEN}PASS: All critical and high-priority tests are implemented${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"

exit $exit_code
