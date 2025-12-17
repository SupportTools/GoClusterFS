#!/bin/bash
# GoClusterFS Network Partition Simulator
# Uses iptables and tc for realistic network simulation

set -e

ACTION="${1:-help}"
TARGET="${2:-all}"
PARAM="${3:-}"

# Cluster subnet (must match docker-compose network)
CLUSTER_SUBNET="192.168.168.0/24"

log() {
    echo "[Network] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

case "$ACTION" in
    isolate)
        # Block all traffic to/from cluster network
        if [ "$TARGET" = "all" ]; then
            log "Isolating node from entire cluster subnet ($CLUSTER_SUBNET)"
            iptables -I OUTPUT -d "$CLUSTER_SUBNET" -j DROP 2>/dev/null || true
            iptables -I INPUT -s "$CLUSTER_SUBNET" -j DROP 2>/dev/null || true
        else
            # Block specific IP
            log "Isolating node from $TARGET"
            iptables -I OUTPUT -d "$TARGET" -j DROP 2>/dev/null || true
            iptables -I INPUT -s "$TARGET" -j DROP 2>/dev/null || true
        fi
        log "Isolation rules applied"
        ;;

    delay)
        # Add network latency using tc
        DELAY_MS="${PARAM:-100}"
        log "Adding ${DELAY_MS}ms latency to all traffic"

        # Remove existing qdisc if present
        tc qdisc del dev eth0 root 2>/dev/null || true

        # Add netem qdisc with delay
        tc qdisc add dev eth0 root netem delay "${DELAY_MS}ms"
        log "Latency applied: ${DELAY_MS}ms"
        ;;

    loss)
        # Add packet loss
        LOSS_PERCENT="${PARAM:-10}"
        log "Adding ${LOSS_PERCENT}% packet loss"

        tc qdisc del dev eth0 root 2>/dev/null || true
        tc qdisc add dev eth0 root netem loss "${LOSS_PERCENT}%"
        log "Packet loss applied: ${LOSS_PERCENT}%"
        ;;

    heal)
        # Remove all partition rules
        log "Healing network partition..."

        # Remove iptables rules
        iptables -D OUTPUT -d "$CLUSTER_SUBNET" -j DROP 2>/dev/null || true
        iptables -D INPUT -s "$CLUSTER_SUBNET" -j DROP 2>/dev/null || true

        if [ -n "$TARGET" ] && [ "$TARGET" != "all" ]; then
            iptables -D OUTPUT -d "$TARGET" -j DROP 2>/dev/null || true
            iptables -D INPUT -s "$TARGET" -j DROP 2>/dev/null || true
        fi

        # Remove tc qdisc
        tc qdisc del dev eth0 root 2>/dev/null || true

        log "Network healed"
        ;;

    status)
        log "=== iptables rules ==="
        iptables -L -n 2>/dev/null || echo "No iptables access"

        log "=== tc qdisc ==="
        tc qdisc show dev eth0 2>/dev/null || echo "No tc access"
        ;;

    help|*)
        echo "Usage: $0 <action> [target] [param]"
        echo ""
        echo "Actions:"
        echo "  isolate [all|IP]    - Block traffic to/from target"
        echo "  delay [ms]          - Add latency (default: 100ms)"
        echo "  loss [percent]      - Add packet loss (default: 10%)"
        echo "  heal                - Remove all partition rules"
        echo "  status              - Show current rules"
        echo ""
        echo "Examples:"
        echo "  $0 isolate all              # Full isolation"
        echo "  $0 isolate 192.168.168.12   # Block specific node"
        echo "  $0 delay 200                # Add 200ms latency"
        echo "  $0 loss 5                   # Add 5% packet loss"
        echo "  $0 heal                     # Remove all rules"
        ;;
esac
