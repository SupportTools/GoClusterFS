/**
 * Node detail slide-in panel
 * Shows detailed information about a cluster node
 */

import { SlidePanel } from '@/components/common'
import type { ClusterNodeInfo } from '@/api'
import { formatRelativeTime } from '@/utils/format'

export interface NodeDetailPanelProps {
  /** The node to display, or null if panel should be closed */
  node: ClusterNodeInfo | null
  /** Whether this node is the cluster leader */
  isLeader: boolean
  /** Callback when panel should close */
  onClose: () => void
}

/**
 * Section component for grouping related information
 */
function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="mb-6 last:mb-0">
      <h3 className="text-sm font-medium text-gfs-text-muted uppercase tracking-wide mb-3">
        {title}
      </h3>
      {children}
    </div>
  )
}

/**
 * Key-value row for displaying node properties
 */
function InfoRow({
  label,
  value,
  valueClassName = '',
  mono = false,
}: {
  label: string
  value: React.ReactNode
  valueClassName?: string
  mono?: boolean
}) {
  return (
    <div className="flex justify-between items-start py-2 border-b border-gfs-border last:border-0">
      <span className="text-sm text-gfs-text-muted">{label}</span>
      <span
        className={`text-sm text-right ${mono ? 'font-mono text-xs' : ''} ${valueClassName || 'text-gfs-text-primary'}`}
      >
        {value}
      </span>
    </div>
  )
}

/**
 * Status indicator with color
 */
function StatusIndicator({ state }: { state: string }) {
  const stateColors: Record<string, { bg: string; text: string }> = {
    healthy: { bg: 'bg-gfs-status-ok', text: 'text-gfs-status-ok' },
    online: { bg: 'bg-gfs-status-ok', text: 'text-gfs-status-ok' },
    active: { bg: 'bg-gfs-status-ok', text: 'text-gfs-status-ok' },
    degraded: { bg: 'bg-gfs-status-warning', text: 'text-gfs-status-warning' },
    unhealthy: { bg: 'bg-gfs-status-error', text: 'text-gfs-status-error' },
    offline: { bg: 'bg-gfs-status-error', text: 'text-gfs-status-error' },
    unknown: { bg: 'bg-gfs-text-muted', text: 'text-gfs-text-muted' },
  }

  const colors = stateColors[state.toLowerCase()] || stateColors.unknown

  return (
    <div className="flex items-center gap-2">
      <span className={`w-2 h-2 rounded-full ${colors.bg} animate-pulse`} />
      <span className={`font-medium ${colors.text}`}>{state}</span>
    </div>
  )
}

/**
 * Latency indicator with color coding
 */
function LatencyIndicator({ latencyMs }: { latencyMs: number }) {
  let colorClass = 'text-gfs-status-ok'
  let status = 'Excellent'

  if (latencyMs > 200) {
    colorClass = 'text-gfs-status-error'
    status = 'Poor'
  } else if (latencyMs > 100) {
    colorClass = 'text-gfs-status-warning'
    status = 'Fair'
  } else if (latencyMs > 50) {
    colorClass = 'text-gfs-text-secondary'
    status = 'Good'
  }

  return (
    <div className="flex items-center gap-2">
      <span className={`font-medium ${colorClass}`}>{latencyMs}ms</span>
      <span className="text-xs text-gfs-text-muted">({status})</span>
    </div>
  )
}

export function NodeDetailPanel({ node, isLeader, onClose }: NodeDetailPanelProps) {
  if (!node) {
    return null
  }

  const lastHeartbeat = node.last_heartbeat
    ? formatRelativeTime(new Date(node.last_heartbeat))
    : 'Unknown'

  const joinedAt = node.joined_at
    ? formatRelativeTime(new Date(node.joined_at))
    : 'Unknown'

  return (
    <SlidePanel
      isOpen={node !== null}
      onClose={onClose}
      title={node.node_id}
      subtitle={node.address}
      width="md"
    >
      {/* Quick status badges */}
      <div className="flex flex-wrap gap-2 mb-6">
        {node.is_self && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-gfs-accent-secondary text-white">
            This Node
          </span>
        )}
        {isLeader && (
          <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-gfs-accent-primary text-white">
            Cluster Leader
          </span>
        )}
        <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-gfs-bg-tertiary text-gfs-text-primary">
          {node.role}
        </span>
      </div>

      {/* Status Section */}
      <Section title="Status">
        <div className="bg-gfs-bg-tertiary rounded-lg p-4">
          <InfoRow
            label="State"
            value={<StatusIndicator state={node.state} />}
          />
          <InfoRow
            label="Role"
            value={isLeader ? 'Leader' : node.role}
            valueClassName={isLeader ? 'text-gfs-accent-primary font-medium' : ''}
          />
          {node.latency_ms !== undefined && (
            <InfoRow
              label="Latency"
              value={<LatencyIndicator latencyMs={node.latency_ms} />}
            />
          )}
        </div>
      </Section>

      {/* Network Section */}
      <Section title="Network">
        <div className="bg-gfs-bg-tertiary rounded-lg p-4">
          <InfoRow label="Address" value={node.address} mono />
          <InfoRow label="Node ID" value={node.node_id} mono />
        </div>
      </Section>

      {/* Timing Section */}
      <Section title="Timing">
        <div className="bg-gfs-bg-tertiary rounded-lg p-4">
          <InfoRow label="Last Heartbeat" value={lastHeartbeat} />
          <InfoRow label="Joined Cluster" value={joinedAt} />
        </div>
      </Section>

      {/* Version Section */}
      {node.version && (
        <Section title="Software">
          <div className="bg-gfs-bg-tertiary rounded-lg p-4">
            <InfoRow label="Version" value={`v${node.version}`} />
          </div>
        </Section>
      )}

      {/* Actions Section */}
      <Section title="Actions">
        <div className="flex flex-col gap-2">
          <button
            type="button"
            className="w-full px-4 py-2 text-sm font-medium text-gfs-text-primary bg-gfs-bg-tertiary border border-gfs-border rounded hover:bg-gfs-bg-primary transition-colors"
            disabled
            title="Coming soon"
          >
            View Logs
          </button>
          <button
            type="button"
            className="w-full px-4 py-2 text-sm font-medium text-gfs-text-primary bg-gfs-bg-tertiary border border-gfs-border rounded hover:bg-gfs-bg-primary transition-colors"
            disabled
            title="Coming soon"
          >
            View Metrics
          </button>
          {!node.is_self && (
            <button
              type="button"
              className="w-full px-4 py-2 text-sm font-medium text-gfs-status-error bg-gfs-bg-tertiary border border-gfs-status-error/30 rounded hover:bg-gfs-status-error/10 transition-colors"
              disabled
              title="Coming soon"
            >
              Remove from Cluster
            </button>
          )}
        </div>
      </Section>
    </SlidePanel>
  )
}

export default NodeDetailPanel
