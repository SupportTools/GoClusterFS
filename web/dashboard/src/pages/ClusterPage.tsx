/**
 * Cluster page - node status and topology
 */

import { useState } from 'react'
import { useClusterData } from '@/hooks'
import { NodeDetailPanel } from '@/components'
import type { ClusterNodeInfo, DistributedLockInfo } from '@/api'
import { formatRelativeTime, formatNumber } from '@/utils/format'

/**
 * Node state badge colors
 */
function NodeStateBadge({ state }: { state: string }) {
  const stateColors: Record<string, string> = {
    healthy: 'bg-gfs-status-ok',
    online: 'bg-gfs-status-ok',
    active: 'bg-gfs-status-ok',
    leader: 'bg-gfs-accent-primary',
    follower: 'bg-gfs-status-info',
    degraded: 'bg-gfs-status-warning',
    unhealthy: 'bg-gfs-status-error',
    offline: 'bg-gfs-status-error',
    unknown: 'bg-gfs-text-muted',
  }

  const color = stateColors[state.toLowerCase()] || 'bg-gfs-text-muted'

  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs font-medium ${color} text-white`}>
      <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse" />
      {state}
    </span>
  )
}

/**
 * Role badge for node role display
 */
function RoleBadge({ role, isLeader }: { role: string; isLeader: boolean }) {
  if (isLeader) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gfs-accent-primary text-white">
        Leader
      </span>
    )
  }

  const roleColors: Record<string, string> = {
    follower: 'bg-gfs-bg-tertiary text-gfs-text-primary',
    candidate: 'bg-gfs-status-warning text-white',
    learner: 'bg-gfs-status-info text-white',
  }

  const color = roleColors[role.toLowerCase()] || 'bg-gfs-bg-tertiary text-gfs-text-primary'

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {role}
    </span>
  )
}

/**
 * Self node indicator
 */
function SelfBadge() {
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gfs-accent-secondary text-white">
      This Node
    </span>
  )
}

/**
 * Node card component displaying node information
 */
function NodeCard({
  node,
  isLeader,
  onClick,
}: {
  node: ClusterNodeInfo
  isLeader: boolean
  onClick: () => void
}) {
  const lastHeartbeat = node.last_heartbeat
    ? formatRelativeTime(new Date(node.last_heartbeat))
    : 'Unknown'

  const joinedAt = node.joined_at
    ? formatRelativeTime(new Date(node.joined_at))
    : 'Unknown'

  return (
    <div
      className={`card transition-all cursor-pointer hover:ring-2 hover:ring-gfs-accent-primary/50 ${node.is_self ? 'ring-2 ring-gfs-accent-secondary/50' : ''}`}
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onClick()
        }
      }}
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <h3 className="text-sm font-semibold text-gfs-text-primary truncate">
              {node.node_id}
            </h3>
            {node.is_self && <SelfBadge />}
          </div>
          <p className="text-xs text-gfs-text-muted truncate">{node.address}</p>
        </div>
        <NodeStateBadge state={node.state} />
      </div>

      {/* Role and version */}
      <div className="flex items-center gap-2 mb-3">
        <RoleBadge role={node.role} isLeader={isLeader} />
        {node.version && (
          <span className="text-xs text-gfs-text-muted">v{node.version}</span>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div>
          <span className="text-gfs-text-muted">Last Heartbeat</span>
          <p className="text-gfs-text-secondary font-medium">{lastHeartbeat}</p>
        </div>
        <div>
          <span className="text-gfs-text-muted">Joined</span>
          <p className="text-gfs-text-secondary font-medium">{joinedAt}</p>
        </div>
        {node.latency_ms !== undefined && (
          <div className="col-span-2">
            <span className="text-gfs-text-muted">Latency</span>
            <p className={`font-medium ${node.latency_ms > 100 ? 'text-gfs-status-warning' : 'text-gfs-status-ok'}`}>
              {node.latency_ms}ms
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

/**
 * Cluster overview stats card
 */
function ClusterStatsCard({
  totalNodes,
  healthyNodes,
  leaderId,
  clusterId,
}: {
  totalNodes: number
  healthyNodes: number
  leaderId?: string
  clusterId?: string
}) {
  const healthPercent = totalNodes > 0 ? (healthyNodes / totalNodes) * 100 : 0
  const healthColor =
    healthPercent >= 100
      ? 'text-gfs-status-ok'
      : healthPercent >= 50
        ? 'text-gfs-status-warning'
        : 'text-gfs-status-error'

  return (
    <div className="card">
      <h2 className="card-header">Cluster Overview</h2>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div>
          <span className="text-sm text-gfs-text-muted">Total Nodes</span>
          <p className="text-2xl font-bold text-gfs-text-primary">
            {formatNumber(totalNodes)}
          </p>
        </div>
        <div>
          <span className="text-sm text-gfs-text-muted">Healthy</span>
          <p className={`text-2xl font-bold ${healthColor}`}>
            {formatNumber(healthyNodes)}
          </p>
        </div>
        <div className="col-span-2 sm:col-span-1">
          <span className="text-sm text-gfs-text-muted">Leader</span>
          <p className="text-sm font-medium text-gfs-text-primary truncate">
            {leaderId || 'None elected'}
          </p>
        </div>
        {clusterId && (
          <div className="col-span-2 sm:col-span-1">
            <span className="text-sm text-gfs-text-muted">Cluster ID</span>
            <p className="text-xs font-mono text-gfs-text-secondary truncate">
              {clusterId}
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

/**
 * Lock info row
 */
function LockRow({ lock }: { lock: DistributedLockInfo }) {
  const acquiredAt = formatRelativeTime(new Date(lock.acquired_at))
  const expiresAt = lock.expires_at
    ? formatRelativeTime(new Date(lock.expires_at))
    : 'Never'

  return (
    <tr className="border-b border-gfs-border last:border-0">
      <td className="py-2 text-sm text-gfs-text-primary truncate max-w-[150px]">
        {lock.resource}
      </td>
      <td className="py-2 text-sm text-gfs-text-secondary truncate max-w-[100px]">
        {lock.owner_node_id}
      </td>
      <td className="py-2 text-xs text-gfs-text-muted">{lock.lock_type}</td>
      <td className="py-2 text-xs text-gfs-text-muted">{acquiredAt}</td>
      <td className="py-2 text-xs text-gfs-text-muted">{expiresAt}</td>
    </tr>
  )
}

/**
 * Locks panel showing active distributed locks
 */
function LocksPanel({
  activeLocks,
  pendingLocks,
  locks,
}: {
  activeLocks: number
  pendingLocks: number
  locks: DistributedLockInfo[]
}) {
  return (
    <div className="card">
      <div className="flex items-center justify-between mb-4">
        <h2 className="card-header mb-0">Distributed Locks</h2>
        <div className="flex gap-4 text-sm">
          <span className="text-gfs-text-muted">
            Active: <span className="text-gfs-status-ok font-medium">{activeLocks}</span>
          </span>
          <span className="text-gfs-text-muted">
            Pending: <span className="text-gfs-status-warning font-medium">{pendingLocks}</span>
          </span>
        </div>
      </div>

      {locks.length === 0 ? (
        <p className="text-sm text-gfs-text-muted text-center py-4">
          No active locks
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-left text-xs text-gfs-text-muted border-b border-gfs-border">
                <th className="pb-2 font-medium">Resource</th>
                <th className="pb-2 font-medium">Owner</th>
                <th className="pb-2 font-medium">Type</th>
                <th className="pb-2 font-medium">Acquired</th>
                <th className="pb-2 font-medium">Expires</th>
              </tr>
            </thead>
            <tbody>
              {locks.slice(0, 10).map((lock) => (
                <LockRow key={lock.lock_id} lock={lock} />
              ))}
            </tbody>
          </table>
          {locks.length > 10 && (
            <p className="text-xs text-gfs-text-muted text-center mt-2">
              Showing 10 of {locks.length} locks
            </p>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * Loading skeleton
 */
function ClusterSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-8 bg-gfs-bg-tertiary rounded w-48" />
      <div className="card h-32" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {[...Array(6)].map((_, i) => (
          <div key={i} className="card h-40" />
        ))}
      </div>
    </div>
  )
}

/**
 * Error display
 */
function ClusterError({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="card border-gfs-status-error">
      <div className="text-center py-8">
        <div className="text-gfs-status-error text-lg font-medium mb-2">
          Failed to load cluster data
        </div>
        <div className="text-gfs-text-muted text-sm mb-4">{error.message}</div>
        <button
          onClick={onRetry}
          className="px-4 py-2 bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover transition-colors"
        >
          Retry
        </button>
      </div>
    </div>
  )
}

/**
 * Empty state when no cluster configured
 */
function EmptyCluster() {
  return (
    <div className="card">
      <div className="text-center py-12">
        <div className="text-4xl mb-4">🔗</div>
        <h3 className="text-lg font-medium text-gfs-text-primary mb-2">
          No Cluster Configured
        </h3>
        <p className="text-sm text-gfs-text-muted max-w-md mx-auto">
          This node is running in standalone mode. Configure cluster settings
          to enable multi-node functionality.
        </p>
      </div>
    </div>
  )
}

export function ClusterPage() {
  const { nodes, locks, stats, loading, error } = useClusterData()

  // Selected node for detail panel
  const [selectedNode, setSelectedNode] = useState<ClusterNodeInfo | null>(null)

  // Combined refetch
  const refresh = () => {
    nodes.refetch()
    locks.refetch()
    stats.refetch()
  }

  if (loading && !nodes.data) {
    return <ClusterSkeleton />
  }

  if (error && !nodes.data) {
    return <ClusterError error={error} onRetry={refresh} />
  }

  const nodesData = nodes.data
  const locksData = locks.data
  const statsData = stats.data

  // Check if cluster is configured
  const hasCluster = nodesData && nodesData.nodes && nodesData.nodes.length > 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gfs-text-primary">Cluster</h1>
          <p className="text-sm text-gfs-text-muted mt-1">
            Node status and topology information
          </p>
        </div>
        <span className="text-xs text-gfs-text-muted">
          {nodes.lastUpdated && `Last updated: ${formatRelativeTime(nodes.lastUpdated)}`}
        </span>
      </div>

      {!hasCluster ? (
        <EmptyCluster />
      ) : (
        <>
          {/* Cluster overview stats */}
          <ClusterStatsCard
            totalNodes={nodesData.total_nodes}
            healthyNodes={nodesData.healthy_nodes}
            leaderId={nodesData.leader_id}
            clusterId={nodesData.cluster_id}
          />

          {/* Node grid */}
          <div>
            <h2 className="text-lg font-semibold text-gfs-text-primary mb-4">
              Nodes ({nodesData.nodes.length})
            </h2>
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {nodesData.nodes.map((node) => (
                <NodeCard
                  key={node.node_id}
                  node={node}
                  isLeader={node.node_id === nodesData.leader_id}
                  onClick={() => setSelectedNode(node)}
                />
              ))}
            </div>
          </div>

          {/* Distributed locks */}
          {locksData && (
            <LocksPanel
              activeLocks={locksData.active_locks}
              pendingLocks={locksData.pending_locks}
              locks={locksData.locks}
            />
          )}

          {/* Cluster stats from stats endpoint (additional info) */}
          {statsData && (
            <div className="card">
              <h2 className="card-header">This Node Stats</h2>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
                <div>
                  <span className="text-gfs-text-muted">Node ID</span>
                  <p className="font-mono text-xs text-gfs-text-primary truncate">
                    {statsData.node_id}
                  </p>
                </div>
                <div>
                  <span className="text-gfs-text-muted">Role</span>
                  <p className="text-gfs-text-primary font-medium">{statsData.role}</p>
                </div>
                <div>
                  <span className="text-gfs-text-muted">State</span>
                  <p className="text-gfs-text-primary font-medium">{statsData.state}</p>
                </div>
                {statsData.last_heartbeat_at && (
                  <div>
                    <span className="text-gfs-text-muted">Last Heartbeat</span>
                    <p className="text-gfs-text-primary font-medium">
                      {formatRelativeTime(new Date(statsData.last_heartbeat_at))}
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}
        </>
      )}

      {/* Node Detail Panel */}
      <NodeDetailPanel
        node={selectedNode}
        isLeader={selectedNode?.node_id === nodesData?.leader_id}
        onClose={() => setSelectedNode(null)}
      />
    </div>
  )
}

export default ClusterPage
