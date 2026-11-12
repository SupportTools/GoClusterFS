import { useState } from 'react'
import { useClusterData } from '@/hooks'
import { NodeDetailPanel, type ClusterNode } from '@/components'
import type { ClusterInfoResponse, LockStatsResponse } from '@/api'
import { formatRelativeTime, formatNumber, formatPercent, nodeStateColor } from '@/utils'

function NodeStateBadge({ state }: { state: string }) {
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs font-medium ${nodeStateColor(state)} text-white`}>
      <span className="w-1.5 h-1.5 rounded-full bg-current" />
      {state}
    </span>
  )
}

function clusterNodes(cluster: ClusterInfoResponse): ClusterNode[] {
  const self: ClusterNode = {
    nodeId: cluster.node_id,
    host: cluster.node_address ?? '',
    state: cluster.state,
    lastHeartbeat: cluster.last_heartbeat_at,
    joinedAt: cluster.joined_at,
    isSelf: true,
  }
  const peers = (cluster.peers ?? []).map((peer) => ({
    nodeId: peer.node_id,
    host: peer.address,
    state: peer.state,
    lastHeartbeat: peer.last_seen,
    isSelf: false,
  }))
  return [self, ...peers]
}

function NodeCard({ node, onClick }: { node: ClusterNode; onClick: () => void }) {
  return (
    <div
      className={`card transition-all cursor-pointer hover:ring-2 hover:ring-gfs-accent-primary/50 ${node.isSelf ? 'ring-2 ring-gfs-accent-secondary/50' : ''}`}
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
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <h3 className="text-sm font-semibold text-gfs-text-primary truncate">{node.host || node.nodeId}</h3>
            {node.isSelf && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gfs-accent-secondary text-white">
                This Node
              </span>
            )}
          </div>
          <p className="text-xs font-mono text-gfs-text-muted truncate">{node.nodeId}</p>
        </div>
        <NodeStateBadge state={node.state} />
      </div>
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div>
          <span className="text-gfs-text-muted">Last Heartbeat</span>
          <p className="text-gfs-text-secondary font-medium">
            {node.lastHeartbeat ? formatRelativeTime(new Date(node.lastHeartbeat)) : 'Unknown'}
          </p>
        </div>
        {node.joinedAt && (
          <div>
            <span className="text-gfs-text-muted">Mounted</span>
            <p className="text-gfs-text-secondary font-medium">{formatRelativeTime(new Date(node.joinedAt))}</p>
          </div>
        )}
      </div>
    </div>
  )
}

function ClusterOverview({ cluster }: { cluster: ClusterInfoResponse }) {
  const { cluster_size: size, healthy_nodes: healthy } = cluster
  const healthColor =
    healthy >= size ? 'text-gfs-status-ok' : healthy * 2 >= size ? 'text-gfs-status-warning' : 'text-gfs-status-error'

  return (
    <div className="card">
      <h2 className="card-header">Cluster Overview</h2>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
        <div>
          <span className="text-sm text-gfs-text-muted">Registered Nodes</span>
          <p className="text-2xl font-bold text-gfs-text-primary">{formatNumber(size)}</p>
        </div>
        <div>
          <span className="text-sm text-gfs-text-muted">Active</span>
          <p className={`text-2xl font-bold ${healthColor}`}>{formatNumber(healthy)}</p>
        </div>
        <div>
          <span className="text-sm text-gfs-text-muted">This Node</span>
          <p className="mt-1">
            <NodeStateBadge state={cluster.state} />
          </p>
        </div>
      </div>
      <p className="text-xs text-gfs-text-muted mt-4">
        Every node is a peer: GoClusterFS has no leader. Nodes coordinate through locks and heartbeats on the shared device.
      </p>
    </div>
  )
}

function LockCounters({ locks }: { locks: LockStatsResponse }) {
  const rows: Array<[string, string]> = [
    ['Locks held by this node', formatNumber(locks.active_locks ?? 0)],
    ['Waiting on contended locks', formatNumber(locks.pending_locks ?? 0)],
    ['Lock hint cache hits', formatNumber(locks.hint_cache_hits)],
    ['Lock hint cache misses', formatNumber(locks.hint_cache_misses)],
    ['Lock hint hit rate', formatPercent(locks.hint_cache_hit_rate * 100)],
  ]
  return (
    <div className="card">
      <h2 className="card-header">Distributed Locks</h2>
      <dl className="space-y-3">
        {rows.map(([label, value]) => (
          <div key={label} className="flex justify-between items-center">
            <dt className="text-sm text-gfs-text-muted">{label}</dt>
            <dd className="text-gfs-text-primary font-medium">{value}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function ClusterSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-8 bg-gfs-bg-tertiary rounded w-48" />
      <div className="card h-32" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="card h-32" />
        ))}
      </div>
    </div>
  )
}

function ClusterError({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="card border-gfs-status-error">
      <div className="text-center py-8">
        <div className="text-gfs-status-error text-lg font-medium mb-2">Failed to load cluster data</div>
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

export function ClusterPage() {
  const { cluster, locks, loading, error } = useClusterData()
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)

  if (loading && !cluster.data) {
    return <ClusterSkeleton />
  }
  if (!cluster.data) {
    return (
      <ClusterError
        error={error ?? new Error('No cluster data')}
        onRetry={() => {
          cluster.refetch()
          locks.refetch()
        }}
      />
    )
  }

  const nodes = clusterNodes(cluster.data)
  const selectedNode = nodes.find((node) => node.nodeId === selectedNodeId) ?? null

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gfs-text-primary">Cluster</h1>
          <p className="text-sm text-gfs-text-muted mt-1">Nodes in the shared device's registry</p>
        </div>
        <span className="text-xs text-gfs-text-muted">
          {cluster.lastUpdated && `Last updated: ${formatRelativeTime(cluster.lastUpdated)}`}
        </span>
      </div>

      <ClusterOverview cluster={cluster.data} />

      <div>
        <h2 className="text-lg font-semibold text-gfs-text-primary mb-4">Nodes ({nodes.length})</h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {nodes.map((node) => (
            <NodeCard key={node.nodeId} node={node} onClick={() => setSelectedNodeId(node.nodeId)} />
          ))}
        </div>
      </div>

      {locks.data && <LockCounters locks={locks.data} />}

      <NodeDetailPanel node={selectedNode} onClose={() => setSelectedNodeId(null)} />
    </div>
  )
}

export default ClusterPage
