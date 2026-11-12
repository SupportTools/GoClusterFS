import { useAllStats, useStatus } from '@/hooks'
import { useRefreshInterval } from '@/stores'
import { CacheStatsPanel } from '@/components'
import { formatBytes, formatNumber, formatPercent } from '@/utils/format'

/**
 * Loading skeleton for statistics
 */
function StatsSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-8 bg-gfs-bg-tertiary rounded w-48" />
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="card h-64" />
        <div className="card h-64" />
        <div className="card h-64" />
        <div className="card h-64" />
      </div>
    </div>
  )
}

/**
 * Error state
 */
function StatsError({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="card border-gfs-status-error">
      <div className="text-center py-8">
        <div className="text-gfs-status-error text-lg font-medium mb-2">
          Failed to load statistics
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

export function StatisticsPage() {
  const refreshInterval = useRefreshInterval()
  const { data: stats, loading, error, refetch } = useAllStats({ refreshInterval })
  const { data: status } = useStatus({ refreshInterval })

  if (loading && !stats) {
    return <StatsSkeleton />
  }

  if (error && !stats) {
    return <StatsError error={error} onRetry={refetch} />
  }

  const cacheStats = stats?.cache
  const journalStats = stats?.journal
  const trimStats = stats?.trim
  const lockStats = stats?.lock
  const capacity = status?.capacity

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gfs-text-primary">Statistics</h1>
        <p className="text-sm text-gfs-text-muted mt-1">
          Cache, journal, TRIM, lock and storage statistics
        </p>
      </div>

      {/* Two column layout */}
      <div className="grid gap-6 lg:grid-cols-2">
        {/* Left column - Cache Charts */}
        <div className="space-y-6">
          <CacheStatsPanel
            inodeCache={cacheStats?.inode_cache}
            extentCache={cacheStats?.extent_cache}
            lockHintCache={cacheStats?.lock_hint_cache}
          />
        </div>

        {/* Right column - Journal and Storage Stats */}
        <div className="space-y-6">
          {/* Journal Statistics */}
          <div className="card">
            <h3 className="card-header">Journal Statistics</h3>
            <div className="space-y-3">
              <StatRow label="State" value={journalStats?.state ?? '-'} />
              <StatRow
                label="Commits"
                value={formatNumber(journalStats?.commit_count ?? 0)}
              />
              <StatRow
                label="Bytes Written"
                value={formatBytes(journalStats?.bytes_written ?? 0)}
              />
              <StatRow
                label="Active Transactions"
                value={journalStats?.active_txn_count ?? 0}
                highlight={(journalStats?.active_txn_count ?? 0) > 0}
              />
              <StatRow
                label="Available Space"
                value={formatBytes(journalStats?.available_space ?? 0)}
              />
              <StatRow
                label="Batch Syncs"
                value={formatNumber(journalStats?.batch_sync_count ?? 0)}
              />
              <StatRow
                label="Last Checkpoint Seq"
                value={journalStats?.last_checkpoint_seq ?? 0}
              />
            </div>
          </div>

          {/* Trim Statistics */}
          <div className="card">
            <h3 className="card-header">Trim Statistics</h3>
            <div className="space-y-3">
              <StatRow
                label="Status"
                value={trimStats?.trim_enabled ? 'Enabled' : 'Disabled'}
                valueClass={trimStats?.trim_enabled ? 'text-gfs-status-ok' : 'text-gfs-text-muted'}
              />
              <StatRow
                label="Operations"
                value={formatNumber(trimStats?.trim_operations ?? 0)}
              />
              <StatRow
                label="Bytes Trimmed"
                value={formatBytes(trimStats?.trim_bytes ?? 0)}
              />
              <StatRow
                label="Blocks Trimmed"
                value={formatNumber(trimStats?.trim_blocks ?? 0)}
              />
              {trimStats?.pending_trim_blocks !== undefined && (
                <StatRow
                  label="Pending Blocks"
                  value={formatNumber(trimStats.pending_trim_blocks)}
                  highlight={trimStats.pending_trim_blocks > 0}
                />
              )}
              {trimStats?.trim_errors !== undefined && trimStats.trim_errors > 0 && (
                <StatRow
                  label="Errors"
                  value={trimStats.trim_errors}
                  valueClass="text-gfs-status-error"
                />
              )}
            </div>
          </div>

          <div className="card">
            <h3 className="card-header">Lock Statistics</h3>
            <div className="space-y-3">
              <StatRow label="Locks held" value={formatNumber(lockStats?.active_locks ?? 0)} />
              <StatRow
                label="Contended waits"
                value={formatNumber(lockStats?.pending_locks ?? 0)}
                highlight={(lockStats?.pending_locks ?? 0) > 0}
              />
              <StatRow label="Hint cache hits" value={formatNumber(lockStats?.hint_cache_hits ?? 0)} />
              <StatRow label="Hint cache misses" value={formatNumber(lockStats?.hint_cache_misses ?? 0)} />
              <StatRow label="Hint hit rate" value={formatPercent((lockStats?.hint_cache_hit_rate ?? 0) * 100)} />
            </div>
          </div>

          <div className="card">
            <h3 className="card-header">Storage Allocation</h3>
            <div className="space-y-3">
              <StatRow label="Total Blocks" value={formatNumber(capacity?.total_blocks ?? 0)} />
              <StatRow label="Used" value={formatNumber(capacity?.used_blocks ?? 0)} />
              <StatRow label="Free" value={formatNumber(capacity?.free_blocks ?? 0)} />
              <StatRow
                label="Used %"
                value={formatPercent(capacity?.used_percent ?? 0)}
                highlight={(capacity?.used_percent ?? 0) > 90}
              />
              <StatRow label="Block Size" value={formatBytes(capacity?.block_size_bytes ?? 0)} />
              <StatRow
                label="Small-file containers"
                value={stats?.sfb ? 'Enabled' : 'Disabled'}
                valueClass="text-gfs-text-muted"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

/**
 * Helper component for stat rows
 */
function StatRow({
  label,
  value,
  valueClass,
  highlight,
}: {
  label: string
  value: string | number
  valueClass?: string
  highlight?: boolean
}) {
  const baseClass = 'font-medium'
  const colorClass = highlight
    ? 'text-gfs-status-warning'
    : valueClass || 'text-gfs-text-primary'

  return (
    <div className="flex justify-between items-center">
      <span className="text-sm text-gfs-text-muted">{label}</span>
      <span className={`${baseClass} ${colorClass}`}>{value}</span>
    </div>
  )
}

export default StatisticsPage
