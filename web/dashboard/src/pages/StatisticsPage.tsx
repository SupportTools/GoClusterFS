/**
 * Statistics page - detailed cache, journal, and I/O stats
 */

import { useAllStats } from '@/hooks'
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
  const { data: stats, loading, error, refetch } = useAllStats({
    refreshInterval: 10000,
  })

  if (loading && !stats) {
    return <StatsSkeleton />
  }

  if (error && !stats) {
    return <StatsError error={error} onRetry={refetch} />
  }

  const cacheStats = stats?.cache
  const journalStats = stats?.journal
  const trimStats = stats?.trim
  const sfbStats = stats?.sfb

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gfs-text-primary">Statistics</h1>
        <p className="text-sm text-gfs-text-muted mt-1">
          Detailed cache, journal, and storage statistics
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

          {/* SFB (Space/Free Block) Statistics */}
          <div className="card">
            <h3 className="card-header">Storage Allocation</h3>
            <div className="space-y-3">
              <StatRow
                label="Total Blocks"
                value={formatNumber(sfbStats?.total_blocks ?? 0)}
              />
              <StatRow
                label="Allocated"
                value={formatNumber(sfbStats?.allocated_blocks ?? 0)}
              />
              <StatRow
                label="Free"
                value={formatNumber(sfbStats?.free_blocks ?? 0)}
              />
              <StatRow
                label="Allocation %"
                value={formatPercent(sfbStats?.allocation_percent ?? 0)}
                highlight={(sfbStats?.allocation_percent ?? 0) > 90}
              />
              {sfbStats?.fragmentation_ratio !== undefined && (
                <StatRow
                  label="Fragmentation"
                  value={formatPercent(sfbStats.fragmentation_ratio * 100)}
                />
              )}
              {sfbStats?.largest_free_extent !== undefined && (
                <StatRow
                  label="Largest Free Extent"
                  value={formatNumber(sfbStats.largest_free_extent)}
                />
              )}
              <StatRow
                label="Total Allocations"
                value={formatNumber(sfbStats?.allocations_total ?? 0)}
              />
              <StatRow
                label="Total Deallocations"
                value={formatNumber(sfbStats?.deallocations_total ?? 0)}
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
