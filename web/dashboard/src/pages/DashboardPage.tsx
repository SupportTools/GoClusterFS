import { useDashboardData } from '@/hooks'
import {
  CapacityGauge,
  BlockGauge,
  HitRateGauge as CircularHitRateGauge,
  MiniGauge,
  JournalGauge,
  TokenHint,
} from '@/components'
import {
  formatBytes,
  formatNumber,
  formatPercent,
  formatDuration,
  formatRelativeTime,
} from '@/utils/format'

/**
 * Status indicator component
 */
function StatusBadge({ status }: { status: string }) {
  const colorMap: Record<string, string> = {
    healthy: 'bg-gfs-status-ok',
    degraded: 'bg-gfs-status-warning',
    unhealthy: 'bg-gfs-status-error',
    ok: 'bg-gfs-status-ok',
    mounted: 'bg-gfs-status-ok',
  }
  const color = colorMap[status.toLowerCase()] || 'bg-gfs-status-info'

  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded text-xs font-medium ${color} text-white`}>
      <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse" />
      {status}
    </span>
  )
}

/**
 * Stat card component for displaying a single metric
 */
function StatCard({
  label,
  value,
  subValue,
  trend,
}: {
  label: string
  value: string | number
  subValue?: string
  trend?: 'up' | 'down' | 'neutral'
}) {
  const trendColors = {
    up: 'text-gfs-status-ok',
    down: 'text-gfs-status-error',
    neutral: 'text-gfs-text-muted',
  }

  return (
    <div className="card">
      <dt className="text-sm font-medium text-gfs-text-muted truncate">{label}</dt>
      <dd className="mt-1 flex items-baseline justify-between">
        <span className="text-2xl font-semibold text-gfs-text-primary">{value}</span>
        {subValue && (
          <span className={`text-sm ${trend ? trendColors[trend] : 'text-gfs-text-secondary'}`}>
            {subValue}
          </span>
        )}
      </dd>
    </div>
  )
}

/**
 * Loading skeleton
 */
function DashboardSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-8 bg-gfs-bg-tertiary rounded w-48" />
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="card h-24" />
        ))}
      </div>
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="card h-48" />
        <div className="card h-48" />
      </div>
    </div>
  )
}

/**
 * Error display
 */
function DashboardError({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="card border-gfs-status-error">
      <div className="text-center py-8">
        <div className="text-gfs-status-error text-lg font-medium mb-2">
          Failed to load dashboard data
        </div>
        <div className="text-gfs-text-muted text-sm mb-4">{error.message}</div>
        <TokenHint error={error} />
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

export function DashboardPage() {
  const { status, stats, health, loading, error } = useDashboardData()

  // Combined refetch function
  const refresh = () => {
    status.refetch()
    stats.refetch()
    health.refetch()
  }

  if (loading && !status.data) {
    return <DashboardSkeleton />
  }

  if (error && !status.data) {
    return <DashboardError error={error} onRetry={refresh} />
  }

  // Extract data from responses with defaults
  const statusData = status.data
  const statsData = stats.data
  const healthData = health.data

  // Capacity info from status
  const capacity = statusData?.capacity ?? {
    total_bytes: 0,
    used_bytes: 0,
    free_bytes: 0,
    used_percent: 0,
    total_blocks: 0,
    used_blocks: 0,
    free_blocks: 0,
    block_size_bytes: 1048576,
  }

  // Cache stats from all stats
  const cacheStats = statsData?.cache
  const inodeCache = cacheStats?.inode_cache ?? { size: 0, hits: 0, misses: 0, hit_rate: 0 }
  const extentCache = cacheStats?.extent_cache ?? { size: 0, hits: 0, misses: 0, hit_rate: 0 }
  const lockHintCache = cacheStats?.lock_hint_cache ?? { size: 0, hits: 0, misses: 0, hit_rate: 0 }

  // Journal stats
  const journalStats = statsData?.journal

  const trimStats = statsData?.trim

  // Calculate totals for cache
  const totalCacheHits = inodeCache.hits + extentCache.hits + lockHintCache.hits
  const totalCacheMisses = inodeCache.misses + extentCache.misses + lockHintCache.misses

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gfs-text-primary">Dashboard</h1>
          <p className="text-sm text-gfs-text-muted mt-1">
            {statusData?.mount_path
              ? `Mounted at ${statusData.mount_path}`
              : 'Filesystem not mounted'}
          </p>
        </div>
        <div className="flex items-center gap-4">
          <StatusBadge status={healthData?.status || statusData?.status || 'unknown'} />
          {stats.lastUpdated && (
            <span className="text-xs text-gfs-text-muted">
              Last updated: {formatRelativeTime(stats.lastUpdated)}
            </span>
          )}
        </div>
      </div>

      {/* Key metrics */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Uptime"
          value={formatDuration(statusData?.uptime_seconds ?? 0)}
        />
        <StatCard
          label="Storage Used"
          value={formatPercent(capacity.used_percent)}
          subValue={formatBytes(capacity.used_bytes)}
        />
        <StatCard
          label="Journal Commits"
          value={formatNumber(journalStats?.commit_count ?? 0)}
          subValue={`${journalStats?.active_txn_count ?? 0} active`}
        />
        <StatCard
          label="Blocks Allocated"
          value={formatNumber(capacity.used_blocks)}
          subValue={`of ${formatNumber(capacity.total_blocks)}`}
        />
      </div>

      {/* Capacity and Cache */}
      <div className="grid gap-6 lg:grid-cols-2">
        {/* Storage capacity */}
        <div className="card">
          <h2 className="card-header">Storage Capacity</h2>
          <div className="flex flex-col items-center space-y-4">
            <div className="flex gap-8 justify-center">
              <CapacityGauge
                usedBytes={capacity.used_bytes}
                totalBytes={capacity.total_bytes}
                label="Bytes Used"
                size={120}
              />
              <BlockGauge
                usedBlocks={capacity.used_blocks}
                totalBlocks={capacity.total_blocks}
                label="Blocks Used"
                size={120}
              />
            </div>
            <div className="grid grid-cols-3 gap-4 text-center w-full pt-2">
              <div>
                <div className="text-lg font-semibold text-gfs-text-primary">
                  {formatNumber(capacity.total_blocks)}
                </div>
                <div className="text-xs text-gfs-text-muted">Total Blocks</div>
              </div>
              <div>
                <div className="text-lg font-semibold text-gfs-status-warning">
                  {formatNumber(capacity.used_blocks)}
                </div>
                <div className="text-xs text-gfs-text-muted">Used</div>
              </div>
              <div>
                <div className="text-lg font-semibold text-gfs-status-ok">
                  {formatNumber(capacity.free_blocks)}
                </div>
                <div className="text-xs text-gfs-text-muted">Free</div>
              </div>
            </div>
          </div>
        </div>

        {/* Cache performance */}
        <div className="card">
          <h2 className="card-header">Cache Performance</h2>
          <div className="flex flex-col items-center space-y-4">
            <CircularHitRateGauge
              hits={totalCacheHits}
              misses={totalCacheMisses}
              label="Overall Hit Rate"
              size={120}
            />
            <div className="grid grid-cols-3 gap-4 text-center w-full pt-2">
              <div className="flex flex-col items-center">
                <MiniGauge
                  percent={inodeCache.hit_rate * 100}
                  size={48}
                  warningThreshold={50}
                  criticalThreshold={80}
                  colors={{ ok: '#f56565', warning: '#ed8936', critical: '#48bb78' }}
                />
                <div className="text-xs text-gfs-text-muted mt-2">Inode Cache</div>
              </div>
              <div className="flex flex-col items-center">
                <MiniGauge
                  percent={extentCache.hit_rate * 100}
                  size={48}
                  warningThreshold={50}
                  criticalThreshold={80}
                  colors={{ ok: '#f56565', warning: '#ed8936', critical: '#48bb78' }}
                />
                <div className="text-xs text-gfs-text-muted mt-2">Extent Cache</div>
              </div>
              <div className="flex flex-col items-center">
                <MiniGauge
                  percent={lockHintCache.hit_rate * 100}
                  size={48}
                  warningThreshold={50}
                  criticalThreshold={80}
                  colors={{ ok: '#f56565', warning: '#ed8936', critical: '#48bb78' }}
                />
                <div className="text-xs text-gfs-text-muted mt-2">Lock Hints</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* TRIM and Journal */}
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="card">
          <h2 className="card-header">TRIM</h2>
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <span className="text-sm text-gfs-text-muted">Status</span>
              <span className={`font-medium ${trimStats?.trim_enabled ? 'text-gfs-status-ok' : 'text-gfs-text-muted'}`}>
                {trimStats?.trim_enabled ? 'Enabled' : 'Off or unsupported by the device'}
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gfs-text-muted">Bytes discarded</span>
              <span className="text-gfs-text-primary font-medium">{formatBytes(trimStats?.trim_bytes ?? 0)}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gfs-text-muted">Blocks waiting</span>
              <span className="text-gfs-text-primary font-medium">{formatNumber(trimStats?.pending_trim_blocks ?? 0)}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gfs-text-muted">Errors</span>
              <span className={`font-medium ${(trimStats?.trim_errors ?? 0) > 0 ? 'text-gfs-status-error' : 'text-gfs-text-primary'}`}>
                {formatNumber(trimStats?.trim_errors ?? 0)}
              </span>
            </div>
          </div>
        </div>

        {/* Journal Status */}
        <div className="card">
          <h2 className="card-header">Journal Status</h2>
          <div className="flex flex-col items-center">
            <JournalGauge
              bytesWritten={journalStats?.bytes_written ?? 0}
              availableSpace={journalStats?.available_space ?? 0}
              state={journalStats?.state ?? 'unknown'}
              commitCount={journalStats?.commit_count ?? 0}
              activeTxnCount={journalStats?.active_txn_count ?? 0}
              size={120}
            />
            <div className="mt-4 w-full space-y-2">
              <div className="flex justify-between items-center text-sm">
                <span className="text-gfs-text-muted">Bytes Written</span>
                <span className="text-gfs-text-primary font-medium">
                  {formatBytes(journalStats?.bytes_written ?? 0)}
                </span>
              </div>
              <div className="flex justify-between items-center text-sm">
                <span className="text-gfs-text-muted">Available Space</span>
                <span className="text-gfs-text-primary font-medium">
                  {formatBytes(journalStats?.available_space ?? 0)}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default DashboardPage
