/**
 * Cache Performance Charts - visualizations for cache metrics
 *
 * Uses Recharts for rendering line/area/bar charts showing:
 * - Cache hit rate over time
 * - Cache size/memory usage
 * - Per-cache breakdown (inode, extent, lock hint)
 */

import { useMemo } from 'react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import type { CacheHitRateInfo } from '@/api'
import { formatNumber, formatPercent } from '@/utils/format'

// Chart theme colors matching GoClusterFS design system
const CHART_COLORS = {
  primary: '#63b3ed', // gfs-accent-primary
  secondary: '#4299e1', // gfs-accent-secondary
  success: '#48bb78', // gfs-status-ok
  warning: '#ed8936', // gfs-status-warning
  error: '#f56565', // gfs-status-error
  grid: '#2d3748', // gfs-border
  text: '#a0aec0', // gfs-text-secondary
}

// Custom tooltip styles
const tooltipStyle = {
  backgroundColor: '#1a1a2e',
  border: '1px solid #2d3748',
  borderRadius: '0.375rem',
  padding: '0.5rem 0.75rem',
  color: '#e2e8f0',
  fontSize: '0.875rem',
}

/**
 * Props for cache breakdown bar chart
 */
interface CacheBreakdownChartProps {
  inodeCache?: CacheHitRateInfo
  extentCache?: CacheHitRateInfo
  lockHintCache?: CacheHitRateInfo
}

/**
 * Bar chart showing cache breakdown by type
 */
export function CacheBreakdownChart({
  inodeCache,
  extentCache,
  lockHintCache,
}: CacheBreakdownChartProps) {
  const data = useMemo(() => [
    {
      name: 'Inode',
      hits: inodeCache?.hits ?? 0,
      misses: inodeCache?.misses ?? 0,
      size: inodeCache?.size ?? 0,
      hitRate: (inodeCache?.hit_rate ?? 0) * 100,
    },
    {
      name: 'Extent',
      hits: extentCache?.hits ?? 0,
      misses: extentCache?.misses ?? 0,
      size: extentCache?.size ?? 0,
      hitRate: (extentCache?.hit_rate ?? 0) * 100,
    },
    {
      name: 'Lock Hint',
      hits: lockHintCache?.hits ?? 0,
      misses: lockHintCache?.misses ?? 0,
      size: lockHintCache?.size ?? 0,
      hitRate: (lockHintCache?.hit_rate ?? 0) * 100,
    },
  ], [inodeCache, extentCache, lockHintCache])

  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ left: 20, right: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={CHART_COLORS.grid} />
          <XAxis
            type="number"
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
            tickFormatter={(value) => formatNumber(value)}
          />
          <YAxis
            dataKey="name"
            type="category"
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
            width={70}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value, name) => [
              formatNumber(Number(value ?? 0)),
              name === 'hits' ? 'Hits' : 'Misses',
            ]}
          />
          <Legend
            wrapperStyle={{ paddingTop: '10px' }}
            formatter={(value) => (
              <span style={{ color: CHART_COLORS.text }}>
                {value === 'hits' ? 'Hits' : 'Misses'}
              </span>
            )}
          />
          <Bar dataKey="hits" fill={CHART_COLORS.success} stackId="a" />
          <Bar dataKey="misses" fill={CHART_COLORS.error} stackId="a" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * Props for hit rate line chart
 */
interface HitRateChartProps {
  inodeCache?: CacheHitRateInfo
  extentCache?: CacheHitRateInfo
  lockHintCache?: CacheHitRateInfo
}

/**
 * Line chart showing hit rates for each cache type
 */
export function HitRateChart({
  inodeCache,
  extentCache,
  lockHintCache,
}: HitRateChartProps) {
  const data = useMemo(() => [
    {
      name: 'Inode',
      hitRate: (inodeCache?.hit_rate ?? 0) * 100,
    },
    {
      name: 'Extent',
      hitRate: (extentCache?.hit_rate ?? 0) * 100,
    },
    {
      name: 'Lock Hint',
      hitRate: (lockHintCache?.hit_rate ?? 0) * 100,
    },
  ], [inodeCache, extentCache, lockHintCache])

  return (
    <div className="h-48">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ left: 0, right: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={CHART_COLORS.grid} />
          <XAxis
            dataKey="name"
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
          />
          <YAxis
            domain={[0, 100]}
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
            tickFormatter={(value) => `${value}%`}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value) => [formatPercent(Number(value ?? 0)), 'Hit Rate']}
          />
          <Bar
            dataKey="hitRate"
            fill={CHART_COLORS.primary}
            radius={[4, 4, 0, 0]}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * Props for cache size chart
 */
interface CacheSizeChartProps {
  inodeCache?: CacheHitRateInfo
  extentCache?: CacheHitRateInfo
  lockHintCache?: CacheHitRateInfo
}

/**
 * Area chart showing cache sizes
 */
export function CacheSizeChart({
  inodeCache,
  extentCache,
  lockHintCache,
}: CacheSizeChartProps) {
  const data = useMemo(() => [
    {
      name: 'Inode',
      size: inodeCache?.size ?? 0,
    },
    {
      name: 'Extent',
      size: extentCache?.size ?? 0,
    },
    {
      name: 'Lock Hint',
      size: lockHintCache?.size ?? 0,
    },
  ], [inodeCache, extentCache, lockHintCache])

  return (
    <div className="h-48">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ left: 0, right: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={CHART_COLORS.grid} />
          <XAxis
            dataKey="name"
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
          />
          <YAxis
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
            tickFormatter={(value) => formatNumber(value)}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value) => [formatNumber(Number(value ?? 0)), 'Entries']}
          />
          <Bar
            dataKey="size"
            fill={CHART_COLORS.secondary}
            radius={[4, 4, 0, 0]}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * Props for time series data
 */
interface TimeSeriesPoint {
  timestamp: string
  inodeHitRate: number
  extentHitRate: number
  lockHintHitRate: number
}

interface TimeSeriesChartProps {
  data: TimeSeriesPoint[]
}

/**
 * Line chart for hit rate over time
 * Note: This requires historical data to be collected by the API
 */
export function HitRateTimeSeriesChart({ data }: TimeSeriesChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-gfs-text-muted">
        <p>No historical data available</p>
      </div>
    )
  }

  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ left: 0, right: 20, top: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={CHART_COLORS.grid} />
          <XAxis
            dataKey="timestamp"
            tick={{ fill: CHART_COLORS.text, fontSize: 11 }}
            tickFormatter={(value) => {
              const date = new Date(value)
              return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            }}
          />
          <YAxis
            domain={[0, 100]}
            tick={{ fill: CHART_COLORS.text, fontSize: 12 }}
            tickFormatter={(value) => `${value}%`}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value, name) => [
              formatPercent(Number(value ?? 0)),
              String(name).replace('HitRate', ' Hit Rate'),
            ]}
            labelFormatter={(value) => new Date(String(value)).toLocaleString()}
          />
          <Legend
            wrapperStyle={{ paddingTop: '10px' }}
            formatter={(value) => (
              <span style={{ color: CHART_COLORS.text, fontSize: '12px' }}>
                {value.replace('HitRate', '')}
              </span>
            )}
          />
          <Line
            type="monotone"
            dataKey="inodeHitRate"
            name="Inode"
            stroke={CHART_COLORS.primary}
            strokeWidth={2}
            dot={false}
          />
          <Line
            type="monotone"
            dataKey="extentHitRate"
            name="Extent"
            stroke={CHART_COLORS.success}
            strokeWidth={2}
            dot={false}
          />
          <Line
            type="monotone"
            dataKey="lockHintHitRate"
            name="Lock Hint"
            stroke={CHART_COLORS.warning}
            strokeWidth={2}
            dot={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

/**
 * Props for the combined cache stats panel
 */
interface CacheStatsPanelProps {
  inodeCache?: CacheHitRateInfo
  extentCache?: CacheHitRateInfo
  lockHintCache?: CacheHitRateInfo
  timeSeries?: TimeSeriesPoint[]
}

/**
 * Combined panel showing all cache charts
 */
export function CacheStatsPanel({
  inodeCache,
  extentCache,
  lockHintCache,
  timeSeries,
}: CacheStatsPanelProps) {
  return (
    <div className="space-y-6">
      {/* Hit rates by cache type */}
      <div className="card">
        <h3 className="card-header">Cache Hit Rates</h3>
        <HitRateChart
          inodeCache={inodeCache}
          extentCache={extentCache}
          lockHintCache={lockHintCache}
        />
      </div>

      {/* Cache sizes */}
      <div className="card">
        <h3 className="card-header">Cache Sizes</h3>
        <CacheSizeChart
          inodeCache={inodeCache}
          extentCache={extentCache}
          lockHintCache={lockHintCache}
        />
      </div>

      {/* Hits vs Misses breakdown */}
      <div className="card">
        <h3 className="card-header">Hits vs Misses</h3>
        <CacheBreakdownChart
          inodeCache={inodeCache}
          extentCache={extentCache}
          lockHintCache={lockHintCache}
        />
      </div>

      {/* Time series (if available) */}
      {timeSeries && timeSeries.length > 0 && (
        <div className="card">
          <h3 className="card-header">Hit Rate Over Time</h3>
          <HitRateTimeSeriesChart data={timeSeries} />
        </div>
      )}
    </div>
  )
}

export default CacheStatsPanel
