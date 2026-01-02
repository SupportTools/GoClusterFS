/**
 * Circular Gauge Components - SVG-based circular progress indicators
 *
 * These gauges display percentage values in a visually appealing circular format
 * with configurable colors, sizes, and warning thresholds.
 */

import { useMemo } from 'react'
import { formatPercent, formatBytes, formatNumber } from '@/utils/format'

/**
 * Color thresholds for gauge status indication
 */
interface ThresholdColors {
  ok: string
  warning: string
  critical: string
}

const DEFAULT_COLORS: ThresholdColors = {
  ok: '#48bb78',      // gfs-status-ok (green)
  warning: '#ed8936', // gfs-status-warning (orange)
  critical: '#f56565', // gfs-status-error (red)
}

/**
 * Get color based on percentage and thresholds
 */
function getColorForPercent(
  percent: number,
  warningThreshold: number,
  criticalThreshold: number,
  colors: ThresholdColors = DEFAULT_COLORS
): string {
  if (percent >= criticalThreshold) return colors.critical
  if (percent >= warningThreshold) return colors.warning
  return colors.ok
}

/**
 * Base props for all circular gauges
 */
interface CircularGaugeBaseProps {
  /** Current percentage (0-100) */
  percent: number
  /** Size in pixels (default: 120) */
  size?: number
  /** Stroke width (default: 8) */
  strokeWidth?: number
  /** Warning threshold percentage (default: 75) */
  warningThreshold?: number
  /** Critical threshold percentage (default: 90) */
  criticalThreshold?: number
  /** Custom color overrides */
  colors?: Partial<ThresholdColors>
  /** Label text below the value */
  label?: string
  /** Show percentage text (default: true) */
  showPercent?: boolean
  /** Animation duration in ms (default: 500) */
  animationDuration?: number
}

/**
 * Basic circular gauge showing a percentage
 */
export function CircularGauge({
  percent,
  size = 120,
  strokeWidth = 8,
  warningThreshold = 75,
  criticalThreshold = 90,
  colors,
  label,
  showPercent = true,
  animationDuration = 500,
}: CircularGaugeBaseProps) {
  const mergedColors = { ...DEFAULT_COLORS, ...colors }

  // Calculate SVG dimensions
  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2

  // Clamp percent to 0-100
  const clampedPercent = Math.min(100, Math.max(0, percent))

  // Calculate stroke offset for the progress arc
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  // Get color based on thresholds
  const strokeColor = getColorForPercent(
    clampedPercent,
    warningThreshold,
    criticalThreshold,
    mergedColors
  )

  return (
    <div className="flex flex-col items-center">
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        className="transform -rotate-90"
      >
        {/* Background circle */}
        <circle
          cx={center}
          cy={center}
          r={radius}
          fill="none"
          stroke="#2d3748"
          strokeWidth={strokeWidth}
          className="opacity-30"
        />
        {/* Progress circle */}
        <circle
          cx={center}
          cy={center}
          r={radius}
          fill="none"
          stroke={strokeColor}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={strokeDashoffset}
          style={{
            transition: `stroke-dashoffset ${animationDuration}ms ease-out, stroke ${animationDuration}ms ease-out`,
          }}
        />
      </svg>
      {/* Center text overlay */}
      <div
        className="absolute flex flex-col items-center justify-center"
        style={{ width: size, height: size, marginTop: -size }}
      >
        {showPercent && (
          <span
            className="font-bold text-gfs-text-primary"
            style={{ fontSize: size / 5 }}
          >
            {formatPercent(clampedPercent)}
          </span>
        )}
      </div>
      {/* Label below gauge */}
      {label && (
        <span className="mt-2 text-sm text-gfs-text-muted">{label}</span>
      )}
    </div>
  )
}

/**
 * Props for capacity gauge with byte values
 */
interface CapacityGaugeProps extends Omit<CircularGaugeBaseProps, 'percent' | 'showPercent'> {
  /** Used bytes */
  usedBytes: number
  /** Total bytes */
  totalBytes: number
  /** Show used/total values (default: true) */
  showValues?: boolean
}

/**
 * Capacity gauge showing storage usage with byte values
 */
export function CapacityGauge({
  usedBytes,
  totalBytes,
  size = 140,
  strokeWidth = 10,
  warningThreshold = 75,
  criticalThreshold = 90,
  colors,
  label = 'Storage',
  showValues = true,
  animationDuration = 500,
}: CapacityGaugeProps) {
  const percent = useMemo(() => {
    if (totalBytes <= 0) return 0
    return (usedBytes / totalBytes) * 100
  }, [usedBytes, totalBytes])

  const mergedColors = { ...DEFAULT_COLORS, ...colors }

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2
  const clampedPercent = Math.min(100, Math.max(0, percent))
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  const strokeColor = getColorForPercent(
    clampedPercent,
    warningThreshold,
    criticalThreshold,
    mergedColors
  )

  return (
    <div className="flex flex-col items-center">
      <div className="relative">
        <svg
          width={size}
          height={size}
          viewBox={`0 0 ${size} ${size}`}
          className="transform -rotate-90"
        >
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#2d3748"
            strokeWidth={strokeWidth}
            className="opacity-30"
          />
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke={strokeColor}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            style={{
              transition: `stroke-dashoffset ${animationDuration}ms ease-out, stroke ${animationDuration}ms ease-out`,
            }}
          />
        </svg>
        {/* Center content */}
        <div
          className="absolute inset-0 flex flex-col items-center justify-center"
        >
          <span
            className="font-bold"
            style={{ fontSize: size / 5, color: strokeColor }}
          >
            {formatPercent(clampedPercent)}
          </span>
          {showValues && (
            <span className="text-xs text-gfs-text-muted mt-1">
              {formatBytes(usedBytes)}
            </span>
          )}
        </div>
      </div>
      {label && (
        <span className="mt-2 text-sm text-gfs-text-muted">{label}</span>
      )}
      {showValues && (
        <span className="text-xs text-gfs-text-muted">
          of {formatBytes(totalBytes)}
        </span>
      )}
    </div>
  )
}

/**
 * Props for inode gauge
 */
interface InodeGaugeProps extends Omit<CircularGaugeBaseProps, 'percent' | 'showPercent'> {
  /** Used inodes */
  usedInodes: number
  /** Total inodes */
  totalInodes: number
  /** Show inode counts (default: true) */
  showCounts?: boolean
}

/**
 * Inode gauge showing inode usage
 */
export function InodeGauge({
  usedInodes,
  totalInodes,
  size = 140,
  strokeWidth = 10,
  warningThreshold = 80,
  criticalThreshold = 95,
  colors,
  label = 'Inodes',
  showCounts = true,
  animationDuration = 500,
}: InodeGaugeProps) {
  const percent = useMemo(() => {
    if (totalInodes <= 0) return 0
    return (usedInodes / totalInodes) * 100
  }, [usedInodes, totalInodes])

  const mergedColors = { ...DEFAULT_COLORS, ...colors }

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2
  const clampedPercent = Math.min(100, Math.max(0, percent))
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  const strokeColor = getColorForPercent(
    clampedPercent,
    warningThreshold,
    criticalThreshold,
    mergedColors
  )

  return (
    <div className="flex flex-col items-center">
      <div className="relative">
        <svg
          width={size}
          height={size}
          viewBox={`0 0 ${size} ${size}`}
          className="transform -rotate-90"
        >
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#2d3748"
            strokeWidth={strokeWidth}
            className="opacity-30"
          />
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke={strokeColor}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            style={{
              transition: `stroke-dashoffset ${animationDuration}ms ease-out, stroke ${animationDuration}ms ease-out`,
            }}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span
            className="font-bold"
            style={{ fontSize: size / 5, color: strokeColor }}
          >
            {formatPercent(clampedPercent)}
          </span>
          {showCounts && (
            <span className="text-xs text-gfs-text-muted mt-1">
              {formatNumber(usedInodes)}
            </span>
          )}
        </div>
      </div>
      {label && (
        <span className="mt-2 text-sm text-gfs-text-muted">{label}</span>
      )}
      {showCounts && (
        <span className="text-xs text-gfs-text-muted">
          of {formatNumber(totalInodes)}
        </span>
      )}
    </div>
  )
}

/**
 * Props for block gauge
 */
interface BlockGaugeProps extends Omit<CircularGaugeBaseProps, 'percent' | 'showPercent'> {
  /** Used blocks */
  usedBlocks: number
  /** Total blocks */
  totalBlocks: number
  /** Show block counts (default: true) */
  showCounts?: boolean
}

/**
 * Block gauge showing block allocation
 */
export function BlockGauge({
  usedBlocks,
  totalBlocks,
  size = 140,
  strokeWidth = 10,
  warningThreshold = 75,
  criticalThreshold = 90,
  colors,
  label = 'Blocks',
  showCounts = true,
  animationDuration = 500,
}: BlockGaugeProps) {
  const percent = useMemo(() => {
    if (totalBlocks <= 0) return 0
    return (usedBlocks / totalBlocks) * 100
  }, [usedBlocks, totalBlocks])

  const mergedColors = { ...DEFAULT_COLORS, ...colors }

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2
  const clampedPercent = Math.min(100, Math.max(0, percent))
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  const strokeColor = getColorForPercent(
    clampedPercent,
    warningThreshold,
    criticalThreshold,
    mergedColors
  )

  return (
    <div className="flex flex-col items-center">
      <div className="relative">
        <svg
          width={size}
          height={size}
          viewBox={`0 0 ${size} ${size}`}
          className="transform -rotate-90"
        >
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#2d3748"
            strokeWidth={strokeWidth}
            className="opacity-30"
          />
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke={strokeColor}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            style={{
              transition: `stroke-dashoffset ${animationDuration}ms ease-out, stroke ${animationDuration}ms ease-out`,
            }}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span
            className="font-bold"
            style={{ fontSize: size / 5, color: strokeColor }}
          >
            {formatPercent(clampedPercent)}
          </span>
          {showCounts && (
            <span className="text-xs text-gfs-text-muted mt-1">
              {formatNumber(usedBlocks)}
            </span>
          )}
        </div>
      </div>
      {label && (
        <span className="mt-2 text-sm text-gfs-text-muted">{label}</span>
      )}
      {showCounts && (
        <span className="text-xs text-gfs-text-muted">
          of {formatNumber(totalBlocks)}
        </span>
      )}
    </div>
  )
}

/**
 * Mini gauge for compact displays (e.g., in cards or tables)
 */
interface MiniGaugeProps {
  percent: number
  size?: number
  strokeWidth?: number
  warningThreshold?: number
  criticalThreshold?: number
  colors?: Partial<ThresholdColors>
}

export function MiniGauge({
  percent,
  size = 40,
  strokeWidth = 4,
  warningThreshold = 75,
  criticalThreshold = 90,
  colors,
}: MiniGaugeProps) {
  const mergedColors = { ...DEFAULT_COLORS, ...colors }

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2
  const clampedPercent = Math.min(100, Math.max(0, percent))
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  const strokeColor = getColorForPercent(
    clampedPercent,
    warningThreshold,
    criticalThreshold,
    mergedColors
  )

  return (
    <div className="relative inline-flex items-center justify-center">
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        className="transform -rotate-90"
      >
        <circle
          cx={center}
          cy={center}
          r={radius}
          fill="none"
          stroke="#2d3748"
          strokeWidth={strokeWidth}
          className="opacity-30"
        />
        <circle
          cx={center}
          cy={center}
          r={radius}
          fill="none"
          stroke={strokeColor}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={strokeDashoffset}
          style={{
            transition: 'stroke-dashoffset 300ms ease-out, stroke 300ms ease-out',
          }}
        />
      </svg>
      <span
        className="absolute font-semibold"
        style={{ fontSize: size / 4, color: strokeColor }}
      >
        {Math.round(clampedPercent)}
      </span>
    </div>
  )
}

/**
 * Props for cache hit rate gauge
 */
interface HitRateGaugeProps {
  /** Number of cache hits */
  hits: number
  /** Number of cache misses */
  misses: number
  /** Size in pixels (default: 140) */
  size?: number
  /** Stroke width (default: 10) */
  strokeWidth?: number
  /** Label text (default: 'Hit Rate') */
  label?: string
  /** Show hit/miss counts (default: true) */
  showCounts?: boolean
  /** Animation duration in ms (default: 500) */
  animationDuration?: number
}

/**
 * Cache hit rate gauge - shows cache performance with inverted thresholds
 * (higher is better, so green >= 80%, yellow >= 50%, red < 50%)
 */
export function HitRateGauge({
  hits,
  misses,
  size = 140,
  strokeWidth = 10,
  label = 'Hit Rate',
  showCounts = true,
  animationDuration = 500,
}: HitRateGaugeProps) {
  const percent = useMemo(() => {
    const total = hits + misses
    if (total <= 0) return 0
    return (hits / total) * 100
  }, [hits, misses])

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2
  const clampedPercent = Math.min(100, Math.max(0, percent))
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  // Custom color logic for hit rate (higher = better)
  const strokeColor =
    clampedPercent >= 80
      ? '#48bb78' // green (good)
      : clampedPercent >= 50
        ? '#ed8936' // orange (warning)
        : '#f56565' // red (bad)

  return (
    <div className="flex flex-col items-center">
      <div className="relative">
        <svg
          width={size}
          height={size}
          viewBox={`0 0 ${size} ${size}`}
          className="transform -rotate-90"
        >
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#2d3748"
            strokeWidth={strokeWidth}
            className="opacity-30"
          />
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke={strokeColor}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            style={{
              transition: `stroke-dashoffset ${animationDuration}ms ease-out, stroke ${animationDuration}ms ease-out`,
            }}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span
            className="font-bold"
            style={{ fontSize: size / 5, color: strokeColor }}
          >
            {formatPercent(clampedPercent)}
          </span>
          {showCounts && (
            <span className="text-xs text-gfs-text-muted mt-1">
              {formatNumber(hits)} / {formatNumber(hits + misses)}
            </span>
          )}
        </div>
      </div>
      {label && (
        <span className="mt-2 text-sm text-gfs-text-muted">{label}</span>
      )}
    </div>
  )
}

/**
 * Props for journal usage gauge
 */
interface JournalGaugeProps {
  /** Bytes written to journal */
  bytesWritten?: number
  /** Available space in journal */
  availableSpace?: number
  /** Total journal size (bytesWritten + availableSpace if not provided) */
  totalSize?: number
  /** Journal state (e.g., 'active', 'flushing', 'idle') */
  state?: string
  /** Number of commits */
  commitCount?: number
  /** Number of active transactions */
  activeTxnCount?: number
  /** Size in pixels (default: 140) */
  size?: number
  /** Stroke width (default: 10) */
  strokeWidth?: number
  /** Animation duration in ms (default: 500) */
  animationDuration?: number
}

/**
 * Journal usage gauge with state indicator
 */
export function JournalGauge({
  bytesWritten = 0,
  availableSpace = 0,
  totalSize,
  state = 'unknown',
  commitCount = 0,
  activeTxnCount = 0,
  size = 140,
  strokeWidth = 10,
  animationDuration = 500,
}: JournalGaugeProps) {
  // Calculate utilization
  const total = totalSize ?? (bytesWritten + availableSpace)
  const percent = useMemo(() => {
    if (total <= 0) return 0
    return ((total - availableSpace) / total) * 100
  }, [total, availableSpace])

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2
  const clampedPercent = Math.min(100, Math.max(0, percent))
  const strokeDashoffset = circumference * (1 - clampedPercent / 100)

  // Color based on utilization (higher = worse for journal)
  const strokeColor =
    clampedPercent >= 90
      ? '#f56565' // red (critical)
      : clampedPercent >= 75
        ? '#ed8936' // orange (warning)
        : '#48bb78' // green (ok)

  // State badge color
  const stateColorMap: Record<string, string> = {
    active: 'bg-gfs-status-ok',
    flushing: 'bg-gfs-status-warning',
    idle: 'bg-gfs-status-info',
    error: 'bg-gfs-status-error',
    unknown: 'bg-gfs-text-muted',
  }
  const stateBgColor = stateColorMap[state.toLowerCase()] || stateColorMap.unknown

  return (
    <div className="flex flex-col items-center">
      <div className="relative">
        <svg
          width={size}
          height={size}
          viewBox={`0 0 ${size} ${size}`}
          className="transform -rotate-90"
        >
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#2d3748"
            strokeWidth={strokeWidth}
            className="opacity-30"
          />
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke={strokeColor}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            style={{
              transition: `stroke-dashoffset ${animationDuration}ms ease-out, stroke ${animationDuration}ms ease-out`,
            }}
          />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span
            className="font-bold"
            style={{ fontSize: size / 5, color: strokeColor }}
          >
            {formatPercent(clampedPercent)}
          </span>
          <span className="text-xs text-gfs-text-muted">
            utilized
          </span>
        </div>
      </div>

      {/* State badge */}
      <div className="mt-2 flex items-center gap-2">
        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${stateBgColor} text-white`}>
          <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />
          {state}
        </span>
      </div>

      {/* Stats row */}
      <div className="mt-2 flex gap-4 text-xs text-gfs-text-muted">
        <span>{formatNumber(commitCount)} commits</span>
        {activeTxnCount > 0 && (
          <span className="text-gfs-status-warning">
            {activeTxnCount} active
          </span>
        )}
      </div>
    </div>
  )
}

export default CircularGauge
