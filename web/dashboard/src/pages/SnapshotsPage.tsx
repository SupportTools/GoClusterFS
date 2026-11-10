/**
 * Snapshots page - list and manage filesystem snapshots
 */

import { useState, useMemo } from 'react'
import { useSnapshotsData } from '@/hooks'
import { CreateSnapshotModal, ConfirmDeleteModal } from '@/components'
import { api } from '@/api'
import type { SnapshotInfo, SnapshotStatus } from '@/api'
import { formatBytes, formatRelativeTime } from '@/utils/format'

// Sort directions
type SortDirection = 'asc' | 'desc'
type SortField = 'name' | 'created_at' | 'size_bytes' | 'status'

/**
 * Status badge component
 */
function StatusBadge({ status }: { status: SnapshotStatus }) {
  const statusStyles: Record<SnapshotStatus, string> = {
    active: 'bg-gfs-status-ok text-white',
    creating: 'bg-gfs-status-info text-white animate-pulse',
    deleting: 'bg-gfs-status-warning text-white animate-pulse',
    error: 'bg-gfs-status-error text-white',
    archived: 'bg-gfs-text-muted text-white',
  }

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${statusStyles[status] || statusStyles.active}`}>
      {status}
    </span>
  )
}

/**
 * Sort indicator icon
 */
function SortIcon({ direction, active }: { direction: SortDirection; active: boolean }) {
  if (!active) {
    return (
      <svg className="w-4 h-4 text-gfs-text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8 9l4-4 4 4m0 6l-4 4-4-4" />
      </svg>
    )
  }

  return (
    <svg className="w-4 h-4 text-gfs-accent-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      {direction === 'asc' ? (
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
      ) : (
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
      )}
    </svg>
  )
}

/**
 * Sortable table header
 */
function SortableHeader({
  label,
  field,
  currentSort,
  currentDirection,
  onSort,
}: {
  label: string
  field: SortField
  currentSort: SortField
  currentDirection: SortDirection
  onSort: (field: SortField) => void
}) {
  const isActive = currentSort === field

  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium text-gfs-text-muted uppercase tracking-wider cursor-pointer hover:text-gfs-text-primary transition-colors"
      onClick={() => onSort(field)}
    >
      <div className="flex items-center gap-1">
        {label}
        <SortIcon direction={currentDirection} active={isActive} />
      </div>
    </th>
  )
}

/**
 * Filter dropdown
 */
function StatusFilter({
  value,
  onChange,
}: {
  value: SnapshotStatus | 'all'
  onChange: (value: SnapshotStatus | 'all') => void
}) {
  const options: Array<{ value: SnapshotStatus | 'all'; label: string }> = [
    { value: 'all', label: 'All Status' },
    { value: 'active', label: 'Active' },
    { value: 'creating', label: 'Creating' },
    { value: 'deleting', label: 'Deleting' },
    { value: 'error', label: 'Error' },
    { value: 'archived', label: 'Archived' },
  ]

  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value as SnapshotStatus | 'all')}
      className="px-3 py-1.5 bg-gfs-bg-tertiary border border-gfs-border rounded text-sm text-gfs-text-primary focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary"
    >
      {options.map((opt) => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  )
}

/**
 * Search input
 */
function SearchInput({
  value,
  onChange,
}: {
  value: string
  onChange: (value: string) => void
}) {
  return (
    <div className="relative">
      <svg
        className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gfs-text-muted"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
        />
      </svg>
      <input
        type="text"
        placeholder="Search snapshots..."
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="pl-9 pr-4 py-1.5 bg-gfs-bg-tertiary border border-gfs-border rounded text-sm text-gfs-text-primary placeholder-gfs-text-muted focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary w-64"
      />
    </div>
  )
}

/**
 * Snapshot table row
 */
function SnapshotRow({
  snapshot,
  onDelete,
}: {
  snapshot: SnapshotInfo
  onDelete: (snapshot: SnapshotInfo) => void
}) {
  const createdAt = formatRelativeTime(new Date(snapshot.created_at))
  const expiresAt = snapshot.expires_at
    ? formatRelativeTime(new Date(snapshot.expires_at))
    : 'Never'

  const canDelete = snapshot.status !== 'deleting' && snapshot.status !== 'creating'

  return (
    <tr className="border-b border-gfs-border hover:bg-gfs-bg-tertiary/50 transition-colors">
      <td className="px-4 py-3">
        <div>
          <div className="text-sm font-medium text-gfs-text-primary">{snapshot.name}</div>
          {snapshot.description && (
            <div className="text-xs text-gfs-text-muted truncate max-w-xs">
              {snapshot.description}
            </div>
          )}
        </div>
      </td>
      <td className="px-4 py-3">
        <StatusBadge status={snapshot.status} />
        {snapshot.progress_percent !== undefined && snapshot.status !== 'active' && (
          <div className="mt-1 w-20 h-1 bg-gfs-bg-tertiary rounded overflow-hidden">
            <div
              className="h-full bg-gfs-accent-primary transition-all"
              style={{ width: `${snapshot.progress_percent}%` }}
            />
          </div>
        )}
        {snapshot.error_message && (
          <div className="text-xs text-gfs-status-error mt-1 truncate max-w-xs">
            {snapshot.error_message}
          </div>
        )}
      </td>
      <td className="px-4 py-3 text-sm text-gfs-text-secondary">
        {formatBytes(snapshot.size_bytes)}
      </td>
      <td className="px-4 py-3 text-sm text-gfs-text-muted">{createdAt}</td>
      <td className="px-4 py-3 text-sm text-gfs-text-muted">{expiresAt}</td>
      <td className="px-4 py-3">
        <div className="flex items-center gap-2">
          <button
            className="p-1 text-gfs-text-muted hover:text-gfs-text-primary transition-colors"
            title="View details"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
            </svg>
          </button>
          <button
            className={`p-1 transition-colors ${
              canDelete
                ? 'text-gfs-status-error hover:text-gfs-status-error/80'
                : 'text-gfs-text-muted cursor-not-allowed opacity-50'
            }`}
            title={canDelete ? 'Delete snapshot' : 'Cannot delete while in progress'}
            disabled={!canDelete}
            onClick={() => canDelete && onDelete(snapshot)}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
            </svg>
          </button>
        </div>
      </td>
    </tr>
  )
}

/**
 * Empty state
 */
function EmptyState() {
  return (
    <div className="text-center py-12">
      <div className="text-4xl mb-4">📸</div>
      <h3 className="text-lg font-medium text-gfs-text-primary mb-2">No Snapshots</h3>
      <p className="text-sm text-gfs-text-muted max-w-md mx-auto">
        Create your first snapshot to preserve a point-in-time copy of your filesystem.
      </p>
      <button className="mt-4 px-4 py-2 bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover transition-colors">
        Create Snapshot
      </button>
    </div>
  )
}

/**
 * Loading skeleton
 */
function SnapshotsSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-8 bg-gfs-bg-tertiary rounded w-48" />
      <div className="flex gap-4">
        <div className="h-9 bg-gfs-bg-tertiary rounded w-64" />
        <div className="h-9 bg-gfs-bg-tertiary rounded w-32" />
      </div>
      <div className="card">
        <div className="space-y-4">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="h-12 bg-gfs-bg-tertiary rounded" />
          ))}
        </div>
      </div>
    </div>
  )
}

/**
 * Error state
 */
function SnapshotsError({ error, onRetry }: { error: Error; onRetry: () => void }) {
  return (
    <div className="card border-gfs-status-error">
      <div className="text-center py-8">
        <div className="text-gfs-status-error text-lg font-medium mb-2">
          Failed to load snapshots
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

export function SnapshotsPage() {
  const { snapshots, loading, error } = useSnapshotsData()

  // Modal state
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [snapshotToDelete, setSnapshotToDelete] = useState<SnapshotInfo | null>(null)

  // Table state
  const [sortField, setSortField] = useState<SortField>('created_at')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')
  const [statusFilter, setStatusFilter] = useState<SnapshotStatus | 'all'>('all')
  const [searchQuery, setSearchQuery] = useState('')

  // Handle sort
  const handleSort = (field: SortField) => {
    if (field === sortField) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDirection('desc')
    }
  }

  // Filter and sort snapshots
  const filteredSnapshots = useMemo(() => {
    const snapshotsList = snapshots.data?.snapshots
    if (!snapshotsList || snapshotsList.length === 0) return []

    let result = [...snapshotsList]

    // Apply status filter
    if (statusFilter !== 'all') {
      result = result.filter((s) => s.status === statusFilter)
    }

    // Apply search filter
    if (searchQuery) {
      const query = searchQuery.toLowerCase()
      result = result.filter(
        (s) =>
          s.name.toLowerCase().includes(query) ||
          s.description?.toLowerCase().includes(query) ||
          s.snapshot_id.toLowerCase().includes(query)
      )
    }

    // Apply sorting
    result.sort((a, b) => {
      let comparison = 0

      switch (sortField) {
        case 'name':
          comparison = a.name.localeCompare(b.name)
          break
        case 'created_at':
          comparison = new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
          break
        case 'size_bytes':
          comparison = a.size_bytes - b.size_bytes
          break
        case 'status':
          comparison = a.status.localeCompare(b.status)
          break
      }

      return sortDirection === 'asc' ? comparison : -comparison
    })

    return result
  }, [snapshots.data, statusFilter, searchQuery, sortField, sortDirection])

  if (loading && !snapshots.data) {
    return <SnapshotsSkeleton />
  }

  if (error && !snapshots.data) {
    return <SnapshotsError error={error} onRetry={snapshots.refetch} />
  }

  const snapshotsData = snapshots.data
  const hasSnapshots = snapshotsData && snapshotsData.snapshots && snapshotsData.snapshots.length > 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gfs-text-primary">Snapshots</h1>
          <p className="text-sm text-gfs-text-muted mt-1">
            Manage filesystem snapshots
          </p>
        </div>
        <button
          onClick={() => setIsCreateModalOpen(true)}
          className="px-4 py-2 bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover transition-colors flex items-center gap-2"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          Create Snapshot
        </button>
      </div>

      {/* Stats summary */}
      {hasSnapshots && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <div className="card">
            <div className="text-sm text-gfs-text-muted">Total Snapshots</div>
            <div className="text-2xl font-bold text-gfs-text-primary">
              {snapshotsData.total_count}
            </div>
          </div>
          <div className="card">
            <div className="text-sm text-gfs-text-muted">Total Size</div>
            <div className="text-2xl font-bold text-gfs-text-primary">
              {formatBytes(snapshotsData.total_size_bytes)}
            </div>
          </div>
          <div className="card">
            <div className="text-sm text-gfs-text-muted">Active</div>
            <div className="text-2xl font-bold text-gfs-status-ok">
              {snapshotsData.snapshots.filter((s) => s.status === 'active').length}
            </div>
          </div>
          <div className="card">
            <div className="text-sm text-gfs-text-muted">Errors</div>
            <div className="text-2xl font-bold text-gfs-status-error">
              {snapshotsData.snapshots.filter((s) => s.status === 'error').length}
            </div>
          </div>
        </div>
      )}

      {!hasSnapshots ? (
        <div className="card">
          <EmptyState />
        </div>
      ) : (
        <>
          {/* Filters */}
          <div className="flex items-center gap-4">
            <SearchInput value={searchQuery} onChange={setSearchQuery} />
            <StatusFilter value={statusFilter} onChange={setStatusFilter} />
            <span className="text-sm text-gfs-text-muted ml-auto">
              Showing {filteredSnapshots.length} of {snapshotsData.snapshots.length} snapshots
            </span>
          </div>

          {/* Table */}
          <div className="card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gfs-bg-tertiary">
                  <tr>
                    <SortableHeader
                      label="Name"
                      field="name"
                      currentSort={sortField}
                      currentDirection={sortDirection}
                      onSort={handleSort}
                    />
                    <SortableHeader
                      label="Status"
                      field="status"
                      currentSort={sortField}
                      currentDirection={sortDirection}
                      onSort={handleSort}
                    />
                    <SortableHeader
                      label="Size"
                      field="size_bytes"
                      currentSort={sortField}
                      currentDirection={sortDirection}
                      onSort={handleSort}
                    />
                    <SortableHeader
                      label="Created"
                      field="created_at"
                      currentSort={sortField}
                      currentDirection={sortDirection}
                      onSort={handleSort}
                    />
                    <th className="px-4 py-3 text-left text-xs font-medium text-gfs-text-muted uppercase tracking-wider">
                      Expires
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gfs-text-muted uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredSnapshots.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-4 py-8 text-center text-gfs-text-muted">
                        No snapshots match your filters
                      </td>
                    </tr>
                  ) : (
                    filteredSnapshots.map((snapshot) => (
                      <SnapshotRow
                        key={snapshot.snapshot_id}
                        snapshot={snapshot}
                        onDelete={setSnapshotToDelete}
                      />
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {/* Create Snapshot Modal */}
      <CreateSnapshotModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onSuccess={() => snapshots.refetch()}
      />

      {/* Delete Confirmation Modal - key ensures fresh state when snapshot changes */}
      <ConfirmDeleteModal
        key={snapshotToDelete?.snapshot_id ?? 'no-snapshot'}
        isOpen={snapshotToDelete !== null}
        onClose={() => setSnapshotToDelete(null)}
        onConfirm={async () => {
          if (!snapshotToDelete) return
          await api.deleteSnapshot(snapshotToDelete.snapshot_id)
          snapshots.refetch()
        }}
        title="Delete Snapshot"
        itemName={snapshotToDelete?.name ?? ''}
        itemType="snapshot"
        warning="This will permanently delete the snapshot and all its data. This action cannot be undone."
        requireTypeToConfirm={true}
      />
    </div>
  )
}

export default SnapshotsPage
