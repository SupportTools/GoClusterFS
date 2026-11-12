import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useSnapshots } from '@/hooks'
import { TextFieldModal, ConfirmDeleteModal } from '@/components'
import { api, ApiError } from '@/api'
import type { SnapshotInfo, SnapshotState } from '@/api'
import { useRefreshInterval, useSettings } from '@/stores'
import { formatBytes, formatNumber, formatRelativeTime } from '@/utils'

type SortDirection = 'asc' | 'desc'
type SortField = 'name' | 'created_at' | 'size_bytes' | 'state'

// Mirrors the server's rules in internal/fileops.
function pathError(path: string): string | null {
  if (!path.startsWith('/')) return 'Use an absolute path inside the mount, such as /data/disk.img'
  if (path === '/') return 'Pick a file, not the root directory'
  return null
}

function snapshotNameError(name: string): string | null {
  return /[:/\0]/.test(name) ? 'Snapshot names cannot contain ":" or "/"' : null
}

function destError(dest: string): string | null {
  return /^\/[^/\0]+$/.test(dest) ? null : 'Clones go in the root directory: use /new-name'
}

function StateBadge({ state }: { state: SnapshotState }) {
  const styles: Record<SnapshotState, string> = {
    active: 'bg-gfs-status-ok text-white',
    creating: 'bg-gfs-status-info text-white animate-pulse',
    deleting: 'bg-gfs-status-warning text-white animate-pulse',
    invalid: 'bg-gfs-status-error text-white',
  }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${styles[state] ?? styles.invalid}`}>
      {state}
    </span>
  )
}

function SortableHeader({
  label,
  field,
  sort,
  direction,
  onSort,
}: {
  label: string
  field: SortField
  sort: SortField
  direction: SortDirection
  onSort: (field: SortField) => void
}) {
  const marker = sort === field ? (direction === 'asc' ? '▲' : '▼') : ''
  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium text-gfs-text-muted uppercase tracking-wider cursor-pointer hover:text-gfs-text-primary transition-colors"
      onClick={() => onSort(field)}
    >
      {label} <span className="text-gfs-accent-primary">{marker}</span>
    </th>
  )
}

function PlainHeader({ label }: { label: string }) {
  return <th className="px-4 py-3 text-left text-xs font-medium text-gfs-text-muted uppercase tracking-wider">{label}</th>
}

// Explains the errors a snapshot request can hit, with what to do about each.
function SnapshotsError({ error, path, onRetry }: { error: ApiError; path: string; onRetry: () => void }) {
  const { token } = useSettings()
  let title = 'Failed to load snapshots'
  let hint: React.ReactNode = error.details || null

  if (error.status === 401) {
    title = token ? 'The API rejected the token' : 'This API needs a token'
    hint = (
      <>
        Enter the token from -api-token-file on the <Link to="/settings">Settings</Link> page.
      </>
    )
  } else if (error.status === 403) {
    title = 'Snapshots are off on this server'
    hint = 'Start goclusterfs-mount with -api-token-file or GOCLUSTERFS_API_TOKEN, then enter the token in Settings.'
  } else if (error.status === 404 && path) {
    title = `No file at ${path}`
    hint = 'Paths start at the root of the mount.'
  }

  return (
    <div className="card border-gfs-status-error">
      <div className="text-center py-8">
        <div className="text-gfs-status-error text-lg font-medium mb-2">{title}</div>
        <div className="text-gfs-text-muted text-sm mb-1">{error.message}</div>
        {hint && <div className="text-gfs-text-muted text-sm mb-4">{hint}</div>}
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

function FileForm({ path, onChange }: { path: string; onChange: (path: string) => void }) {
  const [draft, setDraft] = useState(path)
  const [error, setError] = useState<string | null>(null)

  return (
    <form
      className="card space-y-3"
      onSubmit={(e) => {
        e.preventDefault()
        const next = draft.trim()
        const invalid = next ? pathError(next) : null
        setError(invalid)
        if (!invalid) onChange(next)
      }}
    >
      <label htmlFor="snapshot-file" className="block text-sm font-medium text-gfs-text-primary">
        File
      </label>
      <p className="text-xs text-gfs-text-muted">
        Snapshots belong to one file. Enter a path inside the mount to create, clone or delete them, or leave it empty to
        list every snapshot.
      </p>
      <div className="flex flex-wrap gap-2">
        <input
          id="snapshot-file"
          type="text"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="/data/disk.img"
          className="input flex-1 min-w-[16rem] font-mono"
        />
        <button type="submit" className="px-4 py-2 bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover transition-colors">
          Show
        </button>
        {path && (
          <button
            type="button"
            onClick={() => {
              setDraft('')
              setError(null)
              onChange('')
            }}
            className="px-4 py-2 text-sm font-medium text-gfs-text-primary bg-gfs-bg-tertiary border border-gfs-border rounded hover:bg-gfs-bg-primary transition-colors"
          >
            All snapshots
          </button>
        )}
      </div>
      {error && <p className="text-sm text-gfs-status-error">{error}</p>}
    </form>
  )
}

type Dialog =
  | { kind: 'create' }
  | { kind: 'clone-file' }
  | { kind: 'clone-snapshot'; snapshot: SnapshotInfo }
  | { kind: 'delete'; snapshot: SnapshotInfo }

export function SnapshotsPage() {
  const refreshInterval = useRefreshInterval()
  const [path, setPath] = useState('')
  const snapshots = useSnapshots(path, { refreshInterval })
  const [dialog, setDialog] = useState<Dialog | null>(null)
  const [notice, setNotice] = useState<string | null>(null)

  const [sortField, setSortField] = useState<SortField>('created_at')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')
  const [search, setSearch] = useState('')

  const handleSort = (field: SortField) => {
    if (field === sortField) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDirection('desc')
    }
  }

  const rows = useMemo(() => {
    const list = snapshots.data?.snapshots ?? []
    const query = search.toLowerCase()
    const filtered = query ? list.filter((s) => s.name.toLowerCase().includes(query)) : [...list]
    filtered.sort((a, b) => {
      let comparison = 0
      switch (sortField) {
        case 'name':
          comparison = a.name.localeCompare(b.name)
          break
        case 'created_at':
          comparison = (a.created_at ?? '').localeCompare(b.created_at ?? '')
          break
        case 'size_bytes':
          comparison = a.size_bytes - b.size_bytes
          break
        case 'state':
          comparison = a.state.localeCompare(b.state)
          break
      }
      return sortDirection === 'asc' ? comparison : -comparison
    })
    return filtered
  }, [snapshots.data, search, sortField, sortDirection])

  const closeDialog = () => setDialog(null)
  const list = snapshots.data?.snapshots ?? []
  const totalSize = list.reduce((sum, s) => sum + s.size_bytes, 0)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gfs-text-primary">Snapshots</h1>
          <p className="text-sm text-gfs-text-muted mt-1">Point-in-time copies of single files</p>
        </div>
        {path && (
          <div className="flex gap-2">
            <button
              onClick={() => setDialog({ kind: 'clone-file' })}
              className="px-4 py-2 text-sm font-medium text-gfs-text-primary bg-gfs-bg-tertiary border border-gfs-border rounded hover:bg-gfs-bg-primary transition-colors"
            >
              Clone file
            </button>
            <button
              onClick={() => setDialog({ kind: 'create' })}
              className="px-4 py-2 bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover transition-colors"
            >
              Create snapshot
            </button>
          </div>
        )}
      </div>

      <FileForm
        path={path}
        onChange={(next) => {
          setPath(next)
          setNotice(null)
        }}
      />

      {notice && (
        <div className="card border-gfs-status-info text-sm text-gfs-text-secondary flex justify-between items-center">
          <span>{notice}</span>
          <button onClick={() => setNotice(null)} className="text-gfs-text-muted hover:text-gfs-text-primary">
            Dismiss
          </button>
        </div>
      )}

      {snapshots.error && !snapshots.data ? (
        <SnapshotsError error={snapshots.error} path={path} onRetry={snapshots.refetch} />
      ) : !snapshots.data ? (
        <div className="card h-32 animate-pulse" />
      ) : list.length === 0 ? (
        <div className="card text-center py-12">
          <h3 className="text-lg font-medium text-gfs-text-primary mb-2">No snapshots</h3>
          <p className="text-sm text-gfs-text-muted max-w-md mx-auto">
            {path
              ? `${path} has no snapshots yet. Use "Create snapshot" to take one.`
              : 'No file in this filesystem has a snapshot. Enter a file path above to take one.'}
          </p>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div className="card">
              <div className="text-sm text-gfs-text-muted">Snapshots</div>
              <div className="text-2xl font-bold text-gfs-text-primary">{formatNumber(snapshots.data.total)}</div>
            </div>
            <div className="card">
              <div className="text-sm text-gfs-text-muted">File size at snapshot</div>
              <div className="text-2xl font-bold text-gfs-text-primary">{formatBytes(totalSize)}</div>
            </div>
            <div className="card">
              <div className="text-sm text-gfs-text-muted">Active</div>
              <div className="text-2xl font-bold text-gfs-status-ok">{list.filter((s) => s.state === 'active').length}</div>
            </div>
            <div className="card">
              <div className="text-sm text-gfs-text-muted">Being deleted</div>
              <div className="text-2xl font-bold text-gfs-status-warning">
                {list.filter((s) => s.state === 'deleting').length}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-4">
            <input
              type="text"
              placeholder="Filter by name..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="px-3 py-1.5 bg-gfs-bg-tertiary border border-gfs-border rounded text-sm text-gfs-text-primary placeholder-gfs-text-muted focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary w-64"
            />
            <button onClick={snapshots.refetch} className="text-sm text-gfs-accent-primary hover:underline">
              Refresh
            </button>
            <span className="text-sm text-gfs-text-muted ml-auto">
              Showing {rows.length} of {list.length}
            </span>
          </div>

          <div className="card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gfs-bg-tertiary">
                  <tr>
                    <SortableHeader label="Name" field="name" sort={sortField} direction={sortDirection} onSort={handleSort} />
                    <SortableHeader label="State" field="state" sort={sortField} direction={sortDirection} onSort={handleSort} />
                    <SortableHeader label="Size" field="size_bytes" sort={sortField} direction={sortDirection} onSort={handleSort} />
                    <PlainHeader label="Blocks" />
                    <SortableHeader label="Created" field="created_at" sort={sortField} direction={sortDirection} onSort={handleSort} />
                    {path ? <PlainHeader label="Actions" /> : <PlainHeader label="Inode" />}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((snapshot) => (
                    <tr key={snapshot.id} className="border-b border-gfs-border hover:bg-gfs-bg-tertiary/50 transition-colors">
                      <td className="px-4 py-3 text-sm font-medium text-gfs-text-primary">{snapshot.name}</td>
                      <td className="px-4 py-3">
                        <StateBadge state={snapshot.state} />
                      </td>
                      <td className="px-4 py-3 text-sm text-gfs-text-secondary">{formatBytes(snapshot.size_bytes)}</td>
                      <td className="px-4 py-3 text-sm text-gfs-text-secondary">{formatNumber(snapshot.blocks)}</td>
                      <td className="px-4 py-3 text-sm text-gfs-text-muted">
                        {snapshot.created_at ? formatRelativeTime(new Date(snapshot.created_at)) : '-'}
                      </td>
                      {path ? (
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-3 text-sm">
                            <button
                              disabled={snapshot.state !== 'active'}
                              onClick={() => setDialog({ kind: 'clone-snapshot', snapshot })}
                              className="text-gfs-accent-primary hover:underline disabled:opacity-40 disabled:no-underline disabled:cursor-not-allowed"
                            >
                              Clone
                            </button>
                            <button
                              disabled={snapshot.state !== 'active'}
                              onClick={() => setDialog({ kind: 'delete', snapshot })}
                              className="text-gfs-status-error hover:underline disabled:opacity-40 disabled:no-underline disabled:cursor-not-allowed"
                            >
                              Delete
                            </button>
                          </div>
                        </td>
                      ) : (
                        <td className="px-4 py-3 text-sm font-mono text-gfs-text-muted">{snapshot.inode}</td>
                      )}
                    </tr>
                  ))}
                  {rows.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-4 py-8 text-center text-gfs-text-muted">
                        No snapshots match the filter
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {dialog?.kind === 'create' && (
        <TextFieldModal
          isOpen
          onClose={closeDialog}
          onSuccess={snapshots.refetch}
          title="Create snapshot"
          description={
            <>
              Snapshot of <code>{path}</code>. Names are unique across the filesystem.
            </>
          }
          label="Snapshot name"
          placeholder="pre-upgrade"
          submitLabel="Create snapshot"
          busyLabel="Creating snapshot..."
          successLabel="Snapshot created"
          validate={snapshotNameError}
          onSubmit={async (name) => {
            await api.createSnapshot({ path, name })
          }}
        />
      )}

      {dialog?.kind === 'clone-file' && (
        <TextFieldModal
          isOpen
          onClose={closeDialog}
          title="Clone file"
          description={
            <>
              Copy-on-write clone of <code>{path}</code> as it is now. Clones go in the root directory.
            </>
          }
          label="New file"
          placeholder="/disk-copy.img"
          submitLabel="Clone"
          busyLabel="Cloning..."
          successLabel="Clone created"
          validate={destError}
          onSubmit={async (dest) => {
            await api.cloneFile({ path, dest })
            setNotice(`Cloned ${path} to ${dest}.`)
          }}
        />
      )}

      {dialog?.kind === 'clone-snapshot' && (
        <TextFieldModal
          isOpen
          onClose={closeDialog}
          title={`Clone snapshot ${dialog.snapshot.name}`}
          description={
            <>
              Creates a new file with the contents <code>{path}</code> had when the snapshot was taken. This is how you
              read or restore a snapshot.
            </>
          }
          label="New file"
          placeholder={`/${dialog.snapshot.name}`}
          submitLabel="Clone"
          busyLabel="Cloning..."
          successLabel="Clone created"
          validate={destError}
          onSubmit={async (dest) => {
            await api.cloneSnapshot(dialog.snapshot.name, { path, dest })
            setNotice(`Cloned snapshot ${dialog.snapshot.name} to ${dest}.`)
          }}
        />
      )}

      {dialog?.kind === 'delete' && (
        <ConfirmDeleteModal
          key={dialog.snapshot.id}
          isOpen
          onClose={closeDialog}
          onConfirm={async () => {
            await api.deleteSnapshot(dialog.snapshot.name, path)
            snapshots.refetch()
          }}
          title="Delete snapshot"
          itemName={dialog.snapshot.name}
          itemType="snapshot"
          warning="The snapshot and the blocks only it holds are freed. This cannot be undone."
          successMessage="Delete started. The snapshot shows as deleting until its blocks are freed."
          requireTypeToConfirm
        />
      )}
    </div>
  )
}

export default SnapshotsPage
