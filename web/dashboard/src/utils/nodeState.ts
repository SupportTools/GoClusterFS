// Registry states; "stale" means the heartbeat lapsed but no peer has declared the node dead yet.
const NODE_STATE_COLORS: Record<string, string> = {
  active: 'bg-gfs-status-ok',
  leaving: 'bg-gfs-status-warning',
  stale: 'bg-gfs-status-warning',
  dead: 'bg-gfs-status-error',
}

export function nodeStateColor(state: string): string {
  return NODE_STATE_COLORS[state.toLowerCase()] ?? 'bg-gfs-text-muted'
}
