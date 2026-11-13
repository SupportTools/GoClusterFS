import { Link } from 'react-router-dom'
import { ApiError } from '@/api'

export function TokenHint({ error }: { error: Error }) {
  if (!(error instanceof ApiError) || error.status !== 401) {
    return null
  }
  return (
    <div className="text-gfs-text-muted text-sm mb-4">
      This API needs a token. Enter it on the <Link to="/settings">Settings</Link> page.
    </div>
  )
}
