import { SlidePanel } from '@/components/common'
import { formatRelativeTime, nodeStateColor } from '@/utils'

export interface ClusterNode {
  nodeId: string
  host: string
  state: string
  lastHeartbeat?: string
  joinedAt?: string
  isSelf: boolean
}

export interface NodeDetailPanelProps {
  node: ClusterNode | null
  onClose: () => void
}

function InfoRow({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex justify-between items-center py-2 border-b border-gfs-border last:border-0">
      <dt className="text-sm text-gfs-text-muted">{label}</dt>
      <dd className={`text-sm text-gfs-text-primary ${mono ? 'font-mono text-xs break-all text-right ml-4' : ''}`}>{value}</dd>
    </div>
  )
}

function timeOrUnknown(timestamp?: string): string {
  return timestamp ? formatRelativeTime(new Date(timestamp)) : 'Unknown'
}

export function NodeDetailPanel({ node, onClose }: NodeDetailPanelProps) {
  if (!node) {
    return null
  }

  return (
    <SlidePanel isOpen onClose={onClose} title={node.host || node.nodeId} subtitle={node.isSelf ? 'This node' : 'Peer'} width="md">
      <dl className="bg-gfs-bg-tertiary rounded-lg p-4">
        <InfoRow
          label="State"
          value={
            <span className="flex items-center gap-2">
              <span className={`w-2 h-2 rounded-full ${nodeStateColor(node.state)}`} />
              {node.state}
            </span>
          }
        />
        <InfoRow label="Host" value={node.host || 'Unknown'} />
        <InfoRow label="Node ID" value={node.nodeId} mono />
        <InfoRow label="Last Heartbeat" value={timeOrUnknown(node.lastHeartbeat)} />
        {node.isSelf && <InfoRow label="Mounted" value={timeOrUnknown(node.joinedAt)} />}
      </dl>
    </SlidePanel>
  )
}

export default NodeDetailPanel
