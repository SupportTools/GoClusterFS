/**
 * Reusable confirmation modal for destructive actions
 * Includes type-to-confirm functionality for extra safety
 */

import { useState } from 'react'
import { Modal } from '@/components/common/Modal'

export interface ConfirmDeleteModalProps {
  /** Whether the modal is open */
  isOpen: boolean
  /** Callback when the modal should close */
  onClose: () => void
  /** Callback when deletion is confirmed - should return a promise */
  onConfirm: () => Promise<void>
  /** Title for the modal */
  title?: string
  /** Name of the item being deleted (used for type-to-confirm) */
  itemName: string
  /** Type of item being deleted (e.g., "snapshot", "node") */
  itemType?: string
  /** Additional warning message */
  warning?: string
  /** Whether to require typing the name to confirm */
  requireTypeToConfirm?: boolean
  /** Shown once onConfirm resolves */
  successMessage?: string
}

type ModalState = 'confirm' | 'deleting' | 'success' | 'error'

/**
 * ConfirmDeleteModal - Reusable delete confirmation with type-to-confirm
 *
 * IMPORTANT: To ensure proper state reset between uses, pass a unique `key`
 * prop based on the item being deleted (e.g., key={snapshot?.id}).
 * This causes React to remount the component with fresh state.
 */
export function ConfirmDeleteModal({
  isOpen,
  onClose,
  onConfirm,
  title = 'Confirm Delete',
  itemName,
  itemType = 'item',
  warning,
  requireTypeToConfirm = true,
  successMessage = 'Deleted successfully',
}: ConfirmDeleteModalProps) {
  const [state, setState] = useState<ModalState>('confirm')
  const [confirmText, setConfirmText] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const handleClose = () => {
    onClose()
  }

  const isConfirmEnabled = !requireTypeToConfirm || confirmText === itemName

  const handleDelete = async () => {
    if (!isConfirmEnabled) return

    setState('deleting')
    setErrorMessage('')

    try {
      await onConfirm()
      setState('success')

      // Auto-close after success
      setTimeout(() => {
        handleClose()
      }, 1500)
    } catch (err) {
      setState('error')
      setErrorMessage(err instanceof Error ? err.message : 'Failed to delete')
    }
  }

  const renderContent = () => {
    switch (state) {
      case 'deleting':
        return (
          <div className="py-8 text-center">
            <div className="animate-spin w-12 h-12 border-4 border-gfs-status-error border-t-transparent rounded-full mx-auto mb-4" />
            <p className="text-gfs-text-secondary">Deleting {itemType}...</p>
          </div>
        )

      case 'success':
        return (
          <div className="py-8 text-center">
            <div className="w-12 h-12 bg-gfs-status-ok rounded-full flex items-center justify-center mx-auto mb-4">
              <svg className="w-6 h-6 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <p className="text-gfs-text-primary font-medium">{successMessage}</p>
          </div>
        )

      case 'error':
        return (
          <div className="space-y-4">
            <div className="p-4 bg-gfs-status-error/10 border border-gfs-status-error/20 rounded-lg">
              <div className="flex items-start gap-3">
                <div className="flex-shrink-0 w-5 h-5 text-gfs-status-error mt-0.5">
                  <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <div>
                  <p className="text-sm font-medium text-gfs-status-error">Delete failed</p>
                  <p className="text-sm text-gfs-text-secondary mt-1">{errorMessage}</p>
                </div>
              </div>
            </div>
            <button
              type="button"
              onClick={() => setState('confirm')}
              className="text-sm text-gfs-accent-primary hover:underline"
            >
              Try again
            </button>
          </div>
        )

      default:
        return (
          <div className="space-y-4">
            {/* Warning icon and message */}
            <div className="flex items-start gap-3">
              <div className="flex-shrink-0 w-10 h-10 bg-gfs-status-error/10 rounded-full flex items-center justify-center">
                <svg className="w-5 h-5 text-gfs-status-error" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <div className="flex-1">
                <p className="text-gfs-text-primary">
                  Are you sure you want to delete{' '}
                  <span className="font-semibold text-gfs-status-error">"{itemName}"</span>?
                </p>
                <p className="text-sm text-gfs-text-muted mt-1">
                  This action cannot be undone.
                </p>
              </div>
            </div>

            {/* Additional warning */}
            {warning && (
              <div className="p-3 bg-gfs-status-warning/10 border border-gfs-status-warning/20 rounded-md">
                <p className="text-sm text-gfs-status-warning">{warning}</p>
              </div>
            )}

            {/* Type to confirm */}
            {requireTypeToConfirm && (
              <div>
                <label htmlFor="confirm-delete" className="block text-sm text-gfs-text-secondary mb-2">
                  Type <span className="font-mono bg-gfs-bg-tertiary px-1.5 py-0.5 rounded text-gfs-text-primary">{itemName}</span> to confirm:
                </label>
                <input
                  id="confirm-delete"
                  type="text"
                  value={confirmText}
                  onChange={(e) => setConfirmText(e.target.value)}
                  placeholder={itemName}
                  className="w-full px-3 py-2 bg-gfs-bg-primary border border-gfs-border rounded-md text-gfs-text-primary placeholder-gfs-text-muted focus:outline-none focus:ring-2 focus:ring-gfs-status-error focus:border-transparent"
                  autoComplete="off"
                  spellCheck={false}
                />
              </div>
            )}
          </div>
        )
    }
  }

  const renderFooter = () => {
    if (state === 'deleting' || state === 'success') {
      return null
    }

    if (state === 'error') {
      return (
        <button
          type="button"
          onClick={handleClose}
          className="px-4 py-2 text-sm font-medium text-gfs-text-secondary hover:text-gfs-text-primary transition-colors"
        >
          Close
        </button>
      )
    }

    return (
      <>
        <button
          type="button"
          onClick={handleClose}
          className="px-4 py-2 text-sm font-medium text-gfs-text-secondary hover:text-gfs-text-primary transition-colors"
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={handleDelete}
          disabled={!isConfirmEnabled}
          className="px-4 py-2 text-sm font-medium bg-gfs-status-error text-white rounded hover:bg-red-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          Delete {itemType}
        </button>
      </>
    )
  }

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title={title}
      footer={renderFooter()}
      size="sm"
      closeOnEscape={state === 'confirm'}
      closeOnBackdrop={state === 'confirm'}
    >
      {renderContent()}
    </Modal>
  )
}

export default ConfirmDeleteModal
