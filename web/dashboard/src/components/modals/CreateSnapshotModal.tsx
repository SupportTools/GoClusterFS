/**
 * Modal for creating a new snapshot
 */

import { useState, useCallback } from 'react'
import { Modal } from '@/components/common/Modal'
import { api, ApiError } from '@/api'
import type { CreateSnapshotRequest } from '@/api'

export interface CreateSnapshotModalProps {
  isOpen: boolean
  onClose: () => void
  onSuccess: () => void
}

type ModalState = 'form' | 'submitting' | 'success' | 'error'

export function CreateSnapshotModal({
  isOpen,
  onClose,
  onSuccess,
}: CreateSnapshotModalProps) {
  const [state, setState] = useState<ModalState>('form')
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [retentionPolicy, setRetentionPolicy] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  // Reset form when modal opens/closes
  const resetForm = useCallback(() => {
    setState('form')
    setName('')
    setDescription('')
    setRetentionPolicy('')
    setErrorMessage('')
  }, [])

  const handleClose = () => {
    resetForm()
    onClose()
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!name.trim()) {
      setErrorMessage('Snapshot name is required')
      return
    }

    setState('submitting')
    setErrorMessage('')

    try {
      const request: CreateSnapshotRequest = {
        name: name.trim(),
      }

      if (description.trim()) {
        request.description = description.trim()
      }

      if (retentionPolicy.trim()) {
        request.retention_policy = retentionPolicy.trim()
      }

      await api.createSnapshot(request)
      setState('success')

      // Auto-close after success
      setTimeout(() => {
        handleClose()
        onSuccess()
      }, 1500)
    } catch (err) {
      setState('error')
      if (err instanceof ApiError) {
        setErrorMessage(err.message)
      } else {
        setErrorMessage('Failed to create snapshot')
      }
    }
  }

  const renderContent = () => {
    switch (state) {
      case 'submitting':
        return (
          <div className="py-8 text-center">
            <div className="animate-spin w-12 h-12 border-4 border-gfs-accent-primary border-t-transparent rounded-full mx-auto mb-4" />
            <p className="text-gfs-text-secondary">Creating snapshot...</p>
            <p className="text-sm text-gfs-text-muted mt-1">This may take a moment</p>
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
            <p className="text-gfs-text-primary font-medium">Snapshot created successfully!</p>
            <p className="text-sm text-gfs-text-muted mt-1">"{name}"</p>
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
                  <p className="text-sm font-medium text-gfs-status-error">Failed to create snapshot</p>
                  <p className="text-sm text-gfs-text-secondary mt-1">{errorMessage}</p>
                </div>
              </div>
            </div>
            <button
              type="button"
              onClick={() => setState('form')}
              className="text-sm text-gfs-accent-primary hover:underline"
            >
              Try again
            </button>
          </div>
        )

      default:
        return (
          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Name field */}
            <div>
              <label htmlFor="snapshot-name" className="block text-sm font-medium text-gfs-text-primary mb-1">
                Name <span className="text-gfs-status-error">*</span>
              </label>
              <input
                id="snapshot-name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g., pre-upgrade-backup"
                className="w-full px-3 py-2 bg-gfs-bg-primary border border-gfs-border rounded-md text-gfs-text-primary placeholder-gfs-text-muted focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary focus:border-transparent"
                autoFocus
                required
              />
              <p className="mt-1 text-xs text-gfs-text-muted">
                A descriptive name for this snapshot
              </p>
            </div>

            {/* Description field */}
            <div>
              <label htmlFor="snapshot-description" className="block text-sm font-medium text-gfs-text-primary mb-1">
                Description
              </label>
              <textarea
                id="snapshot-description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Optional description..."
                rows={3}
                className="w-full px-3 py-2 bg-gfs-bg-primary border border-gfs-border rounded-md text-gfs-text-primary placeholder-gfs-text-muted focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary focus:border-transparent resize-none"
              />
            </div>

            {/* Retention policy field */}
            <div>
              <label htmlFor="snapshot-retention" className="block text-sm font-medium text-gfs-text-primary mb-1">
                Retention Policy
              </label>
              <select
                id="snapshot-retention"
                value={retentionPolicy}
                onChange={(e) => setRetentionPolicy(e.target.value)}
                className="w-full px-3 py-2 bg-gfs-bg-primary border border-gfs-border rounded-md text-gfs-text-primary focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary focus:border-transparent"
              >
                <option value="">No expiration</option>
                <option value="1d">1 day</option>
                <option value="7d">7 days</option>
                <option value="30d">30 days</option>
                <option value="90d">90 days</option>
                <option value="365d">1 year</option>
              </select>
              <p className="mt-1 text-xs text-gfs-text-muted">
                When this snapshot should automatically expire
              </p>
            </div>

            {/* Error message */}
            {errorMessage && (
              <div className="p-3 bg-gfs-status-error/10 border border-gfs-status-error/20 rounded-md">
                <p className="text-sm text-gfs-status-error">{errorMessage}</p>
              </div>
            )}
          </form>
        )
    }
  }

  const renderFooter = () => {
    if (state === 'submitting' || state === 'success') {
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
          type="submit"
          onClick={handleSubmit}
          disabled={!name.trim()}
          className="px-4 py-2 text-sm font-medium bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          Create Snapshot
        </button>
      </>
    )
  }

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title="Create Snapshot"
      footer={renderFooter()}
      closeOnEscape={state === 'form'}
      closeOnBackdrop={state === 'form'}
    >
      {renderContent()}
    </Modal>
  )
}

export default CreateSnapshotModal
