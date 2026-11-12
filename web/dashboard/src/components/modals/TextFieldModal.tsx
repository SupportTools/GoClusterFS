import { useState, type ReactNode } from 'react'
import { Modal } from '@/components/common/Modal'

export interface TextFieldModalProps {
  isOpen: boolean
  onClose: () => void
  onSuccess?: () => void
  title: string
  description: ReactNode
  label: string
  placeholder?: string
  initialValue?: string
  submitLabel: string
  busyLabel: string
  successLabel: string
  /** Returns an error message, or null if the value is fine */
  validate: (value: string) => string | null
  onSubmit: (value: string) => Promise<void>
}

type ModalState = 'form' | 'submitting' | 'success'

// Remount with a new key to reset it between uses.
export function TextFieldModal({
  isOpen,
  onClose,
  onSuccess,
  title,
  description,
  label,
  placeholder,
  initialValue = '',
  submitLabel,
  busyLabel,
  successLabel,
  validate,
  onSubmit,
}: TextFieldModalProps) {
  const [state, setState] = useState<ModalState>('form')
  const [value, setValue] = useState(initialValue)
  const [errorMessage, setErrorMessage] = useState('')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const trimmed = value.trim()
    const invalid = validate(trimmed)
    if (invalid) {
      setErrorMessage(invalid)
      return
    }
    setState('submitting')
    setErrorMessage('')
    try {
      await onSubmit(trimmed)
      setState('success')
      onSuccess?.()
      setTimeout(onClose, 1200)
    } catch (err) {
      setState('form')
      setErrorMessage(err instanceof Error ? err.message : 'Request failed')
    }
  }

  const formId = `text-field-${label.replace(/\W+/g, '-').toLowerCase()}`

  const footer =
    state === 'form' ? (
      <>
        <button
          type="button"
          onClick={onClose}
          className="px-4 py-2 text-sm font-medium text-gfs-text-secondary hover:text-gfs-text-primary transition-colors"
        >
          Cancel
        </button>
        <button
          type="submit"
          form={formId}
          disabled={!value.trim()}
          className="px-4 py-2 text-sm font-medium bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {submitLabel}
        </button>
      </>
    ) : null

  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      title={title}
      footer={footer}
      closeOnEscape={state === 'form'}
      closeOnBackdrop={state === 'form'}
    >
      {state === 'submitting' && (
        <div className="py-8 text-center">
          <div className="animate-spin w-12 h-12 border-4 border-gfs-accent-primary border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-gfs-text-secondary">{busyLabel}</p>
          <p className="text-sm text-gfs-text-muted mt-1">Large files can take a while</p>
        </div>
      )}
      {state === 'success' && (
        <div className="py-8 text-center">
          <div className="w-12 h-12 bg-gfs-status-ok rounded-full flex items-center justify-center mx-auto mb-4">
            <svg className="w-6 h-6 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <p className="text-gfs-text-primary font-medium">{successLabel}</p>
        </div>
      )}
      {state === 'form' && (
        <form id={formId} onSubmit={handleSubmit} className="space-y-4">
          <div className="text-sm text-gfs-text-secondary">{description}</div>
          <div>
            <label htmlFor={`${formId}-input`} className="block text-sm font-medium text-gfs-text-primary mb-1">
              {label}
            </label>
            <input
              id={`${formId}-input`}
              type="text"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder={placeholder}
              className="w-full px-3 py-2 bg-gfs-bg-primary border border-gfs-border rounded-md text-gfs-text-primary placeholder-gfs-text-muted focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary focus:border-transparent"
              autoFocus
              required
            />
          </div>
          {errorMessage && (
            <div className="p-3 bg-gfs-status-error/10 border border-gfs-status-error/20 rounded-md">
              <p className="text-sm text-gfs-status-error">{errorMessage}</p>
            </div>
          )}
        </form>
      )}
    </Modal>
  )
}

export default TextFieldModal
