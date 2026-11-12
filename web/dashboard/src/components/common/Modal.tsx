/**
 * Reusable modal component with accessibility support
 */

import { useEffect, useRef, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

export interface ModalProps {
  /** Whether the modal is open */
  isOpen: boolean
  /** Callback when the modal should close */
  onClose: () => void
  /** Modal title */
  title: string
  /** Modal content */
  children: ReactNode
  /** Optional footer content (buttons, etc.) */
  footer?: ReactNode
  /** Size of the modal */
  size?: 'sm' | 'md' | 'lg'
  /** Whether clicking the backdrop closes the modal */
  closeOnBackdrop?: boolean
  /** Whether pressing Escape closes the modal */
  closeOnEscape?: boolean
}

const sizeClasses = {
  sm: 'max-w-md',
  md: 'max-w-lg',
  lg: 'max-w-2xl',
}

export function Modal({
  isOpen,
  onClose,
  title,
  children,
  footer,
  size = 'md',
  closeOnBackdrop = true,
  closeOnEscape = true,
}: ModalProps) {
  const modalRef = useRef<HTMLDivElement>(null)
  const previousActiveElement = useRef<HTMLElement | null>(null)

  // Handle escape key
  useEffect(() => {
    if (!isOpen || !closeOnEscape) return

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose()
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [isOpen, closeOnEscape, onClose])

  // Focus management
  useEffect(() => {
    if (isOpen) {
      previousActiveElement.current = document.activeElement as HTMLElement
      // Focus the modal on open
      modalRef.current?.focus()
    } else if (previousActiveElement.current) {
      // Restore focus on close
      previousActiveElement.current.focus()
    }
  }, [isOpen])

  // Prevent body scroll when modal is open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden'
    } else {
      document.body.style.overflow = ''
    }
    return () => {
      document.body.style.overflow = ''
    }
  }, [isOpen])

  if (!isOpen) return null

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (closeOnBackdrop && e.target === e.currentTarget) {
      onClose()
    }
  }

  const modalContent = (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
    >
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 backdrop-blur-sm transition-opacity"
        onClick={handleBackdropClick}
        aria-hidden="true"
      />

      {/* Modal panel */}
      <div
        ref={modalRef}
        className={`relative w-full ${sizeClasses[size]} bg-gfs-bg-secondary border border-gfs-border rounded-lg shadow-xl transform transition-all`}
        tabIndex={-1}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gfs-border">
          <h2 id="modal-title" className="text-lg font-semibold text-gfs-text-primary">
            {title}
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="p-1 rounded text-gfs-text-muted hover:text-gfs-text-primary hover:bg-gfs-bg-tertiary transition-colors"
            aria-label="Close modal"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div className="px-6 py-4">
          {children}
        </div>

        {/* Footer */}
        {footer && (
          <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gfs-border bg-gfs-bg-tertiary/50">
            {footer}
          </div>
        )}
      </div>
    </div>
  )

  // Render modal in portal
  return createPortal(modalContent, document.body)
}

export default Modal
