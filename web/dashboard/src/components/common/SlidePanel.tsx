/**
 * Reusable slide-in panel component
 * Slides in from the right side of the screen
 */

import { useEffect, useCallback, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

export interface SlidePanelProps {
  /** Whether the panel is open */
  isOpen: boolean
  /** Callback when the panel should close */
  onClose: () => void
  /** Panel title */
  title: string
  /** Optional subtitle */
  subtitle?: string
  /** Panel content */
  children: ReactNode
  /** Panel width - defaults to 'md' */
  width?: 'sm' | 'md' | 'lg' | 'xl'
  /** Whether to close on backdrop click */
  closeOnBackdrop?: boolean
  /** Whether to close on escape key */
  closeOnEscape?: boolean
}

const widthClasses = {
  sm: 'max-w-sm',
  md: 'max-w-md',
  lg: 'max-w-lg',
  xl: 'max-w-xl',
}

export function SlidePanel({
  isOpen,
  onClose,
  title,
  subtitle,
  children,
  width = 'md',
  closeOnBackdrop = true,
  closeOnEscape = true,
}: SlidePanelProps) {
  // Handle escape key
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (closeOnEscape && e.key === 'Escape') {
        onClose()
      }
    },
    [closeOnEscape, onClose]
  )

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown)
      // Prevent body scroll when panel is open
      document.body.style.overflow = 'hidden'

      return () => {
        document.removeEventListener('keydown', handleKeyDown)
        document.body.style.overflow = ''
      }
    }
  }, [isOpen, handleKeyDown])

  if (!isOpen) {
    return null
  }

  const panel = (
    <div className="fixed inset-0 z-50 flex justify-end">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50 transition-opacity"
        onClick={closeOnBackdrop ? onClose : undefined}
        aria-hidden="true"
      />

      {/* Panel */}
      <div
        className={`relative w-full ${widthClasses[width]} bg-gfs-bg-secondary shadow-xl flex flex-col transform transition-transform duration-300 ease-out animate-slide-in-right`}
        role="dialog"
        aria-modal="true"
        aria-labelledby="slide-panel-title"
      >
        {/* Header */}
        <div className="flex items-start justify-between px-6 py-4 border-b border-gfs-border">
          <div className="flex-1 min-w-0 pr-4">
            <h2
              id="slide-panel-title"
              className="text-lg font-semibold text-gfs-text-primary truncate"
            >
              {title}
            </h2>
            {subtitle && (
              <p className="text-sm text-gfs-text-muted mt-0.5 truncate">
                {subtitle}
              </p>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="flex-shrink-0 p-1 text-gfs-text-muted hover:text-gfs-text-primary transition-colors rounded hover:bg-gfs-bg-tertiary"
            aria-label="Close panel"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto px-6 py-4">
          {children}
        </div>
      </div>
    </div>
  )

  return createPortal(panel, document.body)
}

export default SlidePanel
