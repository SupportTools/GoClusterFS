import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { apiClient } from '@/api'
import { loadToken } from '@/stores'

// Set before the first render: child effects fetch before the provider's effects run.
apiClient.setAuthToken(loadToken())

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
