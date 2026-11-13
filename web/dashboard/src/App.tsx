import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { DashboardLayout } from '@/layouts'
import { DashboardPage, ClusterPage, SnapshotsPage, StatisticsPage, SettingsPage } from '@/pages'
import { SettingsProvider } from '@/stores'

function App() {
  return (
    <SettingsProvider>
      <BrowserRouter basename={import.meta.env.BASE_URL.replace(/\/$/, '')}>
        <Routes>
          <Route element={<DashboardLayout />}>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/cluster" element={<ClusterPage />} />
            <Route path="/snapshots" element={<SnapshotsPage />} />
            <Route path="/statistics" element={<StatisticsPage />} />
            <Route path="/settings" element={<SettingsPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </SettingsProvider>
  )
}

export default App
