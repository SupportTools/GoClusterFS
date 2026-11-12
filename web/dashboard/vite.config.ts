import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// The Go server embeds the build from internal/webui and serves it under /ui/.
export default defineConfig({
  base: '/ui/',
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://127.0.0.1:8080',
      '/health': 'http://127.0.0.1:8080',
    },
  },
  build: {
    outDir: '../../internal/webui/build/dist',
    emptyOutDir: true,
    sourcemap: false,
  },
})
