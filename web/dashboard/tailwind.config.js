import forms from '@tailwindcss/forms'

/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}',
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // GoClusterFS theme colors
        gfs: {
          bg: {
            primary: '#0f0f23',
            secondary: '#1a1a2e',
            tertiary: '#16213e',
          },
          accent: {
            primary: '#63b3ed',
            secondary: '#4299e1',
            hover: '#90cdf4',
          },
          border: {
            DEFAULT: '#2d3748',
            light: '#4a5568',
          },
          text: {
            primary: '#e2e8f0',
            secondary: '#a0aec0',
            muted: '#718096',
          },
          status: {
            ok: '#48bb78',
            warning: '#ed8936',
            error: '#f56565',
            info: '#4299e1',
          },
        },
      },
      fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['SF Mono', 'Monaco', 'Cascadia Code', 'Fira Code', 'monospace'],
      },
      spacing: {
        '18': '4.5rem',
        '88': '22rem',
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'spin-slow': 'spin 3s linear infinite',
        'slide-in-right': 'slideInRight 0.3s ease-out forwards',
        'fade-in': 'fadeIn 0.2s ease-out forwards',
      },
      keyframes: {
        slideInRight: {
          '0%': { transform: 'translateX(100%)' },
          '100%': { transform: 'translateX(0)' },
        },
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
      },
    },
  },
  plugins: [forms],
}
