/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        obsidian: '#0a0e17',
        'obsidian-light': '#161b22',
        primary: '#3b82f6',
        success: '#10b981',
      }
    },
  },
  plugins: [],
}
