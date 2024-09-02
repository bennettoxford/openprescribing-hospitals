import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [svelte({
    compilerOptions: {
      customElement: true
    },
    emitCss: true
  }),
  ],
  base: "/static/",
  build: {
    manifest: true,
    outDir: "assets/assets",
    rollupOptions: {
      input: {
        'search-box': "./src/Search.svelte",
        'time-series-chart': "./src/TimeSeriesChart.svelte",
        'searchable-dropdown': "./src/SearchableDropdown.svelte",
        'data-table': "./src/DataTable.svelte",
        'measure': "./src/Measure.svelte",
      }
    }
  },
})