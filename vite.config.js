import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import path from 'path'


// https://vitejs.dev/config/
export default defineConfig({
  plugins: [svelte({
    compilerOptions: {
      customElement: true
    },
    emitCss: true
  })],
  base: "/static/",
  build: {
    manifest: true,
    outDir: "assets/dist",
    emptyOutDir: true,
    rollupOptions: {
      input: {
        'main': path.resolve(__dirname, "src/main.js"),
        'analyse': "./src/components/analyse/Analyse.svelte",
        'search-box': "./src/components/common/Search.svelte",
        'time-series-chart': "./src/components/analyse/results/TimeSeriesChart.svelte",
        'organisation-search': "./src/components/common/OrganisationSearch.svelte",
        'data-table': "./src/components/analyse/results/DataTable.svelte",
        'measure': "./src/components/measures/Measure.svelte",
        'org-submission': "./src/components/dq/OrgSubmission.svelte",
        'collapsible-section': "./src/components/measures/CollapsibleSection.svelte",
        'chart': "./src/components/common/Chart.svelte",
      }
    },
  },
  server: {
    origin: "http://localhost:5173",
  },
  css: {
    postcss: './postcss.config.cjs'
  }
})
