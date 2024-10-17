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
        'search-box': "./src/components/common/Search.svelte",
        'time-series-chart': "./src/components/analyse/results/TimeSeriesChart.svelte",
        'organisation-search': "./src/components/common/OrganisationSearch.svelte",
        'data-table': "./src/components/analyse/results/DataTable.svelte",
        'measure': "./src/components/measures/Measure.svelte",
        'org-submission': "./src/components/dq/OrgSubmission.svelte",
        'collapsible-section': "./src/components/common/CollapsibleSection.svelte",
      }
    }
  },
})