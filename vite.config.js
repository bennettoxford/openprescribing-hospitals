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
        'organisation-search': "./src/components/common/OrganisationSearch.svelte",
        'data-table': "./src/components/analyse/results/DataTable.svelte",
        'measure': "./src/components/measures/Measure.svelte",
        'org-submission': "./src/components/dq/OrgSubmission.svelte",
        'collapsible-section': "./src/components/measures/CollapsibleSection.svelte",
        'chart': "./src/components/common/Chart.svelte",
        'product-details': "./src/components/dq/ProductDetails.svelte",
      },
      output: {
        manualChunks: {
          'highcharts': ['highcharts'],
          'highcharts-modules': [
            'highcharts/modules/stock',
            'highcharts/highcharts-more',
            'highcharts/modules/accessibility',
            'highcharts/modules/exporting',
            'highcharts/modules/export-data'
          ]
        }
      },
    },
  },
  optimizeDeps: {
    include: [
      'highcharts',
      'highcharts/highcharts-more',
      'highcharts/modules/accessibility',
      'highcharts/modules/exporting',
      'highcharts/modules/export-data'
    ]
  },
  resolve: {
    alias: {
      'highcharts': path.resolve(__dirname, 'node_modules/highcharts'),
      'highcharts/highcharts-more': path.resolve(__dirname, 'node_modules/highcharts/highcharts-more.js'),
      'highcharts/modules/accessibility': path.resolve(__dirname, 'node_modules/highcharts/modules/accessibility.js'),
      'highcharts/modules/exporting': path.resolve(__dirname, 'node_modules/highcharts/modules/exporting.js'),
      'highcharts/modules/export-data': path.resolve(__dirname, 'node_modules/highcharts/modules/export-data.js'),
    }
  },
  server: {
    origin: "http://localhost:5173",
  },
  css: {
    postcss: './postcss.config.cjs'
  }
})