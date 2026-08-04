<svelte:options runes={false} />

<script>
    import TrustScopePanel from '../../common/TrustScopePanel.svelte';
    import { ANALYSIS_SCOPE } from '../lib/analysisScope.js';

    export let selectedScope = 'all';
    export let selectedScopeFilters = {};
    export let source;
</script>

<div class="space-y-3">
  <h3 class="text-base sm:text-lg font-semibold text-oxford">Scope</h3>
  <p class="text-sm text-oxford">
    Choose which NHS trusts to include, and the level of reporting. See <a href="/faq/#what-is-analysis-scope" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
  </p>
  <div class="relative min-w-0 max-w-full {selectedScope === ANALYSIS_SCOPE.GROUP ? 'overflow-visible' : 'overflow-x-hidden'}">
    <TrustScopePanel
      {source}
      {selectedScope}
      enableScopeSelection={true}
      initialFilters={selectedScopeFilters}
      on:scopeChange
      on:filtersChange
    />
  </div>
</div>
