<svelte:options runes={false} />

<script>
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import TrustScopePanel from '../../common/TrustScopePanel.svelte';
    import {
        ANALYSIS_SCOPE,
        formatBuilderScopeSummary,
        formatScopeFilterDescription,
    } from '../lib/analysisScope.js';
    import { applyScopeFiltersToSource, hasAnyScopeFilters } from '../../../utils/scopeFilters.js';

    export let selectedScope = 'all';
    export let selectedScopeFilters = {};
    export let source;

    function countGroupTrusts(store, filters) {
        if (!hasAnyScopeFilters(filters)) {
            return (store.items || []).length;
        }
        return applyScopeFiltersToSource({
            allItems: store.items || [],
            filters,
            getTrustType: (name) => source.getTrustType(name),
            getOrgsByRegionsOrICBs: (regions, icbs) => source.getOrgsByRegionsOrICBs(regions, icbs),
            getOrgsByCancerAlliances: (alliances) => source.getOrgsByCancerAlliances(alliances),
            orgShelfordGroup: store.orgShelfordGroup,
        }).orgList.length;
    }

    $: selectedTrustName = selectedScope === ANALYSIS_SCOPE.TRUST
        ? (($source.selectedItems || [])[0] || null)
        : null;
    $: trustCount = selectedScope === ANALYSIS_SCOPE.TRUST
        ? ($source.selectedItems || []).length
        : selectedScope === ANALYSIS_SCOPE.GROUP
            ? countGroupTrusts($source, selectedScopeFilters)
            : ($source.items || []).length;
    $: filterDescription = selectedScope === ANALYSIS_SCOPE.GROUP
        ? formatScopeFilterDescription(selectedScopeFilters)
        : '';
    $: scopeSummary = formatBuilderScopeSummary({
        scope: selectedScope,
        trustCount,
        selectedTrustName,
        hasFilters: hasAnyScopeFilters(selectedScopeFilters),
        filterDescription,
    });
</script>

<div class="space-y-3">
  <h3 class="text-base sm:text-lg font-semibold text-oxford">Scope</h3>
  <p class="text-sm text-oxford">
    The scope of an analysis specifies the NHS trusts to be included and the level of reporting. See <a href="/faq/#what-is-analysis-scope" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
  </p>
  <div class="relative min-w-0 max-w-full {selectedScope === ANALYSIS_SCOPE.TRUST ? 'overflow-visible' : 'overflow-x-hidden'}">
    <TrustScopePanel
      {source}
      {selectedScope}
      enableScopeSelection={true}
      initialFilters={selectedScopeFilters}
      on:scopeChange
      on:filtersChange
    >
      {#snippet singleTrust()}
        <div class="relative min-w-0 w-full z-[1000]">
          <OrganisationSearch
            {source}
            overlayMode={true}
            on:selectionChange
            maxItems={1}
            hideSelectAll={true}
            showTitle={false}
          />
        </div>
      {/snippet}
    </TrustScopePanel>
  </div>
  {#if scopeSummary}
    <p class="text-sm text-gray-600">{scopeSummary}</p>
  {/if}
</div>
