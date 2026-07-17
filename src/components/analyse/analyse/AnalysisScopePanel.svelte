<svelte:options runes={false} />

<script>
    import { createEventDispatcher } from 'svelte';
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import TrustScopeFilterPanel from '../../common/TrustScopeFilterPanel.svelte';

    export let selectedScope = 'all';
    export let selectedScopeFilters = {};
    export let source;

    const dispatch = createEventDispatcher();

    function handleScopeChange(scope) {
        dispatch('scopeChange', scope);
    }
</script>

<div class="space-y-3">
  <h3 class="text-base sm:text-lg font-semibold text-oxford">Scope</h3>
  <p class="text-sm text-oxford">
    The scope of an analysis specifies the NHS trusts to be included and the level of reporting. See <a href="/faq/#what-is-analysis-scope" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
  </p>
  <div class="relative min-w-0">
    <fieldset class="space-y-2 text-sm text-oxford">
      <label class="flex items-center gap-2 cursor-pointer">
        <input
          type="radio"
          name="analysis-scope"
          checked={selectedScope === 'all'}
          on:change={() => handleScopeChange('all')}
        />
        <span>All trusts</span>
      </label>
      <label class="flex items-center gap-2 cursor-pointer">
        <input
          type="radio"
          name="analysis-scope"
          checked={selectedScope === 'national'}
          on:change={() => handleScopeChange('national')}
        />
        <span>National</span>
      </label>
      <label class="flex items-center gap-2 cursor-pointer">
        <input
          type="radio"
          name="analysis-scope"
          checked={selectedScope === 'trust'}
          on:change={() => handleScopeChange('trust')}
        />
        <span>Single trust</span>
      </label>
      <label class="flex items-center gap-2 cursor-pointer">
        <input
          type="radio"
          name="analysis-scope"
          checked={selectedScope === 'group'}
          on:change={() => handleScopeChange('group')}
        />
        <span>Trust group</span>
      </label>
    </fieldset>

    {#if selectedScope === 'trust'}
      <div class="mt-3">
        <OrganisationSearch
          {source}
          overlayMode={false}
          on:selectionChange
          maxItems={1}
          hideSelectAll={true}
          showTitle={false}
        />
      </div>
    {:else if selectedScope === 'group'}
      <div class="mt-3 min-w-0 max-w-full overflow-x-hidden">
        <TrustScopeFilterPanel
          {source}
          initialFilters={selectedScopeFilters}
          on:filtersChange
        />
      </div>
    {/if}
  </div>
</div>
