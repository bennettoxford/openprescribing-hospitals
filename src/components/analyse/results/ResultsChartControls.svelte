<svelte:options runes={false} />

<script>
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import ModeSelector from '../../common/ModeSelector.svelte';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { resultsStore } from '../../../stores/resultsStore';
    import { TRUST_OVERLAY_MAX_COUNT } from '../lib/analysisScope.js';
    import { createEventDispatcher } from 'svelte';

    export let resultsModeSearchStore;
    export let shouldShowOrganisationSearch = false;
    export let availableTrusts = [];
    export let viewModes = [];
    export let isTrustScope = false;
    export let percentilesDisabled = false;
    export let trustPercentileToggleDisabled = true;
    export let isCopyingShareLink = false;

    const dispatch = createEventDispatcher();

    $: isTrustMode = $modeSelectorStore.selectedMode === 'trust';
    $: trustCohortCount = availableTrusts.length > 0
        ? availableTrusts.length
        : ($resultsModeSearchStore.availableItems?.size || 0);
    $: trustMaxItems = isTrustMode ? TRUST_OVERLAY_MAX_COUNT : null;
    $: trustHideSelectAll = isTrustMode && trustCohortCount > TRUST_OVERLAY_MAX_COUNT;
    $: hideClearAll = !isTrustMode || percentilesDisabled;
    $: requireSelection = !isTrustMode;
</script>

<div class="mb-4">
    <div class="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3 mb-3">
        {#if shouldShowOrganisationSearch}
            <div class="w-full md:w-7/12 relative z-10">
                <OrganisationSearch
                    source={resultsModeSearchStore}
                    overlayMode={true}
                    on:selectionChange
                    showTitle={false}
                    maxItems={trustMaxItems}
                    hideSelectAll={trustHideSelectAll}
                    hideClearAll={hideClearAll}
                    requireSelection={requireSelection}
                />
            </div>
        {/if}
        <button
            type="button"
            on:click={() => dispatch('copyLink')}
            class="sm:ml-auto flex items-center gap-2 px-3 py-2 text-sm font-medium text-oxford-600 bg-white border border-oxford-200 rounded-md hover:bg-oxford-50 hover:border-oxford-300 transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-400 disabled:opacity-70 disabled:cursor-not-allowed"
            disabled={isCopyingShareLink}
            title="Copy link to share this analysis with current selections"
        >
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-4 h-4">
                <path stroke-linecap="round" stroke-linejoin="round" d="M7.217 10.907a2.25 2.25 0 100 2.186m0-2.186c.18.324.283.696.283 1.093s-.103.77-.283 1.093m0-2.186l9.566-5.314m-9.566 7.5l9.566 5.314m0 0a2.25 2.25 0 103.935 2.186 2.25 2.25 0 00-3.935-2.186zm0-12.814a2.25 2.25 0 103.935-2.186 2.25 2.25 0 00-3.935 2.186z" />
            </svg>
            {isCopyingShareLink ? 'Copying...' : 'Share analysis'}
        </button>
    </div>
    <div class="flex flex-col sm:flex-row sm:items-center gap-4">
        <ModeSelector
            options={viewModes}
            label="View by"
            variant="pill"
            onChange={(mode) => dispatch('modeChange', { mode })}
        />

        {#if $modeSelectorStore.selectedMode === 'trust' && !isTrustScope}
            <div class="flex flex-col gap-4">
                <div class="flex flex-col items-center gap-2">
                    <span class="text-sm text-gray-600 leading-tight text-center">
                        Show percentiles
                    </span>
                    <div class="flex items-center gap-2">
                        <label class="inline-flex items-center {!trustPercentileToggleDisabled ? 'cursor-pointer' : 'cursor-not-allowed opacity-50'}">
                            <input
                                type="checkbox"
                                class="sr-only peer"
                                checked={$resultsStore.showPercentiles}
                                disabled={trustPercentileToggleDisabled}
                                on:change={() => dispatch('percentileToggle')}
                            />
                            <div class="relative w-9 h-5 bg-gray-200 peer-focus:outline-none {!trustPercentileToggleDisabled ? 'peer-focus:ring-2 peer-focus:ring-blue-500' : ''} rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600 {!trustPercentileToggleDisabled ? '' : 'peer-disabled:opacity-50'}"></div>
                        </label>
                        <div class="relative inline-block group">
                            <button type="button" aria-label="Percentiles information" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                                <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                                </svg>
                            </button>
                            <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform
                                        group-hover:scale-100 w-[280px] -translate-x-1/2 left-1/2 sm:-translate-x-full sm:left-5 top-5 rounded-md shadow-lg bg-white
                                        ring-1 ring-black ring-opacity-5 p-4">
                                <p class="text-sm text-gray-500">
                                    {#if percentilesDisabled}
                                        Percentiles are disabled when there are fewer than 30 trusts in the filtered cohort. All in-scope trusts are shown individually.
                                    {:else if $analyseOptions.selectedOrganisations?.length > 0}
                                        Percentiles show variation in product quantity across NHS Trusts and allow easy comparison of Trust activity relative to the median Trust level. See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret them.
                                    {:else}
                                        Percentiles are always shown when no trusts are selected. Select trusts to enable this toggle. See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                                    {/if}
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        {/if}
    </div>
</div>
