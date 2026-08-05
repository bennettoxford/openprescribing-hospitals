<svelte:options runes={false} />

<script>
    import { createEventDispatcher, onMount } from 'svelte';
    import {
        ACUTE_PARENT,
        acuteSubtypeLabel,
        applyScopeFiltersToSource,
        CANCER_ALLIANCE_NA,
        cancerAllianceDisplayName,
        hasAnyScopeFilters,
        isAcuteType,
        normaliseAcuteSelection as normaliseAcuteSelectionSet,
    } from '../../utils/scopeFilters.js';
    import { updateRegionSelection, updateIcbSelection } from '../../utils/regionIcbFilterUtils.js';

    const dispatch = createEventDispatcher();

    export let source;
    export let initialFilters = null;
    export let applyAvailability = true;
    export let resetKey = undefined;
    export let enableScopeSelection = false;
    export let selectedScope = 'all';

    const SCOPE_ALL = 'all';
    const SCOPE_NATIONAL = 'national';
    const SCOPE_GROUP = 'group';

    function normaliseAcuteSelection(types) {
        return normaliseAcuteSelectionSet(types, acuteTypes, { asArray: true });
    }

    $: trustTypes = ($source ? source.getTrustTypes() : []) || [];
    $: regionsHierarchy = ($source ? source.getRegionsHierarchy() : []) || [];
    $: cancerAlliances = ($source ? source.getCancerAlliances() : []) || [];
    $: hasShelfordData = ($source.orgShelfordGroup || new Map()).size > 0;

    $: acuteTypes = trustTypes.filter((type) => isAcuteType(type));
    $: otherTypes = trustTypes.filter((type) => !isAcuteType(type));

    let selectedTrustTypes = [];
    let selectedRegions = new Set();
    let selectedICBs = new Set();
    let selectedCancerAlliances = new Set();
    let shelfordFilter = null;
    let expandedRegions = new Set();
    let expandedAcute = false;

    let collapsedTrustType = true;
    let collapsedRegionIcb = true;
    let collapsedCancerAlliance = true;
    let collapsedShelfordGroup = true;

    $: acuteParentSelected = selectedTrustTypes.includes(ACUTE_PARENT);
    $: selectedAcuteSubtypeCount = selectedTrustTypes.filter(isAcuteType).length;
    $: acuteIndeterminate = !acuteParentSelected && selectedAcuteSubtypeCount > 0;

    $: shelfordInCount = ($source.items || []).filter(
        (name) => ($source.orgShelfordGroup || new Map()).get(name) === true
    ).length;
    $: shelfordNotCount = ($source.items || []).filter(
        (name) => !($source.orgShelfordGroup || new Map()).get(name)
    ).length;

    $: selectedOtherTrustTypeCount = selectedTrustTypes.filter(
        (type) => type !== ACUTE_PARENT && !isAcuteType(type)
    ).length;
    $: selectedTrustTypeFilterCount =
        selectedOtherTrustTypeCount + (acuteParentSelected ? 1 : selectedAcuteSubtypeCount);
    $: regionIcbFilterCount = selectedRegions.size + selectedICBs.size;
    $: cancerAllianceFilterCount = selectedCancerAlliances.size;
    $: shelfordFilterCount = shelfordFilter !== null ? 1 : 0;
    $: filterBadgeCount =
        selectedTrustTypeFilterCount +
        regionIcbFilterCount +
        cancerAllianceFilterCount +
        shelfordFilterCount;
    $: showGroupFilters = !enableScopeSelection || selectedScope === SCOPE_GROUP;
    $: currentFilters = {
        trustTypes: [...selectedTrustTypes].sort(),
        regions: Array.from(selectedRegions).sort(),
        icbs: Array.from(selectedICBs).sort(),
        cancerAlliances: Array.from(selectedCancerAlliances).sort(),
        shelford: shelfordFilter
    };
    $: groupTrustCount = !hasAnyScopeFilters(currentFilters)
        ? 0
        : applyScopeFiltersToSource({
            allItems: $source.items || [],
            filters: currentFilters,
            getTrustType: (name) => $source.trustTypes?.get(name) ?? null,
            getOrgsByRegionsOrICBs: (regions, icbs) => {
                const hasRegions = regions && regions.size > 0;
                const hasIcbs = icbs && icbs.size > 0;
                if (!hasRegions && !hasIcbs) return [];
                const result = new Set();
                for (const name of $source.items || []) {
                    if (hasRegions && regions.has($source.orgRegions?.get(name))) {
                        result.add(name);
                        continue;
                    }
                    if (hasIcbs && icbs.has($source.orgIcbs?.get(name))) {
                        result.add(name);
                    }
                }
                return Array.from(result);
            },
            getOrgsByCancerAlliances: (alliances) => {
                if (!alliances || alliances.size === 0) return [];
                const itemsSet = new Set($source.items || []);
                const map = $source.orgCancerAlliances || new Map();
                const result = new Set();
                if (alliances.has(CANCER_ALLIANCE_NA)) {
                    for (const name of $source.items || []) {
                        const ca = map.get(name);
                        if (!ca || ca === '') result.add(name);
                    }
                }
                for (const [orgName, ca] of map) {
                    if (ca && alliances.has(ca) && itemsSet.has(orgName)) result.add(orgName);
                }
                return Array.from(result);
            },
            orgShelfordGroup: $source.orgShelfordGroup,
        }).orgList.length;

    function stripNhsPrefix(str) {
        if (str == null || typeof str !== 'string') return '';
        return str.replace(/^NHS\s+/i, '').trim();
    }

    function getCurrentFilters() {
        return {
            trustTypes: [...selectedTrustTypes].sort(),
            regions: Array.from(selectedRegions).sort(),
            icbs: Array.from(selectedICBs).sort(),
            cancerAlliances: Array.from(selectedCancerAlliances).sort(),
            shelford: shelfordFilter
        };
    }

    function emitFiltersChange() {
        dispatch('filtersChange', getCurrentFilters());
    }

    function applyFilters(filters) {
        selectedTrustTypes = normaliseAcuteSelection(
            Array.isArray(filters?.trustTypes) ? filters.trustTypes : []
        );
        selectedRegions = new Set(Array.isArray(filters?.regions) ? filters.regions : []);
        selectedICBs = new Set(Array.isArray(filters?.icbs) ? filters.icbs : []);
        selectedCancerAlliances = new Set(Array.isArray(filters?.cancerAlliances) ? filters.cancerAlliances : []);
        shelfordFilter = filters?.shelford === 'in' || filters?.shelford === 'not_in' ? filters.shelford : null;
        if (selectedTrustTypes.length > 0) collapsedTrustType = false;
        if (selectedTrustTypes.some(isAcuteType) || selectedTrustTypes.includes(ACUTE_PARENT)) {
            expandedAcute = true;
        }
        if (selectedRegions.size > 0 || selectedICBs.size > 0) collapsedRegionIcb = false;
        if (selectedCancerAlliances.size > 0) collapsedCancerAlliance = false;
        if (shelfordFilter !== null) collapsedShelfordGroup = false;
        applyTrustTypeSelection();
    }

    onMount(() => {
        if (hasAnyScopeFilters(initialFilters)) {
            applyFilters(initialFilters);
        }
    });

    let _lastResetKey = resetKey;
    $: if (resetKey !== undefined && resetKey !== _lastResetKey) {
        _lastResetKey = resetKey;
        clearAllFilters();
    }

    let _lastSelectedScope = selectedScope;
    $: if (enableScopeSelection && selectedScope !== _lastSelectedScope) {
        _lastSelectedScope = selectedScope;
        if (selectedScope === SCOPE_GROUP) {
            applyTrustTypeSelection();
        }
    }

    function applyTrustTypeSelection() {
        if (!applyAvailability) return;
        if (typeof source.applyScopeFilters !== 'function') {
            console.error('Trust scope filter source must implement applyScopeFilters');
            return;
        }
        source.applyScopeFilters(getCurrentFilters());
    }

    function toggleTrustType(type) {
        if (isAcuteType(type) && selectedTrustTypes.includes(ACUTE_PARENT)) return;
        const next = new Set(selectedTrustTypes);
        if (next.has(type)) next.delete(type);
        else next.add(type);
        selectedTrustTypes = normaliseAcuteSelection(Array.from(next));
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function toggleAcuteParent() {
        const next = new Set(selectedTrustTypes);
        if (next.has(ACUTE_PARENT)) {
            next.delete(ACUTE_PARENT);
        } else {
            next.add(ACUTE_PARENT);
            acuteTypes.forEach((type) => next.delete(type));
        }
        selectedTrustTypes = Array.from(next).sort();
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function toggleRegionExpansion(regionName) {
        const next = new Set(expandedRegions);
        if (next.has(regionName)) next.delete(regionName);
        else next.add(regionName);
        expandedRegions = next;
    }

    function toggleRegion(regionName) {
        const regionData = regionsHierarchy.find((region) => region.region === regionName) || {
            region: regionName,
            icbs: []
        };
        const updated = updateRegionSelection(regionData, selectedRegions, selectedICBs);
        selectedRegions = updated.selectedRegions;
        selectedICBs = updated.selectedICBs;
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function toggleICB(icbName) {
        const parentRegion = regionsHierarchy.find((region) =>
            (region.icbs || []).some((icb) => icb.name === icbName)
        );
        if (parentRegion) {
            const updated = updateIcbSelection(
                parentRegion,
                { name: icbName },
                selectedRegions,
                selectedICBs
            );
            if (!updated) return;
            selectedRegions = updated.selectedRegions;
            selectedICBs = updated.selectedICBs;
        } else {
            const next = new Set(selectedICBs);
            if (next.has(icbName)) next.delete(icbName);
            else next.add(icbName);
            selectedICBs = next;
        }
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function toggleCancerAlliance(caName) {
        const next = new Set(selectedCancerAlliances);
        if (next.has(caName)) next.delete(caName);
        else next.add(caName);
        selectedCancerAlliances = next;
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function toggleShelford(filterValue) {
        shelfordFilter = shelfordFilter === filterValue ? null : filterValue;
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function clearAllFilters() {
        selectedTrustTypes = [];
        selectedRegions = new Set();
        selectedICBs = new Set();
        selectedCancerAlliances = new Set();
        shelfordFilter = null;
        expandedAcute = false;
        expandedRegions = new Set();
        collapsedTrustType = true;
        collapsedRegionIcb = true;
        collapsedCancerAlliance = true;
        collapsedShelfordGroup = true;
        emitFiltersChange();
        applyTrustTypeSelection();
    }

    function selectScope(scope) {
        if (!enableScopeSelection || scope === selectedScope) return;
        dispatch('scopeChange', scope);
    }
</script>

<div class="w-full flex flex-col rounded-lg border border-gray-200 bg-white overflow-visible">
    {#if !enableScopeSelection}
        <div class="px-3 py-2 border-b border-gray-100 flex items-center justify-between shrink-0">
            <div class="flex items-center gap-1.5 min-w-0">
                <span class="text-xs font-semibold text-gray-600 uppercase tracking-wide">Filter by</span>
                {#if filterBadgeCount > 0}
                    <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums normal-case tracking-normal bg-gray-100/80 px-1.5 py-0.5 rounded">
                        {filterBadgeCount} filter{filterBadgeCount === 1 ? '' : 's'} selected
                    </span>
                {/if}
            </div>
            {#if filterBadgeCount > 0}
                <button
                    type="button"
                    class="text-xs font-medium text-oxford-600 hover:text-oxford-800 py-0.5 px-1.5 rounded hover:bg-oxford-50 transition-colors"
                    on:click={clearAllFilters}
                >
                    Clear all
                </button>
            {/if}
        </div>
    {/if}
    <div class="flex-1 {enableScopeSelection ? '' : 'py-2'} {enableScopeSelection && selectedScope === SCOPE_GROUP ? 'overflow-visible' : 'overflow-y-auto max-h-[min(70vh,400px)]'}">
        {#if enableScopeSelection}
            <div class="divide-y divide-gray-100" role="radiogroup" aria-label="Analysis scope">
                <button
                    type="button"
                    role="radio"
                    aria-checked={selectedScope === SCOPE_ALL}
                    on:click={() => selectScope(SCOPE_ALL)}
                    class="w-full text-left px-3 py-2.5 text-sm min-h-[44px] sm:min-h-0 transition-none focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oxford-500
                        {selectedScope === SCOPE_ALL ? 'bg-oxford-100 text-oxford-700' : 'text-gray-700 hover:bg-gray-50'}"
                >
                    <div class="flex items-center justify-between min-w-0">
                        <span class="font-medium">All trusts <span class="font-normal text-gray-500">(default)</span></span>
                        {#if selectedScope === SCOPE_ALL}
                            <span class="text-xs font-medium text-oxford-600 ml-2 shrink-0">Selected</span>
                        {/if}
                    </div>
                    <span class="block text-xs text-gray-500 mt-0.5 leading-snug">
                        Trust-level data for every NHS Trust. Analyse one or more trusts and compare them.
                    </span>
                </button>
                <button
                    type="button"
                    role="radio"
                    aria-checked={selectedScope === SCOPE_NATIONAL}
                    on:click={() => selectScope(SCOPE_NATIONAL)}
                    class="w-full text-left px-3 py-2.5 text-sm min-h-[44px] sm:min-h-0 transition-none focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oxford-500
                        {selectedScope === SCOPE_NATIONAL ? 'bg-oxford-100 text-oxford-700' : 'text-gray-700 hover:bg-gray-50'}"
                >
                    <div class="flex items-center justify-between min-w-0">
                        <span class="font-medium">National</span>
                        {#if selectedScope === SCOPE_NATIONAL}
                            <span class="text-xs font-medium text-oxford-600 ml-2 shrink-0">Selected</span>
                        {/if}
                    </div>
                    <span class="block text-xs text-gray-500 mt-0.5 leading-snug">
                        National totals across all NHS Trusts.
                    </span>
                </button>
                <div
                    role="radio"
                    tabindex="0"
                    aria-checked={selectedScope === SCOPE_GROUP}
                    on:click={() => selectScope(SCOPE_GROUP)}
                    on:keydown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            selectScope(SCOPE_GROUP);
                        }
                    }}
                    class="w-full text-left px-3 py-2.5 text-sm min-h-[44px] sm:min-h-0 transition-none cursor-pointer focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oxford-500
                        {selectedScope === SCOPE_GROUP ? 'bg-oxford-100 text-oxford-700' : 'text-gray-700 hover:bg-gray-50'}"
                >
                    <div class="flex items-center justify-between min-w-0">
                        <div class="flex items-center gap-1.5 min-w-0">
                            <span class="font-medium shrink-0">Filtered trusts</span>
                            {#if selectedScope === SCOPE_GROUP}
                                <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-white/80 px-1.5 py-0.5 rounded">
                                    {groupTrustCount} trust{groupTrustCount === 1 ? '' : 's'}
                                </span>
                            {/if}
                        </div>
                        {#if selectedScope === SCOPE_GROUP}
                            <span class="text-xs font-medium text-oxford-600 ml-2 shrink-0">Selected</span>
                        {/if}
                    </div>
                    <span class="block text-xs text-gray-500 mt-0.5 leading-snug">
                        Select a subset of trusts by geography, trust type, and more.
                    </span>
                </div>
                {#if selectedScope === SCOPE_GROUP}
                    {#if filterBadgeCount > 0}
                        <div class="px-3 py-1.5 flex items-center justify-between gap-2 bg-white">
                            <span class="text-xs text-gray-500">
                                {filterBadgeCount} filter{filterBadgeCount === 1 ? '' : 's'} selected
                            </span>
                            <button
                                type="button"
                                class="text-xs font-medium text-red-600 hover:text-red-800 py-0.5 px-1.5 rounded hover:bg-red-50 transition-colors shrink-0"
                                on:click={clearAllFilters}
                            >
                                Clear selection
                            </button>
                        </div>
                    {/if}
                    <div class="pt-1 pb-2 min-w-0">
                        {@render groupFilterSections()}
                    </div>
                {/if}
            </div>
        {:else if showGroupFilters}
            {@render groupFilterSections()}
        {/if}
    </div>
</div>

{#snippet groupFilterSections()}
    {#if regionsHierarchy.length > 0}
        <div class="px-2 pb-1">
            <button
                type="button"
                class="w-full flex items-center justify-between gap-2 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 uppercase tracking-wide hover:bg-gray-50 transition-colors"
                on:click={() => (collapsedRegionIcb = !collapsedRegionIcb)}
            >
                <span class="flex items-center gap-1.5 min-w-0">
                    <span>Region &amp; ICB</span>
                    {#if regionIcbFilterCount > 0}
                        <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums normal-case tracking-normal bg-gray-100/80 px-1.5 py-0.5 rounded">
                            {regionIcbFilterCount} filter{regionIcbFilterCount === 1 ? '' : 's'} selected
                        </span>
                    {/if}
                </span>
                <svg
                    class="w-3.5 h-3.5 transition-transform duration-200 {collapsedRegionIcb ? '' : 'rotate-180'}"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                </svg>
            </button>
            {#if !collapsedRegionIcb}
                {#each regionsHierarchy as region (region.region)}
                    <div class="px-1">
                        <div class="flex flex-wrap items-start gap-2 rounded-lg px-2 py-2 sm:py-1.5 text-sm hover:bg-gray-50 transition-colors min-h-[44px] sm:min-h-0 min-w-0 w-full {selectedRegions.has(region.region) ? 'text-oxford-700' : 'text-gray-700'}">
                            <button
                                type="button"
                                class="p-1.5 -m-1 hover:bg-gray-200/60 rounded-md transition-colors shrink-0 text-gray-400"
                                on:click={() => toggleRegionExpansion(region.region)}
                                aria-label={expandedRegions.has(region.region) ? 'Collapse' : 'Expand'}
                            >
                                <svg
                                    class="w-3.5 h-3.5 transition-transform duration-200 {expandedRegions.has(region.region) ? 'rotate-90' : ''}"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                                </svg>
                            </button>
                            <label class="flex items-start gap-2.5 cursor-pointer flex-1 min-w-0">
                                <input
                                    type="checkbox"
                                    checked={selectedRegions.has(region.region)}
                                    on:change={() => toggleRegion(region.region)}
                                    class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0"
                                />
                                <span
                                    class="flex-1 min-w-0 break-words"
                                    title={region.region + (region.region_code ? ' (' + region.region_code + ')' : '')}
                                >
                                    {region.region}{#if region.region_code}<span class="whitespace-nowrap">&nbsp;({region.region_code})</span>{/if}
                                </span>
                            </label>
                            <span class="text-[10px] font-medium text-gray-400 whitespace-normal text-right tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                                {(region.icbs || []).length} ICBs · {(source.getOrgsByRegion(region.region) || []).length} trusts
                            </span>
                        </div>
                        {#if expandedRegions.has(region.region)}
                            <div class="ml-6 pl-3 mt-0.5 border-l-2 border-gray-200 space-y-0.5">
                                {#each region.icbs || [] as icb (icb.name)}
                                    <label class="flex flex-wrap w-full items-start gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-1 text-sm transition-colors min-w-0 {selectedRegions.has(region.region) ? 'opacity-50' : ''} {selectedICBs.has(icb.name) ? 'text-oxford-700' : 'text-gray-600'}">
                                        <input
                                            type="checkbox"
                                            checked={selectedICBs.has(icb.name) || selectedRegions.has(region.region)}
                                            disabled={selectedRegions.has(region.region)}
                                            on:change={() => toggleICB(icb.name)}
                                            class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0"
                                        />
                                        <span
                                            class="flex-1 min-w-0 break-words"
                                            title={icb.name + (icb.code ? ' (' + icb.code + ')' : '')}
                                        >
                                            {stripNhsPrefix(icb.name)}{#if icb.code}<span class="whitespace-nowrap">&nbsp;({icb.code})</span>{/if}
                                        </span>
                                        <span class="text-[10px] font-medium text-gray-400 whitespace-normal text-right tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                                            {(source.getOrgsByICB(icb.name) || []).length} trusts
                                        </span>
                                    </label>
                                {/each}
                            </div>
                        {/if}
                    </div>
                {/each}
            {/if}
        </div>
    {/if}

    {#if trustTypes.length > 0}
        <div class="px-2 pt-1 {regionsHierarchy.length > 0 ? 'border-t border-gray-100' : ''}">
            <div class="w-full flex items-center gap-1.5 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 tracking-wide hover:bg-gray-50 transition-colors">
                <button
                    type="button"
                    class="flex items-center gap-1.5 min-w-0 text-left uppercase"
                    on:click={() => (collapsedTrustType = !collapsedTrustType)}
                    aria-expanded={!collapsedTrustType}
                >
                    <span>Trust type</span>
                </button>
                <div class="relative inline-block group shrink-0 z-10 hover:z-[100] focus-within:z-[100]">
                    <button
                        type="button"
                        aria-label="Trust type information"
                        class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-1 focus:ring-oxford-500 flex items-center"
                    >
                        <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                        </svg>
                    </button>
                    <div
                        class="absolute z-[100] scale-0 transition-all duration-100 origin-top transform
                            group-hover:scale-100 w-[220px] left-1/2 -translate-x-1/4 top-5 rounded-md shadow-lg bg-white
                            ring-1 ring-black ring-opacity-5 p-3 normal-case"
                    >
                        <p class="text-sm text-gray-500 font-normal tracking-normal">
                            Trust type is a classification of NHS trusts by the kind of services they provide, such as Acute, Mental Health, Community, or Ambulance. Every trust has a single type.
                            See <a href="/faq/#how-are-trust-types-determined" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                        </p>
                    </div>
                </div>
                {#if selectedTrustTypeFilterCount > 0}
                    <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums normal-case tracking-normal bg-gray-100/80 px-1.5 py-0.5 rounded">
                        {selectedTrustTypeFilterCount} filter{selectedTrustTypeFilterCount === 1 ? '' : 's'} selected
                    </span>
                {/if}
                <button
                    type="button"
                    class="ml-auto flex-1 flex items-center justify-end"
                    on:click={() => (collapsedTrustType = !collapsedTrustType)}
                    tabindex="-1"
                    aria-hidden="true"
                >
                    <svg
                        class="w-3.5 h-3.5 transition-transform duration-200 {collapsedTrustType ? '' : 'rotate-180'}"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                    >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </button>
            </div>
            {#if !collapsedTrustType}
                {#if acuteTypes.length > 0}
                    <div class="px-2 pt-1 pb-0.5">
                        <div class="flex items-center gap-2 rounded-lg px-2 py-1.5 text-sm font-medium hover:bg-gray-50 transition-colors {selectedTrustTypes.includes(ACUTE_PARENT) ? 'text-oxford-700' : 'text-gray-800'}">
                            <button
                                type="button"
                                class="p-1 -m-1 hover:bg-gray-200/60 rounded-md transition-colors shrink-0 text-gray-400"
                                on:click={() => (expandedAcute = !expandedAcute)}
                                aria-label={expandedAcute ? 'Collapse' : 'Expand'}
                            >
                                <svg
                                    class="w-3.5 h-3.5 transition-transform duration-200 {expandedAcute ? 'rotate-90' : ''}"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                                </svg>
                            </button>
                            <label class="flex items-center gap-2.5 cursor-pointer flex-1 min-w-0">
                                <input
                                    type="checkbox"
                                    checked={acuteParentSelected}
                                    indeterminate={acuteIndeterminate}
                                    on:change={toggleAcuteParent}
                                    class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0"
                                />
                                <span>Acute</span>
                            </label>
                            <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                                {acuteTypes.reduce((n, t) => n + (source.getOrgsByTrustTypeGlobal(t) || []).length, 0)} trusts
                            </span>
                        </div>
                        {#if expandedAcute}
                            <div class="ml-6 pl-3 mt-0.5 border-l-2 border-gray-200 space-y-0.5">
                                {#each acuteTypes as type (type)}
                                    <label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-1 text-sm transition-colors {selectedTrustTypes.includes(ACUTE_PARENT) ? 'opacity-50' : ''} {selectedTrustTypes.includes(ACUTE_PARENT) || selectedTrustTypes.includes(type) ? 'text-oxford-700' : 'text-gray-600'}">
                                        <input
                                            type="checkbox"
                                            checked={selectedTrustTypes.includes(ACUTE_PARENT) || selectedTrustTypes.includes(type)}
                                            on:change={() => toggleTrustType(type)}
                                            class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0"
                                            disabled={selectedTrustTypes.includes(ACUTE_PARENT)}
                                        />
                                        <span>{acuteSubtypeLabel(type)}</span>
                                        <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded ml-auto">
                                            {(source.getOrgsByTrustTypeGlobal(type) || []).length} trusts
                                        </span>
                                    </label>
                                {/each}
                            </div>
                        {/if}
                    </div>
                {/if}
                {#each otherTypes as type (type)}
                    <label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-2 sm:py-1.5 text-sm text-gray-700 transition-colors mx-1 min-h-[44px] sm:min-h-0">
                        <input
                            type="checkbox"
                            checked={selectedTrustTypes.includes(type)}
                            on:change={() => toggleTrustType(type)}
                            class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0"
                        />
                        <span class="flex-1 min-w-0">{type}</span>
                        <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                            {(source.getOrgsByTrustTypeGlobal(type) || []).length} trusts
                        </span>
                    </label>
                {/each}
            {/if}
        </div>
    {/if}

    {#if cancerAlliances.length > 0}
        <div class="px-2 pt-1 {regionsHierarchy.length > 0 || trustTypes.length > 0 ? 'border-t border-gray-100' : ''}">
            <div class="w-full flex items-center gap-1.5 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 tracking-wide hover:bg-gray-50 transition-colors">
                <button
                    type="button"
                    class="flex items-center gap-1.5 min-w-0 text-left uppercase"
                    on:click={() => (collapsedCancerAlliance = !collapsedCancerAlliance)}
                    aria-expanded={!collapsedCancerAlliance}
                >
                    <span>Cancer Alliance</span>
                </button>
                <div class="relative inline-block group shrink-0 z-10 hover:z-[100] focus-within:z-[100]">
                    <button
                        type="button"
                        aria-label="Cancer Alliance information"
                        class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-1 focus:ring-oxford-500 flex items-center"
                    >
                        <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                        </svg>
                    </button>
                    <div
                        class="absolute z-[100] scale-0 transition-all duration-100 origin-top transform
                            group-hover:scale-100 w-[220px] left-1/2 -translate-x-1/2 top-5 rounded-md shadow-lg bg-white
                            ring-1 ring-black ring-opacity-5 p-3 normal-case"
                    >
                        <p class="text-sm text-gray-500 font-normal tracking-normal">
                            Cancer Alliances are regional groupings for cancer care.
                            See <a href="/faq/#where-do-cancer-alliance-boundaries-come-from" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                        </p>
                    </div>
                </div>
                {#if cancerAllianceFilterCount > 0}
                    <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums normal-case tracking-normal bg-gray-100/80 px-1.5 py-0.5 rounded">
                        {cancerAllianceFilterCount} filter{cancerAllianceFilterCount === 1 ? '' : 's'} selected
                    </span>
                {/if}
                <button
                    type="button"
                    class="ml-auto flex-1 flex items-center justify-end"
                    on:click={() => (collapsedCancerAlliance = !collapsedCancerAlliance)}
                    tabindex="-1"
                    aria-hidden="true"
                >
                    <svg
                        class="w-3.5 h-3.5 transition-transform duration-200 {collapsedCancerAlliance ? '' : 'rotate-180'}"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                    >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </button>
            </div>
            {#if !collapsedCancerAlliance}
                <div class="max-h-48 overflow-y-auto">
                    {#each cancerAlliances as ca (ca.name)}
                        <label class="flex flex-wrap items-start gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-2 sm:py-1.5 text-sm text-gray-700 transition-colors mx-1 min-h-[44px] sm:min-h-0 min-w-0 w-full {selectedCancerAlliances.has(ca.name) ? 'text-oxford-700' : ''}">
                            <input
                                type="checkbox"
                                checked={selectedCancerAlliances.has(ca.name)}
                                on:change={() => toggleCancerAlliance(ca.name)}
                                class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0"
                            />
                            <span class="flex-1 min-w-0 break-words" title={ca.name}>{cancerAllianceDisplayName(ca.name)}</span>
                            <span class="text-[10px] font-medium text-gray-400 whitespace-normal text-right tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                                {(source.getOrgsByCancerAlliance(ca.name) || []).length} trusts
                            </span>
                        </label>
                    {/each}
                    <label class="flex flex-wrap items-start gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-2 sm:py-1.5 text-sm text-gray-700 transition-colors mx-1 min-h-[44px] sm:min-h-0 border-t border-gray-100 mt-1 pt-1.5 min-w-0 w-full {selectedCancerAlliances.has(CANCER_ALLIANCE_NA) ? 'text-oxford-700' : ''}">
                        <input
                            type="checkbox"
                            checked={selectedCancerAlliances.has(CANCER_ALLIANCE_NA)}
                            on:change={() => toggleCancerAlliance(CANCER_ALLIANCE_NA)}
                            class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0"
                        />
                        <span class="flex-1 min-w-0 break-words" title="Trusts not associated with a Cancer Alliance">{cancerAllianceDisplayName(CANCER_ALLIANCE_NA)}</span>
                        <span class="text-[10px] font-medium text-gray-400 whitespace-normal text-right tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                            {(source.getOrgsWithNoCancerAlliance() || []).length} trusts
                        </span>
                    </label>
                </div>
            {/if}
        </div>
    {/if}

    {#if hasShelfordData}
        <div class="px-2 pt-1 {regionsHierarchy.length > 0 || trustTypes.length > 0 || cancerAlliances.length > 0 ? 'border-t border-gray-100' : ''}">
            <div class="w-full flex items-center gap-1.5 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 tracking-wide hover:bg-gray-50 transition-colors">
                <button
                    type="button"
                    class="flex items-center gap-1.5 min-w-0 text-left uppercase"
                    on:click={() => (collapsedShelfordGroup = !collapsedShelfordGroup)}
                    aria-expanded={!collapsedShelfordGroup}
                >
                    <span>Shelford Group</span>
                </button>
                <div class="relative inline-block group shrink-0 z-10 hover:z-[100] focus-within:z-[100]">
                    <button
                        type="button"
                        aria-label="Shelford Group information"
                        class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-1 focus:ring-oxford-500 flex items-center"
                    >
                        <svg class="h-3.5 w-3.5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                        </svg>
                    </button>
                    <div
                        class="absolute z-[100] scale-0 transition-all duration-100 origin-top transform
                            group-hover:scale-100 w-[220px] left-1/2 -translate-x-1/2 top-5 rounded-md shadow-lg bg-white
                            ring-1 ring-black ring-opacity-5 p-3 normal-case"
                    >
                        <p class="text-sm text-gray-500 font-normal tracking-normal">
                            The Shelford Group is a collaboration of ten large teaching and research NHS hospital trusts in England.
                            See <a href="/faq/#what-is-the-shelford-group" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                        </p>
                    </div>
                </div>
                {#if shelfordFilterCount > 0}
                    <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums normal-case tracking-normal bg-gray-100/80 px-1.5 py-0.5 rounded">
                        {shelfordFilterCount} filter{shelfordFilterCount === 1 ? '' : 's'} selected
                    </span>
                {/if}
                <button
                    type="button"
                    class="ml-auto flex-1 flex items-center justify-end"
                    on:click={() => (collapsedShelfordGroup = !collapsedShelfordGroup)}
                    tabindex="-1"
                    aria-hidden="true"
                >
                    <svg
                        class="w-3.5 h-3.5 transition-transform duration-200 {collapsedShelfordGroup ? '' : 'rotate-180'}"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                    >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </button>
            </div>
            {#if !collapsedShelfordGroup}
                <label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-2 sm:py-1.5 text-sm text-gray-700 transition-colors mx-1 min-h-[44px] sm:min-h-0 {shelfordFilter === 'in' ? 'text-oxford-700' : ''}">
                    <input
                        type="checkbox"
                        checked={shelfordFilter === 'in'}
                        on:change={() => toggleShelford('in')}
                        class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0"
                    />
                    <span class="truncate flex-1 min-w-0" title="Trusts in the Shelford Group">In Shelford Group</span>
                    <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                        {shelfordInCount} trusts
                    </span>
                </label>
                <label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-2 sm:py-1.5 text-sm text-gray-700 transition-colors mx-1 min-h-[44px] sm:min-h-0 {shelfordFilter === 'not_in' ? 'text-oxford-700' : ''}">
                    <input
                        type="checkbox"
                        checked={shelfordFilter === 'not_in'}
                        on:change={() => toggleShelford('not_in')}
                        class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0"
                    />
                    <span class="truncate flex-1 min-w-0" title="Trusts not in the Shelford Group">Not in Shelford Group</span>
                    <span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">
                        {shelfordNotCount} trusts
                    </span>
                </label>
            {/if}
        </div>
    {/if}
{/snippet}
