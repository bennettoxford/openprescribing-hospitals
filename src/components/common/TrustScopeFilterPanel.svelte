<script>
    import { createEventDispatcher, onMount } from 'svelte';
    import {
        ACUTE_PARENT,
        acuteSubtypeLabel,
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
    $: filterBadgeCount =
        selectedTrustTypeFilterCount +
        selectedRegions.size +
        selectedICBs.size +
        selectedCancerAlliances.size +
        (shelfordFilter !== null ? 1 : 0);

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
        expandedRegions = new Set();
        emitFiltersChange();
        applyTrustTypeSelection();
    }
</script>

<div class="w-full flex flex-col rounded-lg border border-gray-200 bg-white overflow-hidden">
    <div class="px-3 py-2 border-b border-gray-100 flex items-center justify-between shrink-0">
        <div class="flex items-center gap-1.5 min-w-0">
            <span class="text-xs font-semibold text-gray-600 uppercase tracking-wide">Filter by</span>
            {#if filterBadgeCount > 0}
                <span class="inline-flex items-center justify-center min-w-[1rem] h-4 px-1 rounded-full text-[10px] font-medium leading-none bg-oxford-100 text-oxford-700">{filterBadgeCount}</span>
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
    <div class="flex-1 overflow-y-auto py-2 max-h-[min(70vh,400px)]">
        {#if trustTypes.length > 0}
            <div class="px-2 pb-2">
                <button
                    type="button"
                    class="w-full flex items-center justify-between gap-2 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 uppercase tracking-wide mb-1 hover:bg-gray-50 transition-colors"
                    on:click={() => (collapsedTrustType = !collapsedTrustType)}
                >
                    <span>Trust type</span>
                    <svg
                        class="w-3.5 h-3.5 transition-transform duration-200 {collapsedTrustType ? '' : 'rotate-180'}"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                    >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </button>
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

        {#if regionsHierarchy.length > 0}
            <div class="px-2 pt-2 {trustTypes.length > 0 ? 'border-t border-gray-100' : ''}">
                <button
                    type="button"
                    class="w-full flex items-center justify-between gap-2 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 uppercase tracking-wide mb-1 hover:bg-gray-50 transition-colors"
                    on:click={() => (collapsedRegionIcb = !collapsedRegionIcb)}
                >
                    <span>Region &amp; ICB</span>
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

        {#if cancerAlliances.length > 0}
            <div class="px-2 pt-2 {trustTypes.length > 0 || regionsHierarchy.length > 0 ? 'border-t border-gray-100' : ''}">
                <button
                    type="button"
                    class="w-full flex items-center justify-between gap-2 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 uppercase tracking-wide mb-1 hover:bg-gray-50 transition-colors"
                    on:click={() => (collapsedCancerAlliance = !collapsedCancerAlliance)}
                >
                    <span>Cancer Alliance</span>
                    <svg
                        class="w-3.5 h-3.5 transition-transform duration-200 {collapsedCancerAlliance ? '' : 'rotate-180'}"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                    >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </button>
                {#if !collapsedCancerAlliance}
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
                {/if}
            </div>
        {/if}

        {#if hasShelfordData}
            <div class="px-2 pt-2 {trustTypes.length > 0 || regionsHierarchy.length > 0 || cancerAlliances.length > 0 ? 'border-t border-gray-100' : ''}">
                <button
                    type="button"
                    class="w-full flex items-center justify-between gap-2 rounded-lg px-2 py-1.5 text-xs font-medium text-gray-500 uppercase tracking-wide mb-1 hover:bg-gray-50 transition-colors"
                    on:click={() => (collapsedShelfordGroup = !collapsedShelfordGroup)}
                >
                    <span>Shelford Group</span>
                    <svg
                        class="w-3.5 h-3.5 transition-transform duration-200 {collapsedShelfordGroup ? '' : 'rotate-180'}"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                    >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </button>
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
    </div>
</div>
