<script>
    import { onMount } from 'svelte';

    export let regionsHierarchy = [];
    export let selectedRegions = new Set();
    export let selectedICBs = new Set();
    export let onRegionClick = (_region) => {};
    export let onIcbClick = (_region, _icb) => {};
    export let onClear = () => {};

    let isFilterOpen = false;
    let expandedRegions = new Set();
    let filterContainerEl;

    $: totalICBs = regionsHierarchy.reduce(
        (total, r) => total + (r.icbs?.length || 0),
        0
    );
    $: selectedRegionICBs = Array.from(selectedRegions).reduce(
        (count, name) =>
            count + (regionsHierarchy.find((r) => r.region === name)?.icbs?.length || 0),
        0
    );
    $: selectedICBsCount = selectedRegionICBs + selectedICBs.size;

    function toggleFilter() {
        isFilterOpen = !isFilterOpen;
        if (isFilterOpen && selectedICBs.size > 0) {
            const toExpand = regionsHierarchy
                .filter((region) => region.icbs?.some((icb) => selectedICBs.has(icb.name)))
                .map((r) => r.region);
            expandedRegions = new Set([...expandedRegions, ...toExpand]);
        }
    }

    function handleClickOutside(event) {
        if (isFilterOpen && filterContainerEl && !filterContainerEl.contains(event.target)) {
            isFilterOpen = false;
            expandedRegions = new Set();
        }
    }

    function toggleRegionExpansion(region) {
        if (expandedRegions.has(region)) {
            expandedRegions = new Set([...expandedRegions].filter((r) => r !== region));
        } else {
            expandedRegions = new Set([...expandedRegions, region]);
        }
    }

    onMount(() => {
        document.addEventListener('click', handleClickOutside);
        return () => document.removeEventListener('click', handleClickOutside);
    });
</script>

<div class="filter-container relative mb-4" bind:this={filterContainerEl}>
    <div class="flex flex-col gap-2">
        <div class="flex justify-between items-center">
            <label for="region-icb-filter-btn" class="text-sm font-medium text-gray-700"
                >Filter by Region/ICB</label
            >
            {#if selectedRegions.size > 0 || selectedICBs.size > 0}
                <button
                    type="button"
                    class="text-red-600 hover:text-red-800 font-medium text-sm"
                    on:click={() => {
                        onClear();
                        isFilterOpen = false;
                    }}
                >
                    Clear Filters
                </button>
            {/if}
        </div>
        <div class="flex">
            <button
                id="region-icb-filter-btn"
                type="button"
                on:click={toggleFilter}
                class="flex-grow p-2 text-left border border-gray-300 rounded-l-md bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-black transition-all duration-200 relative {isFilterOpen ? 'rounded-bl-none z-[997]' : ''}"
            >
                <span class="text-gray-600">Select regions and ICBs...</span>
            </button>
            <div
                class="flex items-center gap-2 bg-gray-50 px-3 border border-l-0 border-gray-300 rounded-r-md {isFilterOpen ? 'rounded-br-none' : ''} min-w-[120px]"
            >
                <div class="flex flex-col items-center text-xs text-gray-500 py-1 w-full">
                    <span class="font-medium">{selectedICBsCount}/{totalICBs}</span>
                    <span>ICBs</span>
                </div>
            </div>
        </div>
    </div>

    {#if isFilterOpen}
        <div
            class="absolute top-[calc(100%_-_1px)] left-0 right-0 bg-white border border-gray-300 rounded-md rounded-t-none shadow-lg z-[996] flex flex-col max-h-72"
        >
            <div class="overflow-y-auto divide-y divide-gray-200">
                {#each regionsHierarchy as region}
                    <div class="transition duration-150 ease-in-out divide-y divide-gray-200">
                        <div
                            role="button"
                            tabindex="0"
                            class="flex items-center justify-between cursor-pointer p-2 {selectedRegions.has(region.region) ? 'bg-oxford-100 text-oxford-500 hover:bg-oxford-200' : 'hover:bg-gray-100'}"
                            on:keydown={(e) =>
                                e.key === 'Enter' &&
                                !e.target.closest('.expand-button') &&
                                onRegionClick(region)}
                            on:click={(e) => {
                                if (!e.target.closest('.expand-button')) onRegionClick(region);
                            }}
                        >
                            <div class="flex items-center gap-2 w-full">
                                <button
                                    type="button"
                                    class="expand-button p-1 hover:bg-gray-200 rounded-full transition-colors"
                                    on:click|stopPropagation={() =>
                                        toggleRegionExpansion(region.region)}
                                    aria-label={expandedRegions.has(region.region)
                                        ? 'Collapse region'
                                        : 'Expand region'}
                                >
                                    <svg
                                        class="w-4 h-4 transform transition-transform duration-200 {expandedRegions.has(region.region) ? 'rotate-90' : ''}"
                                        fill="none"
                                        stroke="currentColor"
                                        viewBox="0 0 24 24"
                                    >
                                        <path
                                            stroke-linecap="round"
                                            stroke-linejoin="round"
                                            stroke-width="2"
                                            d="M9 5l7 7-7 7"
                                        />
                                    </svg>
                                </button>
                                <span>{region.region} ({region.region_code || ''})</span>
                                <span class="text-sm text-gray-500 ml-auto">
                                    ({region.icbs?.filter((icb) => selectedICBs.has(icb.name))
                                        .length ?? 0}/{region.icbs?.length ?? 0} ICBs)
                                </span>
                            </div>
                            {#if selectedRegions.has(region.region)}
                                <span class="ml-2 text-sm font-medium">Selected</span>
                            {/if}
                        </div>

                        {#if expandedRegions.has(region.region) && region.icbs}
                            {#each region.icbs as icb}
                                <div
                                    role="button"
                                    tabindex="0"
                                    class="pl-6 p-2 {selectedRegions.has(region.region)
                                        ? 'text-gray-400 cursor-not-allowed'
                                        : selectedICBs.has(icb.name)
                                          ? 'bg-oxford-100 text-oxford-500 hover:bg-oxford-200'
                                          : 'cursor-pointer hover:bg-gray-100'}"
                                    on:keydown={(e) =>
                                        e.key === 'Enter' && onIcbClick(region, icb)}
                                    on:click={() => onIcbClick(region, icb)}
                                >
                                    <div class="flex items-center justify-between">
                                        <div class="text-sm">
                                            <span class="mr-2">↳</span>
                                            <span>{icb.name} ({icb.code || ''})</span>
                                        </div>
                                        {#if selectedICBs.has(icb.name) &&
                                            !selectedRegions.has(region.region)}
                                            <span class="ml-auto text-sm font-medium"
                                                >Selected</span
                                            >
                                        {/if}
                                    </div>
                                </div>
                            {/each}
                        {/if}
                    </div>
                {/each}
            </div>
            <div
                class="py-2 px-3 border-t border-gray-200 flex justify-end bg-gray-50"
            >
                <button
                    type="button"
                    on:click={() => {
                        isFilterOpen = false;
                    }}
                    class="inline-flex justify-center items-center px-3 py-1.5 bg-oxford-50 text-oxford-600 rounded-md hover:bg-oxford-100 transition-colors duration-200 font-medium text-sm border border-oxford-200"
                >
                    Done
                </button>
            </div>
        </div>
    {/if}
</div>

<style>
    .filter-container {
        z-index: 997;
    }
</style>
