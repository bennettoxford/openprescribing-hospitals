<svelte:options customElement={{
    tag: 'collapsible-section',
    props: {
        title: { type: 'String' },
        description: { type: 'String' },
        items: { type: 'String' },
        numeratorItems: { type: 'String' },
        previewCount: { type: 'Number' }
    },
    shadow: 'none'
}} />

<script>
    export let title = '';
    export let description = '';
    export let items = '[]';
    export let numeratorItems = '[]';
    export let previewCount = 3;

    let isExpanded = false;
    let parsedItems = [];
    let parsedNumeratorItems = [];
    let sortedItems = [];

    $: {
        try {
            parsedItems = JSON.parse(items);
            parsedNumeratorItems = JSON.parse(numeratorItems);
            
            const sortedNumeratorItems = parsedNumeratorItems
                .sort((a, b) => a.name.localeCompare(b.name));
            const sortedNonNumeratorItems = parsedItems
                .filter(item => !isInNumerator(item))
                .sort((a, b) => a.name.localeCompare(b.name));
            
            sortedItems = [...sortedNumeratorItems, ...sortedNonNumeratorItems];
        } catch (error) {
            console.error('Failed to parse items:', error);
            parsedItems = [];
            parsedNumeratorItems = [];
            sortedItems = [];
        }
    }

    function toggleExpanded() {
        isExpanded = !isExpanded;
    }

    function isInNumerator(item) {
        return parsedNumeratorItems.some(numItem => numItem.code === item.code);
    }

    $: displayedItems = isExpanded || sortedItems.length <= previewCount ? sortedItems : sortedItems.slice(0, previewCount);
    $: remainingCount = sortedItems.length - previewCount;
    $: numeratorCount = parsedNumeratorItems.length;
    $: denominatorCount = parsedItems.length;
</script>

<div class="border border-gray-200 rounded-lg shadow-sm my-4 overflow-hidden">
    <div class="px-4 py-3 bg-white">
        <div class="flex justify-between items-center mb-2">
            <h3 class="text-lg font-semibold text-gray-800">{title}</h3>
        </div>

        <p class="text-sm text-gray-600 mb-2">
            <span class="block mt-1">
                There are <span class="font-semibold">{denominatorCount} products</span> included in the denominator for this measure, 
                of which <span class="font-semibold">{numeratorCount}</span> are included in the numerator.
            </span>
        </p>
        
        <div class="flex flex-wrap gap-2 mb-4 text-xs text-gray-600">
            <div class="inline-flex items-center gap-1 bg-oxford-50 px-2 py-1 rounded">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24">
                    <rect x="6" y="3" width="12" height="5" fill="#003D73" rx="1" />
                    <rect x="6" y="16" width="12" height="5" fill="#003D73" rx="1" />
                    <rect x="4" y="11.5" width="16" height="1" fill="#003D73" />
                </svg>
                <span>Numerator and denominator</span>
            </div>
            <div class="inline-flex items-center gap-1 bg-gray-50 px-2 py-1 rounded">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24">
                    <rect x="6" y="3" width="12" height="5" fill="none" stroke="#9CA3AF" stroke-width="1" rx="1" />
                    <rect x="6" y="16" width="12" height="5" fill="#9CA3AF" rx="1" />
                    <rect x="4" y="11.5" width="16" height="1" fill="#9CA3AF" />
                </svg>
                <span>Denominator only</span>
            </div>
        </div>
     
      

        <div class="overflow-x-auto">
            <div class={`overflow-y-auto transition-all duration-200 ease-in-out ${isExpanded ? 'max-h-[24rem]' : 'max-h-[20rem]'}`}>
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50 sticky top-0 z-10">
                        <tr>
                            <th scope="col" class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">Product Name</th>
                            <th scope="col" class="hidden lg:table-cell px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">Product Code</th>
                            <th scope="col" class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">Unit of measure</th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
                        {#each displayedItems as item}
                            {@const inNumerator = isInNumerator(item)}
                            <tr class="hover:bg-gray-50 transition-colors duration-150 ease-in-out"
                                class:bg-oxford-50={inNumerator}>
                                <td class="px-3 py-2 text-sm font-medium text-gray-900">
                                    <div class="flex items-center gap-2">
                                        {#if inNumerator}
                                            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 flex-shrink-0" viewBox="0 0 24 24">
                                                <rect x="6" y="3" width="12" height="5" fill="#003D73" rx="1" />
                                                <rect x="6" y="16" width="12" height="5" fill="#003D73" rx="1" />
                                                <rect x="4" y="11.5" width="16" height="1" fill="#003D73" />
                                            </svg>
                                        {:else}
                                            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 flex-shrink-0" viewBox="0 0 24 24">
                                                <rect x="6" y="3" width="12" height="5" fill="none" stroke="#9CA3AF" stroke-width="1" rx="1" />
                                                <rect x="6" y="16" width="12" height="5" fill="#9CA3AF" rx="1" />
                                                <rect x="4" y="11.5" width="16" height="1" fill="#9CA3AF" />
                                            </svg>
                                        {/if}
                                        <span>{item.name}</span>
                                    </div>
                                </td>
                                <td class="hidden lg:table-cell px-3 py-2 text-sm text-gray-500">{item.code}</td>
                                <td class="px-3 py-2 text-sm text-gray-500">{item.unit || '-'}</td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>

        {#if !isExpanded && remainingCount > 0}
            <div class="mt-2 text-center border-t border-gray-100 pt-2">
                <button 
                    on:click={toggleExpanded}
                    class="text-sm text-oxford-600 hover:text-oxford-800 inline-flex items-center gap-1.5 py-1 px-3 hover:bg-oxford-50 rounded-full transition-colors"
                >
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                    <span>Show all {sortedItems.length} items</span>
                </button>
            </div>
        {:else if isExpanded}
            <div class="mt-2 text-center border-t border-gray-100 pt-2">
                <button 
                    on:click={toggleExpanded}
                    class="text-sm text-oxford-600 hover:text-oxford-800 inline-flex items-center gap-1.5 py-1 px-3 hover:bg-oxford-50 rounded-full transition-colors"
                >
                    <svg class="w-4 h-4 rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                    <span>Show fewer</span>
                </button>
            </div>
        {/if}
    </div>
</div>
