<svelte:options customElement={{
    tag: 'collapsible-section',
    props: {
        title: { type: 'String' },
        items: { type: 'String' },
        numeratorItems: { type: 'String' },
        previewCount: { type: 'Number' }
    },
    shadow: 'none'
}} />

<script>
    export let title = '';
    export let items = '[]';
    export let numeratorItems = '[]';
    export let previewCount = 1;

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

    $: displayedItems = isExpanded ? sortedItems : sortedItems.slice(0, previewCount);
    $: remainingCount = sortedItems.length - previewCount;
</script>

<div class="border border-gray-200 rounded-lg shadow-sm my-4 overflow-hidden">
    <div class="px-4 py-3 bg-white">
        <div class="flex justify-between items-center mb-4">
            <h3 class="text-lg font-semibold text-gray-800">{title}</h3>
            <button 
                on:click={toggleExpanded}
                class="text-sm text-oxford-600 hover:text-oxford-800 font-medium flex items-center gap-1"
            >
                {isExpanded ? 'Show less' : `Show all (${sortedItems.length})`}
                <svg class:rotate-180={isExpanded} class="w-4 h-4 text-current transition-transform duration-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                </svg>
            </button>
        </div>

        <div class="overflow-x-auto">
            <div class={`overflow-y-auto transition-all duration-200 ease-in-out ${isExpanded ? 'max-h-[24rem]' : 'max-h-[20rem]'}`}>
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50 sticky top-0 z-10">
                        <tr>
                            <th scope="col" class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">Product Name</th>
                            <th scope="col" class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">VMP Code</th>
                            <th scope="col" class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">Unit of measure</th>
                            <th scope="col" class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">Measure component</th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
                        {#each displayedItems as item}
                            {@const inNumerator = isInNumerator(item)}
                            <tr class="hover:bg-gray-50 transition-colors duration-150 ease-in-out"
                                class:bg-oxford-50={inNumerator}>
                                <td class="px-3 py-2 text-sm font-medium text-gray-900">{item.name}</td>
                                <td class="px-3 py-2 text-sm text-gray-500">{item.code}</td>
                                <td class="px-3 py-2 text-sm text-gray-500">{item.unit || '-'}</td>
                                <td class="px-3 py-2">
                                    <div class="flex flex-col gap-1">
                                        {#if inNumerator}
                                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-oxford-100 text-oxford-800">
                                                Numerator
                                            </span>
                                        {/if}
                                        <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-800">
                                            Denominator
                                        </span>
                                    </div>
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>

        {#if !isExpanded && remainingCount > 0}
            <div class="mt-2 text-sm text-gray-500 text-center">
                and {remainingCount} more items...
            </div>
        {/if}
    </div>
</div>
