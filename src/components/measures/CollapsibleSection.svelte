<svelte:options customElement={{
    tag: 'collapsible-section',
    props: {
        title: { type: 'String' },
        items: { type: 'String' },
        numeratorItems: { type: 'String' }
    },
    shadow: 'none'
}} />

<script>
    export let title = '';
    export let items = '[]';
    export let numeratorItems = '[]';

    let isOpen = false;
    let parsedItems = [];
    let parsedNumeratorItems = [];
    let sortedItems = [];

    $: {
        try {
            parsedItems = JSON.parse(items);
            parsedNumeratorItems = JSON.parse(numeratorItems);
            
            // Sort items alphabetically
            const sortedNumeratorItems = parsedNumeratorItems
                .sort((a, b) => a.name.localeCompare(b.name));
            const sortedNonNumeratorItems = parsedItems
                .filter(item => !isInNumerator(item))
                .sort((a, b) => a.name.localeCompare(b.name));
            
            // Combine sorted numerator and non-numerator items
            sortedItems = [...sortedNumeratorItems, ...sortedNonNumeratorItems];
        } catch (error) {
            console.error('Failed to parse items:', error);
            parsedItems = [];
            parsedNumeratorItems = [];
            sortedItems = [];
        }
    }

    function toggleOpen() {
        isOpen = !isOpen;
    }

    function isInNumerator(item) {
        return parsedNumeratorItems.some(numItem => numItem.code === item.code);
    }
</script>

<div class="border border-gray-200 rounded-lg shadow-sm my-4 overflow-hidden">
    <button on:click={toggleOpen} class="flex justify-between items-center w-full px-4 sm:px-6 py-3 bg-white hover:bg-gray-50 transition-colors duration-200">
        <h3 class="text-base sm:text-lg font-semibold text-gray-800">{title}</h3>
        <svg class:rotate-180={isOpen} class="w-4 h-4 sm:w-5 sm:h-5 text-gray-500 transition-transform duration-200" fill="currentColor" viewBox="0 0 20 20">
            <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd"/>
        </svg>
    </button>
    {#if isOpen}
        <div class="px-4 sm:px-6 py-2 sm:py-3 bg-gray-50">
            <p class="text-sm text-gray-600 mb-3">
                All products below are included in the denominator of this measure. A subset of these products are included in the numerator. This results in a proportion between 0 and 1.
            </p>
            <div class="max-h-60 sm:max-h-96 overflow-y-auto pr-2">
                <ul class="space-y-1.5">
                    {#each sortedItems as item}
                        {@const inNumerator = isInNumerator(item)}
                        <li class="flex flex-col sm:flex-row sm:items-center justify-between px-3 sm:px-4 py-1.5 sm:py-2 bg-white rounded-lg shadow-sm transition duration-150 ease-in-out"
                            class:bg-oxford-100={inNumerator}>
                            <span class="font-medium text-sm">{item.name}</span>
                            <div class="flex flex-col sm:flex-row items-start sm:items-center space-y-0.5 sm:space-y-0 sm:space-x-2">
                                <span class="text-xs text-gray-500">VMP: {item.code}</span>
                                <div class="flex flex-col space-y-1">
                                    {#if inNumerator}
                                        <span class="text-xs font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded-full bg-oxford-200 text-oxford-800">
                                            Numerator
                                        </span>
                                    {/if}
                                    <span class="text-xs font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded-full bg-gray-200 text-gray-800">
                                        Denominator
                                    </span>
                                </div>
                            </div>
                        </li>
                    {/each}
                </ul>
            </div>
        </div>
    {/if}
</div>
