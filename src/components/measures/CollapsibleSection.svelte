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

<div class="border rounded-lg shadow-sm my-4">
    <button on:click={toggleOpen} class="flex justify-between items-center w-full px-4 py-2 bg-gray-100 hover:bg-gray-200 transition-colors duration-200 rounded-t-lg">
        <h3 class="text-lg font-semibold text-gray-800">{title}</h3>
        <svg class:rotate-180={isOpen} class="w-5 h-5 transition-transform duration-200" fill="currentColor" viewBox="0 0 20 20">
            <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd"/>
        </svg>
    </button>
    {#if isOpen}
        <div class="px-4 py-2">
            <div class="max-h-60 overflow-y-auto">
                <ul class="space-y-1 border border-gray-200 rounded-md">
                    {#each sortedItems as item}
                        {@const inNumerator = isInNumerator(item)}
                        <li class="flex items-center justify-between px-2 py-1 transition duration-150 ease-in-out"
                            class:bg-oxford-200={inNumerator}
                            class:text-oxford-700={inNumerator}>
                            <span>{item.name} (VMP: {item.code})</span>
                            {#if inNumerator}
                                <span class="text-sm font-medium">Numerator</span>
                            {/if}
                        </li>
                    {/each}
                </ul>
            </div>
        </div>
    {/if}
</div>
