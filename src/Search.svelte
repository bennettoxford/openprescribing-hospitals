<svelte:options customElement={{
    tag: 'search-box',
    shadow: 'none'
  }} />
<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import './styles/styles.css';

    const dispatch = createEventDispatcher();

    export let items = [];
    let searchTerm = '';
    let searchResults = [];
    let selectedItems = [];

    function search() {
        searchResults = items.filter(item =>
            item.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }

    function addItem(item) {
        if (!selectedItems.includes(item)) {
            selectedItems = [...selectedItems, item];
            dispatchSelectionChange();
        }
        searchTerm = '';
        searchResults = [];
    }

    function removeItem(item) {
        selectedItems = selectedItems.filter(i => i !== item);
        dispatchSelectionChange();
    }

    function dispatchSelectionChange() {
        dispatch('selectionChange', selectedItems);
    }

    $: {
        if (searchTerm.length > 0) {
            search();
        } else {
            searchResults = [];
        }
    }

    onMount(() => {
        // Any initialization code if needed
    });
</script>

<div class="p-4 border border-gray-300 rounded-md">
    <input
        type="text"
        bind:value={searchTerm}
        placeholder="Search VMP names..."
        class="w-full p-2 border border-gray-300 rounded-md"
    />
    
    {#if searchResults.length > 0}
        <ul class="mt-2 border border-gray-300 rounded-md">
            {#each searchResults as result}
                <li 
                    class="p-2 hover:bg-gray-100 cursor-pointer"
                    on:click={() => addItem(result)}
                >
                    {result}
                </li>
            {/each}
        </ul>
    {/if}

    {#if selectedItems.length > 0}
        <div class="mt-4">
            <h3 class="font-bold">Selected VMP Names:</h3>
            <ul class="mt-2">
                {#each selectedItems as item}
                    <li class="flex items-center justify-between p-2 bg-gray-100 rounded-md mb-2">
                        {item}
                        <button 
                            on:click={() => removeItem(item)}
                            class="text-red-500 hover:text-red-700"
                        >
                            Remove
                        </button>
                    </li>
                {/each}
            </ul>
        </div>
    {/if}
</div>
