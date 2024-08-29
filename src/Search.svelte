<svelte:options customElement={{
    tag: 'search-box',
    shadow: 'none'
  }} />
<script>
    import { onMount } from 'svelte';
    import './styles/styles.css';

    let searchTerm = '';
    let searchResults = [];
    let selectedItems = [];

    function search() {
        // Simulated search results - replace with actual API call
        searchResults = ['Result 1', 'Result 2', 'Result 3'].filter(result =>
            result.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }

    function addItem(item) {
        if (!selectedItems.includes(item)) {
            selectedItems = [...selectedItems, item];
        }
        searchTerm = '';
        searchResults = [];
    }

    function removeItem(item) {
        selectedItems = selectedItems.filter(i => i !== item);
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
        placeholder="Search..."
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
            <h3 class="font-bold">Selected Items:</h3>
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
