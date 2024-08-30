<svelte:options customElement={{
    tag: 'search-box',
    shadow: 'none'
  }} />
<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import './styles/styles.css';

    const dispatch = createEventDispatcher();

    let searchTerm = '';
    let searchResults = [];
    let selectedItems = [];
    let searchType = 'vmp'; // Default search type
    let items = [];

    const searchTypes = [
        { value: 'vmp', label: 'VMP' },
        { value: 'vtm', label: 'VTM' },
        { value: 'ingredient', label: 'Ingredient' }
    ];

    async function fetchItems(type) {
        const endpoints = {
            vmp: '/api/unique-vmp-names/',
            vtm: '/api/unique-vtm-names/',
            ingredient: '/api/unique-ingredient-names/'
        };

        try {
            const response = await fetch(endpoints[type]);
            if (response.ok) {
                items = await response.json();
            } else {
                console.error('Failed to fetch items:', response.status, response.statusText);
                items = [];
            }
        } catch (error) {
            console.error('Error fetching items:', error);
            items = [];
        }
    }

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
        dispatch('selectionChange', { type: searchType, items: selectedItems });
    }

    $: {
        if (searchTerm.length > 0) {
            search();
        } else {
            searchResults = [];
        }
    }

    $: {
        fetchItems(searchType);
        selectedItems = []; // Clear selected items when search type changes
        dispatchSelectionChange();
    }

    onMount(() => {
        fetchItems(searchType);
    });
</script>

<div class="p-4 border border-gray-300 rounded-md">
    <div class="flex mb-2">
        <select
            bind:value={searchType}
            class="p-2 border border-gray-300 rounded-l-md"
        >
            {#each searchTypes as type}
                <option value={type.value}>{type.label}</option>
            {/each}
        </select>
        <input
            type="text"
            bind:value={searchTerm}
            placeholder={`Search ${searchType.toUpperCase()} names...`}
            class="w-full p-2 border border-gray-300 rounded-r-md"
        />
    </div>
    
    {#if searchResults.length > 0}
        <ul class="mt-2 border border-gray-300 rounded-md max-h-60 overflow-y-auto">
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
            <h3 class="font-bold">Selected {searchType.toUpperCase()} Names:</h3>
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
