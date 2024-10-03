<svelte:options customElement={{
    tag: 'search-box',
    shadow: 'none'
  }} />
<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import '../../styles/styles.css';

    const dispatch = createEventDispatcher();

    export let items = [];

    let searchTerm = '';
    let searchResults = [];
    let selectedItems = [];
    let searchType = 'vmp';

    const searchTypes = [
        { value: 'vmp', label: 'VMP' },
        { value: 'vtm', label: 'VTM' },
        { value: 'ingredient', label: 'Ingredient' },
        { value: 'atc', label: 'ATC' }
    ];

    async function fetchItems(type) {
        const endpoints = {
            vmp: '/api/unique-vmp-names/',
            vtm: '/api/unique-vtm-names/',
            ingredient: '/api/unique-ingredient-names/',
            atc: '/api/unique-atc-codes/'
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
        if (searchType === 'atc') {
            searchResults = items.filter(item =>
                item.toLowerCase().includes(searchTerm.toLowerCase())
            ).sort((a, b) => {
                const aCode = a.split(' | ')[0];
                const bCode = b.split(' | ')[0];
                // First, sort by whether the item starts with the search term
                if (aCode.startsWith(searchTerm) && !bCode.startsWith(searchTerm)) return -1;
                if (!aCode.startsWith(searchTerm) && bCode.startsWith(searchTerm)) return 1;
                // Then, sort by code length
                if (aCode.length !== bCode.length) {
                    return aCode.length - bCode.length;
                }
                // If lengths are equal, sort alphanumerically
                return aCode.localeCompare(bCode);
            });
        } else {
            searchResults = items.filter(item =>
                item.toLowerCase().includes(searchTerm.toLowerCase())
            );
        }
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

    function isItemSelected(item) {
        return selectedItems.includes(item);
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
        selectedItems = [];
        dispatchSelectionChange();
    }

    onMount(() => {
        fetchItems(searchType);
    });
</script>

<div class="flex flex-col gap-2">
    <div class="flex">
        <select
            bind:value={searchType}
            class="p-2 border border-gray-300 rounded-l-md min-w-[100px] focus:outline-none focus:ring-2 focus:ring-oxford-500"
        >
            {#each searchTypes as type}
                <option value={type.value}>{type.label}</option>
            {/each}
        </select>
        <input
            type="text"
            bind:value={searchTerm}
            placeholder={`Search ${searchType.toUpperCase()} names...`}
            class="w-full p-2 border border-gray-300 rounded-r-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
        />
    </div>
    
    {#if searchResults.length > 0}
        <ul class="mb-4 border border-gray-300 rounded-b-md rounded-l-md rounded-r-md max-h-60 overflow-y-auto divide-y divide-gray-200">
            {#each searchResults as result}
                {#if isItemSelected(result)}
                    <li class="p-2 bg-oxford-200 text-oxford-700 flex items-center">
                        <span>{result}</span>
                        <span class="ml-auto text-sm font-medium">Selected</span>
                    </li>
                {:else}
                    <li 
                        class="p-2 hover:bg-gray-100 cursor-pointer transition duration-150 ease-in-out"
                        on:click={() => addItem(result)}
                    >
                        {result}
                    </li>
                {/if}
            {/each}
        </ul>
    {/if}

    {#if selectedItems.length > 0}
        <div>
            <h3 class="font-semibold mb-2 text-md text-gray-700">Selected {searchType.toUpperCase()} Names:</h3>
            <ul class="border border-gray-200 rounded-md">
                {#each selectedItems as item}
                    <li class="flex items-center justify-between px-2 py-1">
                        <span class="text-gray-800">{item}</span>
                        <button 
                            on:click={() => removeItem(item)}
                            class="btn-red-sm"
                        >
                            Remove
                        </button>
                    </li>
                {/each}
            </ul>
        </div>
    {/if}
</div>
