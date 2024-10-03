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
    let selectedChildItems = new Set();
    let searchType = 'vmp';

    const searchTypes = [
        { value: 'vmp', label: 'VMP' },
        { value: 'vtm', label: 'VTM' },
        { value: 'ingredient', label: 'Ingredient' },
        { value: 'atc', label: 'ATC' }
    ];

    let atcHierarchy = {};

    let vmpCount = 0;
    let isCalculating = false;

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

    async function fetchAtcHierarchy() {
        try {
            const response = await fetch('/api/atc-hierarchy/');
            if (response.ok) {
                atcHierarchy = await response.json();
            } else {
                console.error('Failed to fetch ATC hierarchy:', response.status, response.statusText);
            }
        } catch (error) {
            console.error('Error fetching ATC hierarchy:', error);
        }
    }

    async function fetchAtcCodes() {
        try {
            const response = await fetch('/api/unique-atc-codes/');
            if (response.ok) {
                const data = await response.json();
                items = data.map(item => item.name);
                atcHierarchy = data.reduce((acc, item) => {
                    acc[item.code] = item.children;
                    return acc;
                }, {});
            } else {
                console.error('Failed to fetch ATC codes:', response.status, response.statusText);
                items = [];
            }
        } catch (error) {
            console.error('Error fetching ATC codes:', error);
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
        if (searchType === 'atc') {
            const [selectedCode, selectedName] = item.split(' | ');
            if (!selectedItems.some(i => i.code === selectedCode)) {
                selectedItems = [...selectedItems, { code: selectedCode, name: selectedName }];
                // Add all child codes to selectedChildItems
                function addChildren(code) {
                    if (atcHierarchy[code]) {
                        atcHierarchy[code].forEach(childCode => {
                            selectedChildItems.add(childCode);
                            addChildren(childCode);
                        });
                    }
                }
                addChildren(selectedCode);
                selectedChildItems = new Set(selectedChildItems);
            }
        } else {
            if (!selectedItems.includes(item)) {
                selectedItems = [...selectedItems, item];
            }
        }
        dispatchSelectionChange();
        searchTerm = '';
        searchResults = [];
    }

    function removeItem(item) {
        if (searchType === 'atc') {
            const codeToRemove = typeof item === 'string' ? item.split(' | ')[0] : item.code;
            selectedItems = selectedItems.filter(i => i.code !== codeToRemove);
            // Remove all child codes from selectedChildItems
            function removeChildren(code) {
                if (atcHierarchy[code]) {
                    atcHierarchy[code].forEach(childCode => {
                        selectedChildItems.delete(childCode);
                        removeChildren(childCode);
                    });
                }
            }
            removeChildren(codeToRemove);
            selectedChildItems = new Set(selectedChildItems); 
        } else {
            selectedItems = selectedItems.filter(i => i !== item);
        }
        dispatchSelectionChange();
    }

    function dispatchSelectionChange() {
        dispatch('selectionChange', { 
            type: searchType, 
            items: searchType === 'atc' ? selectedItems.map(i => i.code) : selectedItems 
        });
        fetchVmpCount();
    }

    async function fetchVmpCount() {
        if (selectedItems.length === 0) {
            vmpCount = 0;
            return;
        }

        isCalculating = true;
        try {
            const response = await fetch('/api/filtered-vmp-count/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    names: searchType === 'atc' ? selectedItems.map(i => i.code) : selectedItems,
                    search_type: searchType,
                }),
            });
            const data = await response.json();
            vmpCount = data.vmp_count;
        } catch (error) {
            console.error('Error fetching VMP count:', error);
        } finally {
            isCalculating = false;
        }
    }

    function isItemSelected(item) {
        if (searchType === 'atc') {
            const [code] = item.split(' | ');
            return selectedItems.some(i => i.code === code) || selectedChildItems.has(code);
        }
        return selectedItems.includes(item);
    }

    onMount(() => {
        if (searchType === 'atc') {
            fetchAtcCodes();
        } else {
            fetchItems(searchType);
        }
    });

    $: {
        if (searchTerm.length > 0) {
            search();
        } else {
            searchResults = [];
        }
    }

    $: {
        if (searchType === 'atc') {
            fetchAtcCodes();
        } else {
            fetchItems(searchType);
        }
        selectedItems = [];
        dispatchSelectionChange();
    }

    $: {
        if (searchType === 'atc' && !atcHierarchy) {
            fetchAtcHierarchy();
        }
    }

    onMount(() => {
        if (searchType === 'atc') {
            fetchAtcCodes();
        } else {
            fetchItems(searchType);
        }
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
                {@const isSelected = isItemSelected(result)}
                <li 
                    class="p-2 transition duration-150 ease-in-out flex items-center"
                    class:bg-oxford-200={isSelected}
                    class:text-oxford-700={isSelected}
                    class:hover:bg-gray-100={!isSelected}
                    class:cursor-pointer={!isSelected}
                    on:click={() => !isSelected && addItem(result)}
                >
                    <span>{result}</span>
                    {#if isSelected}
                        <span class="ml-auto text-sm font-medium">Selected</span>
                    {/if}
                </li>
            {/each}
        </ul>
    {/if}

    {#if selectedItems.length > 0}
        <div>
            <h3 class="font-semibold mb-2 text-md text-gray-700">Selected {searchType.toUpperCase()} Names:</h3>
            <ul class="border border-gray-200 rounded-md">
                {#each selectedItems as item}
                    <li class="flex items-center justify-between px-2 py-1">
                        <span class="text-gray-800">
                            {searchType === 'atc' ? `${item.code} | ${item.name}` : item}
                        </span>
                        <button 
                            on:click={() => removeItem(item)}
                            class="btn-red-sm"
                        >
                            Remove
                        </button>
                    </li>
                {/each}
            </ul>
            <p class="mt-2 text-sm text-gray-600">
                {#if isCalculating}
                    Calculating number of individual products analysed...
                {:else if vmpCount > 0}
                    This selection will analyse {vmpCount} unique VMP{vmpCount !== 1 ? 's' : ''}.
                    {#if vmpCount > 100}
                        <span class="text-amber-600 font-semibold">
                            Warning: This is a large number of products. The analysis may take a long time. 
                            Consider making your selection more specific.
                        </span>
                    {/if}
                {/if}
            </p>
        </div>
    {/if}
</div>