<svelte:options customElement={{
    tag: 'search-box',
    shadow: 'none'
  }} />
<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import '../../styles/styles.css';
    import { getCookie } from '../../utils/utils';
    import { analyseOptions } from '../../stores/analyseOptionsStore';
    const dispatch = createEventDispatcher();

    export let placeholder = "Search by product name or code...";
    export let type = "product";
    export let isAdvancedMode = false;

    let searchTerm = '';
    let filteredItems = [];
    let selectedItems = [];
    let vmpCount = 0;
    let isCalculating = false;

    const csrftoken = getCookie('csrftoken');

    let searchTimeout;
    let isLoading = false;
    let lastSearchResults = [];
    let searchBoxRef;

    $: {
        if (type === 'product') {
            filteredItems = filterItems($analyseOptions.productNames, searchTerm);
        } else if (type === 'ingredient') {
            filteredItems = filterItems($analyseOptions.ingredientNames, searchTerm);
        } else if (type === 'atc') {
            filteredItems = filterItems($analyseOptions.atcNames, searchTerm);
        }
    }

    function filterItems(items, term) {
        if (!items) return [];
        if (type === 'atc') {
            return items.filter(item => 
                item.name.toLowerCase().includes(term.toLowerCase())
            );
        }
        return items.filter(item => 
            item.toLowerCase().includes(term.toLowerCase())
        );
    }

    async function handleInput() {
        clearTimeout(searchTimeout);
        
        if (!searchTerm || searchTerm.length < 1) {
            filteredItems = [];
            lastSearchResults = [];
            isLoading = false;
            return;
        }
        
        isLoading = true;
        
        searchTimeout = setTimeout(async () => {
            try {
                const response = await fetch(`/api/search/?type=${type}&term=${encodeURIComponent(searchTerm)}`);
                const data = await response.json();
                
                filteredItems = data.results;
                console.log(filteredItems);
                lastSearchResults = data.results;
            } catch (error) {
                console.error('Error fetching search results:', error);
                filteredItems = [];
                lastSearchResults = [];
            } finally {
                isLoading = false;
            }
        }, 300);
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
                    'X-CSRFToken': csrftoken,
                },
                body: JSON.stringify({
                    names: selectedItems,
                    search_type: type,
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

    function handleSelect(item) {
        const selectedItem = type === 'atc' ? item.name : item;
        if (!selectedItems.includes(selectedItem)) {
            selectedItems = [...selectedItems, selectedItem];
            dispatch('selectionChange', { items: selectedItems, type });
            fetchVmpCount();
        } else {
            selectedItems = selectedItems.filter(i => i !== selectedItem);
            dispatch('selectionChange', { items: selectedItems, type });
            fetchVmpCount();
        }
        filteredItems = lastSearchResults;
    }

    function handleRemove(item) {
        selectedItems = selectedItems.filter(i => i !== item);
        dispatch('selectionChange', { items: selectedItems, type });
        fetchVmpCount();
    }

    function handleTypeChange(newType) {
        type = newType;
        selectedItems = [];
        searchTerm = '';
        filteredItems = []; // Clear filtered items when changing type
        dispatch('selectionChange', { items: selectedItems, type });
        vmpCount = 0;
    }

    $: {
        if ($analyseOptions.selectedVMPs.length === 0 && selectedItems.length > 0) {
            selectedItems = [];
            searchTerm = '';
            vmpCount = 0;
            filteredItems = []; // Only clear filtered items when resetting everything
        }
    }
</script>

<div 
    class="w-full search-box relative"
    bind:this={searchBoxRef}
>
    {#if isAdvancedMode}
        <div class="flex space-x-2 mb-2 pointer-events-auto">
            <button class="px-2 py-1 rounded {type === 'product' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('product')}>Product</button>
            <button class="px-2 py-1 rounded {type === 'ingredient' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('ingredient')}>Ingredient</button>
            <button class="px-2 py-1 rounded {type === 'atc' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('atc')}>ATC</button>
        </div>
    {/if}
    <div class="relative pointer-events-auto">
        <input
            type="text"
            {placeholder}
            bind:value={searchTerm}
            on:input={handleInput}
            on:focus={() => {
                if (searchTerm && lastSearchResults.length > 0) {
                    filteredItems = lastSearchResults;
                }
            }}
            on:keydown={(e) => {
                if (e.key === 'Escape' && searchTerm) {
                    e.preventDefault();
                    searchTerm = '';
                    filteredItems = [];
                    lastSearchResults = [];
                }
            }}
            class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oxford-500 pr-8
                   {filteredItems.length > 0 ? 'rounded-b-none' : ''}"
        />
        {#if isLoading}
            <div class="absolute right-8 top-0 h-full flex items-center">
                <div class="animate-spin h-4 w-4 border-2 border-oxford-500 rounded-full border-t-transparent"></div>
            </div>
        {/if}
        {#if searchTerm}
            <button
                class="absolute right-2 top-0 h-full flex items-center justify-center text-gray-400 hover:text-gray-600 w-5"
                on:click|stopPropagation={() => searchTerm = ''}
            >
                ✕
            </button>
        {/if}
    </div>
    {#if filteredItems.length > 0 || isLoading}
        <div class="fixed inset-0 bg-transparent" on:click={() => filteredItems = []}></div>
        <div class="relative z-10">
            <ul class="border border-gray-300 rounded-none border-t-0 max-h-96 overflow-y-auto divide-y divide-gray-200">
                {#each filteredItems as item}
                    <li 
                        class="group pt-4 pb-1 px-2 cursor-pointer flex items-center justify-between relative transition-colors duration-150 ease-in-out"
                        class:bg-oxford-100={selectedItems.includes(type === 'atc' ? item.name : item)}
                        class:text-gray-400={type === 'atc' && !item.has_vmps}
                        class:pointer-events-none={type === 'atc' && !item.has_vmps}
                        class:hover:bg-gray-100={true}
                        on:click={() => handleSelect(item)}
                    >
                        <span class="mt-1">
                            {#if type === 'atc'}
                                {item.name.split('|')[1].trim()}
                            {:else}
                                {item.name}
                            {/if}
                        </span>
                        <span class="absolute top-0 right-0 text-xs px-2 py-1 rounded-bl transition-colors duration-150 ease-in-out
                                 {selectedItems.includes(type === 'atc' ? item.name : item) 
                                    ? 'bg-oxford-100 text-oxford-700 group-hover:bg-gray-100' 
                                    : 'bg-gray-100 text-gray-600 group-hover:bg-gray-200'}">
                            {#if type === 'atc'}
                                ATC code: {item.name.split('|')[0].trim()}
                            {:else}
                                {#if item.type === 'vtm'}
                                    VTM code: {item.code}
                                {:else if item.type === 'vmp'}
                                    VMP code: {item.code}
                                {:else}
                                    Ingredient code: {item.code}
                                {/if}
                           
                            {/if}
                        </span>
                        {#if type === 'atc' && !item.has_vmps}
                            <span class="text-sm text-gray-400">(No products)</span>
                        {:else if selectedItems.includes(type === 'atc' ? item.name : item)}
                            <span class="text-sm font-medium text-oxford-500 mt-2">Selected</span>
                        {/if}
                    </li>
                {/each}
            </ul>
            <button
                on:click={() => {
                    const listContainer = document.querySelector('.search-box .overflow-y-auto');
                    if (listContainer) {
                        listContainer.scrollTo({ top: 0 });
                    }
                }}
                class="w-full p-2 bg-gray-100 border-t border-gray-200 flex items-center justify-center hover:bg-oxford-50 active:bg-oxford-100 cursor-pointer transition-colors duration-150 gap-2 rounded-b-md"
            >
                <span class="text-sm font-medium text-gray-700">
                    {selectedItems.length} {type === 'product' ? type : type.toUpperCase()}s selected • Click to scroll to top
                </span>
                <svg class="w-4 h-4 text-gray-700" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
                </svg>
            </button>
        </div>
    {/if}
    {#if selectedItems.length > 0}
        <div>
            <h3 class="font-semibold my-2 text-md text-gray-700">Selected {type === 'product' ? type : type.toUpperCase()} names:</h3>
            <ul class="border border-gray-200 rounded-md">
                {#each selectedItems as item}
                    <li class="flex items-center justify-between px-2 py-1">
                        <span class="text-gray-800">
                            {typeof item === 'string' ? item : `${item.code} | ${item.name}`}
                        </span>
                        <button 
                            on:click={() => handleRemove(item)}
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
