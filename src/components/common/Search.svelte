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

    let selectedDisplayNames = {};

    let listContainer;
    let showScrollTop = false;

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
        
        if (!searchTerm || searchTerm.length < 3) {
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
                
                filteredItems = data.results.map(item => ({
                    ...item,
                    isExpanded: item.type === 'vtm' ? true : false
                }));
                lastSearchResults = filteredItems;
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
            if (data.display_names) {
                selectedDisplayNames = data.display_names;
            }
        } catch (error) {
            console.error('Error fetching VMP count:', error);
        } finally {
            isCalculating = false;
        }
    }

    function handleSelect(item) {
        if (item.type === 'vtm') {
            if (!selectedItems.includes(item.code)) {
                const vmpCodes = item.vmps.map(vmp => vmp.code);
                selectedItems = selectedItems.filter(i => !vmpCodes.includes(i));
                selectedItems = [...selectedItems, item.code];
            } else {
                selectedItems = selectedItems.filter(i => i !== item.code);
            }
        } else {
            const parentVtm = filteredItems.find(vtm => 
                vtm.type === 'vtm' && vtm.vmps?.some(vmp => vmp.code === item.code)
            );
            if (!parentVtm || !selectedItems.includes(parentVtm.code)) {
                if (!selectedItems.includes(item.code)) {
                    selectedItems = [...selectedItems, item.code];
                } else {
                    selectedItems = selectedItems.filter(i => i !== item.code);
                }
            }
        }
        
        dispatch('selectionChange', { items: selectedItems, type });
        fetchVmpCount();
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
        filteredItems = [];
        dispatch('selectionChange', { items: selectedItems, type });
        vmpCount = 0;
    }

    $: {
        if ($analyseOptions.selectedVMPs.length === 0 && selectedItems.length > 0) {
            selectedItems = [];
            searchTerm = '';
            vmpCount = 0;
            filteredItems = [];
        }
    }

    function toggleExpand(item, event) {
        event.preventDefault();
        event.stopPropagation();
        item.isExpanded = !item.isExpanded;
        filteredItems = [...filteredItems];
    }

    function updateScrollButtonVisibility() {
        if (listContainer) {
            showScrollTop = listContainer.scrollHeight > listContainer.clientHeight && listContainer.scrollTop > 0;
        }
    }

    onMount(() => {
        setTimeout(() => {
            listContainer = document.querySelector('.search-box .overflow-y-auto');
            if (listContainer) {
                listContainer.addEventListener('scroll', updateScrollButtonVisibility);
                updateScrollButtonVisibility();
            }
        }, 100);

        return () => {
            if (listContainer) {
                listContainer.removeEventListener('scroll', updateScrollButtonVisibility);
            }
        };
    });

    $: if (filteredItems && listContainer) {
        setTimeout(updateScrollButtonVisibility, 50);
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
    <div class="grid gap-4">
        <div class="relative pointer-events-auto">
            <div class="relative">
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
                    <div class="absolute right-8 top-1/2 -translate-y-1/2 flex items-center z-20">
                        <div class="animate-spin h-4 w-4 border-2 border-oxford-500 rounded-full border-t-transparent"></div>
                    </div>
                {/if}
                {#if searchTerm}
                    <button
                        class="absolute right-2 top-1/2 -translate-y-1/2 flex items-center justify-center text-gray-400 hover:text-gray-600 w-5 z-20"
                        on:click|stopPropagation={() => searchTerm = ''}
                    >
                        ✕
                    </button>
                {/if}
            </div>

            {#if searchTerm && searchTerm.length < 3}
                <div class="absolute top-[calc(100%_-_1px)] left-0 right-0 bg-white border border-gray-300 rounded-b-md shadow-lg z-50 p-3">
                    <p class="text-gray-600 text-sm">
                        Please type at least 3 characters to start searching
                    </p>
                </div>
            {:else if filteredItems.length > 0 || isLoading}
                <div class="fixed inset-0 bg-transparent" on:click={() => filteredItems = []}></div>
                <div class="z-10">
                    <ul class="border border-gray-300 rounded-none border-t-0 max-h-96 overflow-y-auto divide-y divide-gray-200 bg-white {!showScrollTop ? 'rounded-b-md' : ''}"
                        bind:this={listContainer}
                        on:scroll={updateScrollButtonVisibility}>
                        {#each filteredItems as item}
                            {#if item.type === 'vtm'}
                                <li class="group">
                                    <div 
                                        class="pt-2 pb-1 px-3 cursor-pointer flex items-center justify-between relative transition-colors duration-150 ease-in-out hover:bg-gray-50"
                                        class:bg-oxford-50={selectedItems.includes(item.code)}
                                        on:click={() => handleSelect(item)}
                                    >
                                        <div class="flex-1">
                                            <div class="flex items-center gap-2">
                                                {#if item.vmps?.length > 0}
                                                    <button 
                                                        class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                        on:click|stopPropagation={(event) => toggleExpand(item, event)}
                                                    >
                                                        <svg 
                                                            class="w-3 h-3 transition-transform duration-200"
                                                            class:rotate-90={item.isExpanded}
                                                            fill="none" 
                                                            stroke="currentColor" 
                                                            viewBox="0 0 24 24"
                                                        >
                                                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                                                        </svg>
                                                    </button>
                                                {/if}
                                                <span class="font-medium text-sm">{item.name}</span>
                                            </div>
                                            <div class="flex items-center gap-2 mt-0.5">
                                                {#if isAdvancedMode && type === 'product'}
                                                    <span class="text-xs px-2 py-0.5 bg-gray-200 text-gray-700 rounded">
                                                        {#if item.type === 'vtm'}
                                                            VTM: {item.code}
                                                        {:else}
                                                            VMP: {item.code}
                                                        {/if}
                                                    </span>
                                                {:else if isAdvancedMode && type === 'ingredient'}
                                                    <span class="text-xs px-2 py-0.5 bg-gray-200 text-gray-700 rounded">
                                                        Ingredient: {item.code}
                                                    </span>
                                                {/if}
                                                {#if item.vmps?.length}
                                                    <span class="text-xs text-gray-500">
                                                        {item.vmps.length} product{item.vmps.length !== 1 ? 's' : ''}
                                                    </span>
                                                {/if}
                                            </div>
                                        </div>
                                        {#if selectedItems.includes(item.code)}
                                            <span class="text-xs font-medium text-oxford-600 ml-2">Selected (all products)</span>
                                        {/if}
                                    </div>
                                    
                                    {#if item.vmps && item.isExpanded}
                                        <ul class="border-t border-gray-200">
                                            {#each item.vmps as vmp}
                                                <li 
                                                    class="py-1.5 px-3 pl-8 cursor-pointer flex items-center justify-between relative transition-colors duration-150 ease-in-out hover:bg-gray-50"
                                                    class:bg-oxford-50={selectedItems.includes(vmp.code)}
                                                    class:opacity-50={selectedItems.includes(item.code)}
                                                    class:pointer-events-none={selectedItems.includes(item.code)}
                                                    on:click={() => handleSelect(vmp)}
                                                >
                                                    <div>
                                                        <span class="text-sm">{vmp.name}</span>
                                                        <div class="mt-0.5">
                                                            {#if isAdvancedMode && type === 'product'}
                                                                <span class="text-xs px-2 py-0.5 bg-gray-200 text-gray-700 rounded">
                                                                    VMP: {vmp.code}
                                                                </span>
                                                            {:else if isAdvancedMode && type === 'ingredient'}
                                                                <span class="text-xs px-2 py-0.5 bg-gray-200 text-gray-700 rounded">
                                                                    Ingredient: {vmp.code}
                                                                </span>
                                                            {/if}
                                                        </div>
                                                    </div>
                                                    {#if selectedItems.includes(vmp.code) && !selectedItems.includes(item.code)}
                                                        <span class="text-xs font-medium text-oxford-600 ml-2">Selected</span>
                                                    {/if}
                                                </li>
                                            {/each}
                                        </ul>
                                    {/if}
                                </li>
                            {:else}
                                <li 
                                    class="py-1.5 px-3 cursor-pointer relative transition-colors duration-150 ease-in-out hover:bg-gray-50"
                                    class:bg-oxford-50={selectedItems.includes(item.code)}
                                    on:click={() => handleSelect(item)}
                                >
                                    <div>
                                        <span class="text-sm">{item.name}</span>
                                        <div class="flex items-center gap-2 mt-0.5">
                                            {#if isAdvancedMode && type === 'product'}
                                                <span class="text-xs px-2 py-0.5 bg-gray-200 text-gray-700 rounded">
                                                    VMP: {item.code}
                                                </span>
                                            {:else if isAdvancedMode && type === 'ingredient'}
                                                <span class="text-xs px-2 py-0.5 bg-gray-200 text-gray-700 rounded">
                                                    Ingredient: {item.code}
                                                </span>
                                            {/if}
                                            {#if item.vmps?.length}
                                                <span class="text-xs text-gray-500">
                                                    {item.vmps.length} product{item.vmps.length !== 1 ? 's' : ''}
                                                </span>
                                            {/if}
                                        </div>
                                    </div>
                                    {#if selectedItems.includes(item.code)}
                                        <span class="text-xs font-medium text-oxford-600 mt-0.5 block">Selected</span>
                                    {/if}
                                </li>
                            {/if}
                        {/each}
                    </ul>
                    {#if showScrollTop}
                        <button
                            on:click={() => {
                                if (listContainer) {
                                    listContainer.scrollTo({ top: 0 });
                                }
                            }}
                            class="w-full p-2 bg-gray-100 border-t border-gray-200 flex items-center justify-center hover:bg-oxford-50 active:bg-oxford-100 cursor-pointer transition-colors duration-150 gap-2 rounded-b-md"
                        >
                            <span class="text-sm font-medium text-gray-700">
                                Click to scroll to top
                            </span>
                            <svg class="w-4 h-4 text-gray-700" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
                            </svg>
                        </button>
                    {/if}
                </div>
            {/if}
        </div>
        {#if selectedItems.length > 0}
            <div>
                <h3 class="font-semibold my-2 text-md text-gray-700">
                    Selected {isAdvancedMode ? (type === 'product' ? type : type.toUpperCase()) : 'product'} names:
                </h3>
                <ul class="border border-gray-200 rounded-md">
                    {#each selectedItems as item}
                        <li class="flex items-center justify-between px-2 py-1">
                            <span class="text-gray-800">
                                {#if type === 'atc' || type === 'ingredient'}
                                    {(selectedDisplayNames[item] || 
                                      filteredItems.find(i => i.code === item)?.name || 
                                      lastSearchResults.find(i => i.code === item)?.name || 
                                      item)}
                                {:else if isAdvancedMode}
                                    {selectedDisplayNames[item] || 
                                     filteredItems.find(i => i.code === item)?.display_name || 
                                     lastSearchResults.find(i => i.code === item)?.display_name || 
                                     item}
                                {:else}
                                    {(selectedDisplayNames[item] || 
                                      filteredItems.find(i => i.code === item)?.display_name || 
                                      lastSearchResults.find(i => i.code === item)?.display_name || 
                                      item).replace(/\([^)]*\)/g, '').trim()}
                                {/if}
                            </span>
                            <button 
                                on:click={() => handleRemove(item)}
                                class="px-2 py-1 text-sm text-white bg-red-600 hover:bg-red-700 rounded-md"
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
                        This selection will analyse {vmpCount} unique product{vmpCount !== 1 ? 's' : ''}.
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
</div>
