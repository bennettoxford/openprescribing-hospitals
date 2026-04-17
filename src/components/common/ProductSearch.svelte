<svelte:options customElement={{
    tag: 'product-search-box',
    shadow: 'none'
  }} />
<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import '../../styles/styles.css';
    import { analyseOptions } from '../../stores/analyseOptionsStore';
    const dispatch = createEventDispatcher();

    export let type = "product";
    export let maxVmpCount = null;

    let searchTerm = '';
    let filteredItems = [];
    let selectedItems = [];
    let vmpCount = 0;

    $: effectiveMaxVmpCount = Number(maxVmpCount) > 0 ? Number(maxVmpCount) : null;
    $: overVmpLimit = effectiveMaxVmpCount !== null && vmpCount > effectiveMaxVmpCount;
    $: dispatch('vmpCountChange', { overLimit: overVmpLimit });

    let searchTimeout;
    let isLoading = false;
    let lastSearchResults = [];
    let searchBoxRef;
    let isOpen = false;

    let listContainer;
    let showScrollTop = false;

    let selectedItemsData = [];
    let expandedItems = new Set();  // Track which VTMs are expanded

    $: if (!searchTerm && filteredItems.length > 0) {
        filteredItems = [];
    }

    async function handleInput() {
        isOpen = true;
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
                const response = await fetch(`/api/search-products/?type=${type}&term=${encodeURIComponent(searchTerm)}`);
                const data = await response.json();
                if (!response.ok) {
                    filteredItems = [];
                    lastSearchResults = [];
                    return;
                }
                const results = Array.isArray(data.results) ? data.results : [];
                filteredItems = results.map(item => ({
                    ...item,
                    isExpanded: item.type === 'vtm' ? true : false,
                    has_vmps: true
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

    function calculateVmpCount() {
        if (selectedItems.length === 0) {
            vmpCount = 0;
            return;
        }

        const vmpCodes = new Set();

        for (const item of selectedItems) {
            const data = findItemInArray(selectedItemsData, item) ||
                        lastSearchResults.find(i => i.code === item.code) ||
                        filteredItems.find(i => i.code === item.code);

            if (data) {
                if (item.type === 'vmp') {
                    vmpCodes.add(item.code);
                } else if (data.vmps) {
                    for (const vmp of data.vmps) {
                        vmpCodes.add(vmp.code);
                    }
                }
            }
        }

        vmpCount = vmpCodes.size;
    }

    function findItemInArray(items, targetItem) {
        return items.find(item => item.code === targetItem.code && item.type === targetItem.type);
    }
    
    function isItemSelected(item, selectedItems) {
        return selectedItems.some(selected => selected.code === item.code && selected.type === item.type);
    }
    
    function removeItemFromSelected(item, selectedItems) {
        return selectedItems.filter(selected => !(selected.code === item.code && selected.type === item.type));
    }

    function handleSelect(item) {
        if (item.type === 'vtm') {
            if (!isItemSelected(item, selectedItems)) {
                const vmpCodes = item.vmps.map(vmp => ({code: vmp.code, type: 'vmp'}));
                selectedItems = selectedItems.filter(selected => !vmpCodes.some(vmp => vmp.code === selected.code && vmp.type === selected.type));
                selectedItems = [...selectedItems, item];
                selectedItemsData = [...selectedItemsData, item];
            } else {
                selectedItems = removeItemFromSelected(item, selectedItems);
                selectedItemsData = selectedItemsData.filter(data => !(data.code === item.code && data.type === item.type));
            }
        } else if (item.type === 'atc' || item.type === 'ingredient') {
            if (!isItemSelected(item, selectedItems)) {
                selectedItems = [...selectedItems, item];
                selectedItemsData = [...selectedItemsData, item];
            } else {
                selectedItems = removeItemFromSelected(item, selectedItems);
                selectedItemsData = selectedItemsData.filter(data => !(data.code === item.code && data.type === item.type));
            }
        } else {
            const parentVtm = filteredItems.find(vtm =>
                vtm.type === 'vtm' && vtm.vmps?.some(vmp => vmp.code === item.code)
            );
            const parentVtmSelected = parentVtm && isItemSelected(parentVtm, selectedItems);
            if (!parentVtm || !parentVtmSelected) {
                if (!isItemSelected(item, selectedItems)) {
                    selectedItems = [...selectedItems, item];
                    selectedItemsData = [...selectedItemsData, item];
                } else {
                    selectedItems = removeItemFromSelected(item, selectedItems);
                    selectedItemsData = selectedItemsData.filter(data => !(data.code === item.code && data.type === item.type));
                }
            }
        }

        
        dispatch('selectionChange', { items: selectedItems });
        calculateVmpCount();
        isOpen = true;
    }

    function handleRemove(item) {
        selectedItems = removeItemFromSelected(item, selectedItems);
        selectedItemsData = selectedItemsData.filter(data => !(data.code === item.code && data.type === item.type));
        dispatch('selectionChange', { items: selectedItems });
        calculateVmpCount();
    }

    function resetSearchState() {
        clearTimeout(searchTimeout);
        searchTerm = '';
        filteredItems = [];
        lastSearchResults = [];
        isLoading = false;
    }

    function deselectAll() {
        selectedItems = [];
        selectedItemsData = [];
        expandedItems = new Set();
        vmpCount = 0;
        dispatch('selectionChange', { items: selectedItems });
    }

    function doneCloseSearch() {
        isOpen = false;
        resetSearchState();
    }

    function handleTypeChange(newType) {
        type = newType;
        resetSearchState();
    }

    $: {
        const storeSelections = $analyseOptions.selectedVMPs || [];

        if (storeSelections.length === 0 && selectedItems.length > 0) {
            selectedItems = [];
            vmpCount = 0;
            selectedItemsData = [];
            isOpen = false;
            resetSearchState();
        } else if (storeSelections.length > 0) {
            const hasDifferences =
                storeSelections.length !== selectedItems.length ||
                storeSelections.some((item, index) => {
                    const current = selectedItems[index];
                    return !current || current.code !== item.code || current.type !== item.type;
                });

            if (hasDifferences) {
                selectedItems = storeSelections.map(item => ({ ...item }));
                selectedItemsData = storeSelections.map(item => ({ ...item }));
                calculateVmpCount();
            }
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
        const handleClickOutside = (event) => {
            if (searchBoxRef && !searchBoxRef.contains(event.target) && isOpen) {
                isOpen = false;
            }
        };
        document.addEventListener('click', handleClickOutside);

        return () => {
            document.removeEventListener('click', handleClickOutside);
        };
    });

    $: if (filteredItems && listContainer) {
        setTimeout(updateScrollButtonVisibility, 50);
    }

    $: selectedItemsWithData = selectedItems.map(item => {
        const data = findItemInArray(selectedItemsData, item) || 
                     lastSearchResults.find(i => i.code === item.code) || 
                     filteredItems.find(i => i.code === item.code);
        return {
            item,
            data
        };
    });

    function vtmKey(item) {
        return `${item.code}:${item.type}`;
    }

    function toggleVTMExpand(item) {
        const key = vtmKey(item);
        if (expandedItems.has(key)) {
            expandedItems.delete(key);
        } else {
            expandedItems.add(key);
        }
        expandedItems = expandedItems;
    }

    $: placeholder = type === 'product' ? "Search by product name or dm+d code..." :
                     type === 'ingredient' ? "Search by ingredient name or dm+d code..." :
                     type === 'atc' ? "Search by ATC level name or ATC code..." :
                     "Search by product name or code...";

    $: showSearchHint = isOpen && searchTerm && searchTerm.length < 3;
    $: showResultList = isOpen && (filteredItems.length > 0 || isLoading);
    $: resultsPanelOpen =
        showSearchHint || showResultList;
</script>

<div 
    class="w-full search-box relative"
    bind:this={searchBoxRef}
>

    <div class="flex space-x-2 mb-2 pointer-events-auto">
        <button type="button" class="px-2 py-1 rounded {type === 'product' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('product')}>Product</button>
        <button type="button" class="px-2 py-1 rounded {type === 'ingredient' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('ingredient')}>Ingredient</button>
        <button type="button" class="px-2 py-1 rounded {type === 'atc' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('atc')}>ATC Code</button>
    </div>

    <div class="grid gap-4">
        <div class="relative pointer-events-auto">
            <div class="relative">
                <input
                    type="text"
                    {placeholder}
                    bind:value={searchTerm}
                    on:input={handleInput}
                    on:focus={() => {
                        isOpen = true;
                        if (searchTerm && lastSearchResults.length > 0) {
                            filteredItems = lastSearchResults;
                        }
                    }}
                    on:keydown={(e) => {
                        if (e.key === 'Escape') {
                            e.preventDefault();
                            if (isOpen) {
                                isOpen = false;
                                resetSearchState();
                            }
                        }
                    }}
                    class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oxford-500 pr-8 placeholder:text-sm
                           {resultsPanelOpen ? 'rounded-b-none' : ''}"
                />
                {#if isLoading}
                    <div class="absolute right-8 top-1/2 -translate-y-1/2 flex items-center z-20">
                        <div class="animate-spin h-4 w-4 border-2 border-oxford-500 rounded-full border-t-transparent"></div>
                    </div>
                {/if}
                {#if searchTerm}
                    <button
                        type="button"
                        class="absolute right-2 top-1/2 -translate-y-1/2 flex items-center justify-center text-gray-400 hover:text-gray-600 w-5 z-20"
                        on:click|stopPropagation={resetSearchState}
                    >
                        ✕
                    </button>
                {/if}
            </div>

            {#if resultsPanelOpen}
                <div class="bg-white border border-gray-300 rounded-b-md shadow-lg overflow-hidden">
                    {#if showSearchHint}
                        <div class="p-3">
                            <p class="text-gray-600 text-sm">
                                Please type at least 3 characters to start searching
                            </p>
                        </div>
                    {:else}
                        <ul class="max-h-[50vh] overflow-y-auto divide-y divide-gray-200 bg-white"
                            bind:this={listContainer}
                            on:scroll={updateScrollButtonVisibility}>
                            {#each filteredItems as item}
                                {#if item.type === 'vtm'}
                                    <li class="group">
                                        <div 
                                            class="pt-2 pb-1 px-3 flex items-center justify-between relative transition-colors duration-150 ease-in-out cursor-pointer hover:bg-gray-50"
                                            class:bg-oxford-50={isItemSelected(item, selectedItems)}
                                            role="button"
                                            tabindex="0"
                                            on:click={() => handleSelect(item)}
                                            on:keydown={(event) => {
                                                if (event.key === 'Enter' || event.key === ' ') {
                                                    event.preventDefault();
                                                    handleSelect(item);
                                                }
                                            }}
                                        >
                                            <div class="flex-1">
                                                <div class="flex items-center gap-2">
                                                    {#if item.vmps?.length > 0}
                                                        <button
                                                            type="button"
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
                                                    {#if item.vmps?.length}
                                                        <span class="text-xs text-gray-500">
                                                            {item.vmps.length} product{item.vmps.length !== 1 ? 's' : ''}
                                                        </span>
                                                    {/if}
                                                </div>
                                            </div>
                                            {#if isItemSelected(item, selectedItems)}
                                                <span class="text-xs font-medium text-oxford-600 ml-2">Selected (all products)</span>
                                            {/if}
                                        </div>
                                        
                                        {#if item.vmps && item.isExpanded}
                                            <ul class="border-t border-gray-200 divide-y divide-gray-100">
                                                {#each item.vmps as vmp}
                                                    <li>
                                                        <button
                                                            type="button"
                                                            class="w-full py-1 pl-5 pr-3 flex items-center justify-between relative transition-colors duration-150 ease-in-out hover:bg-gray-50 text-left"
                                                            class:bg-oxford-50={isItemSelected({code: vmp.code, type: 'vmp'}, selectedItems)}
                                                            class:opacity-50={isItemSelected(item, selectedItems)}
                                                            class:pointer-events-none={isItemSelected(item, selectedItems)}
                                                            on:click={() => handleSelect(vmp)}
                                                        >
                                                            <div>
                                                                <span class="text-sm">{vmp.name}</span>
                                                            </div>
                                                            {#if isItemSelected({code: vmp.code, type: 'vmp'}, selectedItems) && !isItemSelected(item, selectedItems)}
                                                                <span class="text-xs font-medium text-oxford-600 ml-2">Selected</span>
                                                            {/if}
                                                        </button>
                                                    </li>
                                                {/each}
                                            </ul>
                                        {/if}
                                    </li>
                                {:else if item.type === 'atc'}
                                    <li class="group">
                                        <div 
                                            class="pt-2 pb-1 px-3 flex items-center justify-between relative transition-colors duration-150 ease-in-out cursor-pointer hover:bg-gray-50"
                                            class:bg-oxford-50={isItemSelected(item, selectedItems)}
                                            role="button"
                                            tabindex="0"
                                            on:click={() => handleSelect(item)}
                                            on:keydown={(event) => {
                                                if (event.key === 'Enter' || event.key === ' ') {
                                                    event.preventDefault();
                                                    handleSelect(item);
                                                }
                                            }}
                                        >
                                            <div class="flex-1">
                                                <div class="flex flex-col">
                                                    <div class="flex items-start gap-3">
                                                        <span class="text-sm">{item.name}</span>
                                                        
                                                        {#if item.hierarchy_path && item.hierarchy_path.length > 1}
                                                            <div class="flex-shrink-0">
                                                                <button
                                                                    type="button"
                                                                    class="text-xs text-gray-600 hover:text-gray-800 flex items-center gap-1"
                                                                    on:click|stopPropagation={() => {
                                                                        item.showHierarchy = !item.showHierarchy;
                                                                        filteredItems = [...filteredItems];
                                                                    }}
                                                                >
                                                                    <svg 
                                                                        class="w-3 h-3 transition-transform {item.showHierarchy ? 'rotate-90' : ''}" 
                                                                        fill="none" 
                                                                        stroke="currentColor" 
                                                                        viewBox="0 0 24 24"
                                                                    >
                                                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
                                                                    </svg>
                                                                    Show hierarchy
                                                                </button>
                                                            </div>
                                                        {/if}
                                                    </div>
                                                    
                                                    {#if item.hierarchy_path && item.hierarchy_path.length > 1 && item.showHierarchy}
                                                        <div class="mt-2">
                                                            {#each item.hierarchy_path as pathItem, index}
                                                                <div class="flex items-start text-xs text-gray-600 mb-1" style="padding-left: {index * 16}px;">
                                                                    <div class="w-3 h-3 border-l border-b border-gray-400 mr-2 flex-shrink-0 mt-1"></div>
                                                                    <span title={pathItem} class="leading-relaxed break-words {index === item.hierarchy_path.length - 1 ? 'font-bold' : ''}">{pathItem}</span>
                                                                </div>
                                                            {/each}
                                                        </div>
                                                    {/if}
                                                </div>
                                                <div class="flex items-center gap-2 mt-0.5">
                                                    <span class="text-xs text-gray-500">
                                                        Code: {item.code}
                                                    </span>
                                                    {#if item.vmp_count !== undefined}
                                                        <span class="text-xs text-gray-500">
                                                            {item.vmp_count} product{item.vmp_count !== 1 ? 's' : ''}
                                                        </span>
                                                    {/if}
                                                </div>
                                            </div>
                                            {#if isItemSelected(item, selectedItems)}
                                                <span class="text-xs font-medium text-oxford-600 ml-2">Selected</span>
                                            {/if}
                                        </div>
                                    </li>
                                {:else}
                                    <li>
                                        <button
                                            type="button"
                                            class="w-full py-1.5 px-3 relative transition-colors duration-150 ease-in-out hover:bg-gray-50 text-left"
                                            class:bg-oxford-50={isItemSelected(item, selectedItems)}
                                            class:opacity-50={!item.has_vmps && item.type !== 'atc'}
                                            disabled={!item.has_vmps && item.type !== 'atc'}
                                            on:click={() => handleSelect(item)}
                                        >
                                            <div>
                                                <span class="text-sm">{item.name}</span>
                                                {#if !item.has_vmps && item.type !== 'atc'}
                                                    <span class="text-xs text-gray-400">(No products)</span>
                                                {/if}
                                                <div class="flex items-center gap-2 mt-0.5">
                                                    
                                                    {#if item.vmp_count !== undefined}
                                                        <span class="text-xs text-gray-500">
                                                            {item.vmp_count} product{item.vmp_count !== 1 ? 's' : ''}
                                                        </span>
                                                    {:else if item.vmps?.length}
                                                        <span class="text-xs text-gray-500">
                                                            {item.vmps.length} product{item.vmps.length !== 1 ? 's' : ''}
                                                        </span>
                                                    {/if}
                                                </div>
                                            </div>
                                            {#if isItemSelected(item, selectedItems)}
                                                <span class="text-xs font-medium text-oxford-600 mt-0.5 block">Selected</span>
                                            {/if}
                                        </button>
                                    </li>
                                {/if}
                            {/each}
                        </ul>
                    {/if}
                    <div class="py-2 px-3 border-t border-gray-200 flex items-center gap-2 bg-gray-50">
                        <div class="w-20 shrink-0"></div>
                        <div class="flex-grow flex justify-center min-h-[2.25rem] items-center">
                            {#if showResultList && showScrollTop}
                                <button
                                    type="button"
                                    on:click={() => {
                                        if (listContainer) {
                                            listContainer.scrollTo({ top: 0 });
                                        }
                                    }}
                                    class="inline-flex items-center gap-1 text-sm font-medium text-gray-600 hover:text-oxford-600 transition-colors"
                                >
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
                                    </svg>
                                    Scroll to top
                                </button>
                            {/if}
                        </div>
                        <div class="w-20 shrink-0 flex justify-end">
                            <button
                                type="button"
                                on:click={doneCloseSearch}
                                class="inline-flex justify-center items-center px-3 py-1.5 bg-oxford-50 text-oxford-600 rounded-md hover:bg-oxford-100 transition-colors duration-200 font-medium text-sm border border-oxford-200"
                            >
                                Done
                            </button>
                        </div>
                    </div>
                </div>
            {/if}
        </div>
        {#if selectedItems.length > 0}
            <div>
                <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
                    <h3 class="font-medium text-sm text-gray-900">
                        Selected items
                    </h3>
                    <button
                        type="button"
                        class="text-red-600 hover:text-red-800 font-medium text-xs py-1.5 px-2 sm:py-0.5 sm:px-1.5 rounded hover:bg-red-50 transition-colors shrink-0"
                        on:click={deselectAll}
                    >
                        Deselect all
                    </button>
                </div>
                <ul class="bg-white border border-gray-200 rounded-lg shadow-sm divide-y divide-gray-100 max-h-[400px] overflow-y-auto">
                    {#each selectedItemsWithData as {item, data}}
                        {@const code = item.code}
                        {@const type = item.type}
                        <li class="group">
                            <div class="p-3 hover:bg-gray-50 transition-colors duration-150">
                                <div class="grid grid-cols-[1fr,auto] gap-3">
                                    <div class="flex flex-col min-w-0">
                                        <span class="text-sm text-gray-700 break-words">
                                            {data?.name || `${item.code} (${item.type})`}
                                        </span>
                                        {#if type === 'atc' && data?.code}
                                            <span class="text-xs text-gray-500 mt-1">
                                                ATC Code: {data.code}
                                            </span>
                                        {/if}
                                       
                                        {#if (type === 'vtm' || type === 'ingredient' || type === 'atc') && data?.vmps?.length > 0}
                                            <button 
                                                type="button"
                                                on:click={() => toggleVTMExpand(item)}
                                                class="flex items-center gap-1.5 mt-2 text-sm text-gray-500 hover:text-gray-700 transition-colors duration-150"
                                            >
                                                <svg 
                                                    class="w-3.5 h-3.5 transform transition-transform duration-150 {expandedItems.has(vtmKey(item)) ? 'rotate-90' : ''}" 
                                                    fill="none" 
                                                    stroke="currentColor" 
                                                    viewBox="0 0 24 24"
                                                >
                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                                                </svg>
                                                <span class="text-xs">
                                                    {data?.vmps?.length || 0} product{data?.vmps?.length !== 1 ? 's' : ''} included
                                                </span>
                                            </button>
                                        {/if}
                                    </div>
                                    <div class="flex flex-col items-center gap-2">
                                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium
                                                {type === 'ingredient' ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 
                                                 type === 'atc' ? 'bg-purple-50 text-purple-700 border border-purple-200' :
                                                 'bg-blue-50 text-blue-700 border border-blue-200'}">
                                                {type === 'ingredient' ? 'Ingredient' : 
                                                 type === 'atc' ? 'ATC' : 
                                                 'Product'}
                                            </span>
                                        <button 
                                            type="button"
                                            on:click={() => handleRemove(item)}
                                            class="px-2 py-1 text-xs font-medium text-white bg-red-600 
                                                   hover:bg-red-700 rounded-md transition-colors duration-150"
                                        >
                                            Remove
                                        </button>
                                    </div>
                                </div>
                            </div>
                            {#if (type === 'vtm' || type === 'ingredient' || type === 'atc') && data?.vmps && expandedItems.has(vtmKey(item))}
                                <ul class="bg-gray-50 border-t border-gray-100 py-2 px-3">
                                    {#each data.vmps as vmp}
                                        <li class="flex items-center py-1">
                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                            <span class="text-sm text-gray-600">
                                                {vmp.name}
                                            </span>
                                        </li>
                                    {/each}
                                </ul>
                            {/if}
                        </li>
                    {/each}
                </ul>
                    <p class="mt-3 text-sm text-gray-600">
                        {#if vmpCount > 0}
                            This selection will analyse {vmpCount} unique product{vmpCount !== 1 ? 's' : ''}.
                        {/if}
                        {#if overVmpLimit}
                            <span class="block mt-1 text-red-600 font-medium">
                                This is over the limit of {effectiveMaxVmpCount} unique products. Please narrow your selection before running the analysis, or <a href="/contact/" target="_blank" class="underline hover:text-red-800">contact us</a> if you need to analyse a larger selection.
                            </span>
                        {/if}
                    </p>
            </div>
        {/if}
    </div>
</div>
