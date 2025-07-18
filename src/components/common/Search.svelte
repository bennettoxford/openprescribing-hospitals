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

    let selectedItemsData = {};
    let expandedItems = new Set();  // Track which VTMs are expanded

    $: {
        if (type === 'product') {
            filteredItems = filterItems($analyseOptions.productNames, searchTerm);
        } else if (type === 'ingredient') {
            filteredItems = filterItems($analyseOptions.ingredientNames, searchTerm);
        } else if (type === 'atc') {
            if (!searchTerm) {
                filteredItems = [];
            }
        }
    }

    function filterItems(items, term) {
        if (!items) return [];
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
                const response = await fetch(`/api/search-products/?type=${type}&term=${encodeURIComponent(searchTerm)}`);
                const data = await response.json();
                
                filteredItems = data.results.map(item => ({
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

    async function fetchVmpCount() {
        if (selectedItems.length === 0) {
            vmpCount = 0;
            return;
        }

        isCalculating = true;

        try {
            const response = await fetch('/api/vmp-count/', {
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
            if (data.vmp_details) {
                for (const [itemKey, vmps] of Object.entries(data.vmp_details)) {
                    if (selectedItemsData[itemKey]) {
                        selectedItemsData[itemKey].vmps = vmps;
                    } else {
                        const [code, itemType] = itemKey.split('|');
                        selectedItemsData[itemKey] = {
                            code,
                            type: itemType,
                            vmps
                        };
                    }
                }
            }
        } catch (error) {
            console.error('Error fetching VMP count:', error);
        } finally {
            isCalculating = false;
        }
    }

    function handleSelect(item) {
        const itemWithType = `${item.code}|${item.type}`;
        if (!isAdvancedMode) {
            if (selectedItems.includes(itemWithType)) {
                selectedItems = [];
                selectedItemsData = {};
            } else {
                selectedItems = [itemWithType];
                selectedItemsData[itemWithType] = item;
            }
        } else {
            if (item.type === 'vtm') {
                if (!selectedItems.includes(itemWithType)) {
                    const vmpCodes = item.vmps.map(vmp => `${vmp.code}|vmp`);
                    selectedItems = selectedItems.filter(i => !vmpCodes.includes(i));
                    selectedItems = [...selectedItems, itemWithType];
                    selectedItemsData[itemWithType] = item;
                } else {
                    selectedItems = selectedItems.filter(i => i !== itemWithType);
                    delete selectedItemsData[itemWithType];
                }
            } else if (item.type === 'atc' || item.type === 'ingredient') {
                if (!selectedItems.includes(itemWithType)) {
                    selectedItems = [...selectedItems, itemWithType];
                    selectedItemsData[itemWithType] = item;
                } else {
                    selectedItems = selectedItems.filter(i => i !== itemWithType);
                    delete selectedItemsData[itemWithType];
                }
            } else {
                const parentVtm = filteredItems.find(vtm => 
                    vtm.type === 'vtm' && vtm.vmps?.some(vmp => vmp.code === item.code)
                );
                if (!parentVtm || !selectedItems.includes(`${parentVtm.code}|vtm`)) {
                    if (!selectedItems.includes(itemWithType)) {
                        selectedItems = [...selectedItems, itemWithType];
                        selectedItemsData[itemWithType] = item;
                    } else {
                        selectedItems = selectedItems.filter(i => i !== itemWithType);
                        delete selectedItemsData[itemWithType];
                    }
                }
            }
        }
        
        dispatch('selectionChange', { items: selectedItems });
        fetchVmpCount();
        filteredItems = [];
        searchTerm = '';
    }

    function handleRemove(item) {
        selectedItems = selectedItems.filter(i => i !== item);
        dispatch('selectionChange', { items: selectedItems });
        fetchVmpCount();
    }

    function handleTypeChange(newType) {
        type = newType;
        searchTerm = '';
        filteredItems = [];
        dispatch('selectionChange', { items: selectedItems });
    }

    $: {
        if ($analyseOptions.selectedVMPs.length === 0 && selectedItems.length > 0) {
            selectedItems = [];
            searchTerm = '';
            vmpCount = 0;
            filteredItems = [];
            lastSearchResults = [];
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

    $: selectedItemsWithData = selectedItems.map(item => {
        const [code, type] = item.split('|');
        return {
            item,
            data: selectedItemsData[item] || 
                  lastSearchResults.find(i => i.code === code) || 
                  filteredItems.find(i => i.code === code)
        };
    });

    function toggleVTMExpand(itemCode) {
        if (expandedItems.has(itemCode)) {
            expandedItems.delete(itemCode);
        } else {
            expandedItems.add(itemCode);
        }
        expandedItems = expandedItems; // Trigger reactivity
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
            <button class="px-2 py-1 rounded {type === 'atc' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('atc')}>ATC Code</button>
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
                <div class="bg-white border border-gray-300 rounded-b-md shadow-lg p-3">
                    <p class="text-gray-600 text-sm">
                        Please type at least 3 characters to start searching
                    </p>
                </div>
            {:else if filteredItems.length > 0 || isLoading}
                <div class="bg-white">
                    <ul class="border border-gray-300 rounded-none border-t-0 max-h-[20vh] overflow-y-auto divide-y divide-gray-200 bg-white {!showScrollTop ? 'rounded-b-md' : ''}"
                        bind:this={listContainer}
                        on:scroll={updateScrollButtonVisibility}>
                        {#each filteredItems as item}
                            {#if item.type === 'vtm'}
                                <li class="group">
                                    <div 
                                        class="pt-2 pb-1 px-3 flex items-center justify-between relative transition-colors duration-150 ease-in-out"
                                        class:bg-oxford-50={selectedItems.includes(`${item.code}|vtm`)}
                                        class:cursor-pointer={isAdvancedMode}
                                        class:cursor-not-allowed={!isAdvancedMode}
                                        class:opacity-60={!isAdvancedMode}
                                        class:hover:bg-gray-50={isAdvancedMode}
                                        on:click={() => isAdvancedMode ? handleSelect(item) : null}
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
                                                {#if isAdvancedMode}
                                                    <span class="text-xs text-gray-500">
                                                        Code: {item.code}
                                                    </span>
                                                {/if}
                                                {#if item.vmps?.length}
                                                    <span class="text-xs text-gray-500">
                                                        {item.vmps.length} product{item.vmps.length !== 1 ? 's' : ''}
                                                    </span>
                                                {/if}
                                            </div>
                                        </div>
                                        {#if selectedItems.includes(`${item.code}|vtm`)}
                                            <span class="text-xs font-medium text-oxford-600 ml-2">Selected (all products)</span>
                                        {/if}
                                    </div>
                                    
                                    {#if item.vmps && item.isExpanded}
                                        <ul class="border-t border-gray-200">
                                            {#each item.vmps as vmp}
                                                <li 
                                                    class="py-1.5 px-3 pl-8 cursor-pointer flex items-center justify-between relative transition-colors duration-150 ease-in-out hover:bg-gray-50"
                                                    class:bg-oxford-50={selectedItems.includes(`${vmp.code}|vmp`)}
                                                    class:opacity-50={selectedItems.includes(`${item.code}|vtm`)}
                                                    class:pointer-events-none={selectedItems.includes(`${item.code}|vtm`)}
                                                    on:click={() => handleSelect(vmp)}
                                                >
                                                    <div>
                                                        <span class="text-sm">{vmp.name}</span>
                                                        <div class="mt-0.5">
                                                            {#if isAdvancedMode}
                                                                <span class="text-xs text-gray-500">
                                                                    Code: {vmp.code}
                                                                </span>
                                                            {/if}
                                                        </div>
                                                    </div>
                                                    {#if selectedItems.includes(`${vmp.code}|vmp`) && !selectedItems.includes(`${item.code}|vtm`)}
                                                        <span class="text-xs font-medium text-oxford-600 ml-2">Selected</span>
                                                    {/if}
                                                </li>
                                            {/each}
                                        </ul>
                                    {/if}
                                </li>
                            {:else if item.type === 'atc'}
                                <li class="group">
                                    <div 
                                        class="pt-2 pb-1 px-3 flex items-center justify-between relative transition-colors duration-150 ease-in-out"
                                        class:bg-oxford-50={selectedItems.includes(`${item.code}|atc`)}
                                        class:cursor-pointer={isAdvancedMode}
                                        class:cursor-not-allowed={!isAdvancedMode}
                                        class:opacity-60={!isAdvancedMode}
                                        class:hover:bg-gray-50={isAdvancedMode}
                                        on:click={() => isAdvancedMode ? handleSelect(item) : null}
                                    >
                                        <div class="flex-1">
                                            <div class="flex flex-col">
                                                <div class="flex items-start gap-3">
                                                    <span class="text-sm">{item.name}</span>
                                                    
                                                    {#if item.hierarchy_path && item.hierarchy_path.length > 1}
                                                        <div class="flex-shrink-0">
                                                            <button 
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
                                        {#if selectedItems.includes(`${item.code}|atc`)}
                                            <span class="text-xs font-medium text-oxford-600 ml-2">Selected</span>
                                        {/if}
                                    </div>
                                </li>
                            {:else}
                                <li 
                                    class="py-1.5 px-3 cursor-pointer relative transition-colors duration-150 ease-in-out hover:bg-gray-50"
                                    class:bg-oxford-50={selectedItems.includes(`${item.code}|${item.type}`)}
                                    class:opacity-50={!item.has_vmps && item.type !== 'atc'}
                                    class:pointer-events-none={!item.has_vmps && item.type !== 'atc'}
                                    on:click={() => (item.has_vmps || item.type === 'atc') && handleSelect(item)}
                                >
                                    <div>
                                        <span class="text-sm">{item.name}</span>
                                        {#if !item.has_vmps && item.type !== 'atc'}
                                            <span class="text-xs text-gray-400">(No products)</span>
                                        {/if}
                                        <div class="flex items-center gap-2 mt-0.5">
                                            {#if isAdvancedMode}
                                                <span class="text-xs text-gray-500">
                                                    Code: {item.code}
                                                </span>
                                            {/if}
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
                                    {#if selectedItems.includes(`${item.code}|${item.type}`)}
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
                <h3 class="font-medium text-sm text-gray-900 mb-3">
                    Selected items
                </h3>
                <ul class="bg-white border border-gray-200 rounded-lg shadow-sm divide-y divide-gray-100 max-h-[400px] overflow-y-auto">
                    {#each selectedItemsWithData as {item, data}}
                        {@const [code, type] = item.split('|')}
                        <li class="group">
                            <div class="p-3 hover:bg-gray-50 transition-colors duration-150">
                                <div class="grid grid-cols-[1fr,auto] gap-3">
                                    <div class="flex flex-col min-w-0">
                                        <span class="text-sm text-gray-700 break-words">
                                            {(selectedDisplayNames[item] || 
                                              data?.display_name || 
                                              item).replace(/\([^)]*\)/g, '').trim()}
                                        </span>
                                        {#if isAdvancedMode}
                                        <span class="text-xs text-gray-500 mt-1">
                                            Code: {code}
                                        </span>
                                        {/if}
                                        {#if (type === 'vtm' || type === 'ingredient' || type === 'atc') && isAdvancedMode && data?.vmps?.length > 0}
                                            <button 
                                                on:click={() => toggleVTMExpand(item)}
                                                class="flex items-center gap-1.5 mt-2 text-sm text-gray-500 hover:text-gray-700 transition-colors duration-150"
                                            >
                                                <svg 
                                                    class="w-3.5 h-3.5 transform transition-transform duration-150 {expandedItems.has(item) ? 'rotate-90' : ''}" 
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
                                        {#if isAdvancedMode}
                                            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium
                                                {type === 'ingredient' ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 
                                                 type === 'atc' ? 'bg-purple-50 text-purple-700 border border-purple-200' :
                                                 'bg-blue-50 text-blue-700 border border-blue-200'}">
                                                {type === 'ingredient' ? 'Ingredient' : 
                                                 type === 'atc' ? 'ATC' : 
                                                 'Product'}
                                            </span>
                                        {/if}
                                        <button 
                                            on:click={() => handleRemove(item)}
                                            class="px-2 py-1 text-xs font-medium text-white bg-red-600 
                                                   hover:bg-red-700 rounded-md transition-colors duration-150"
                                        >
                                            Remove
                                        </button>
                                    </div>
                                </div>
                            </div>
                            {#if (type === 'vtm' || type === 'ingredient' || type === 'atc') && data?.vmps && expandedItems.has(item)}
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
                {#if isAdvancedMode}
                    <p class="mt-3 text-sm text-gray-600">
                        {#if isCalculating}
                            <span class="flex items-center gap-2">
                                <svg class="animate-spin h-4 w-4 text-gray-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                </svg>
                                Calculating number of individual products...
                            </span>
                        {:else if vmpCount > 0}
                            This selection will analyse {vmpCount} unique product{vmpCount !== 1 ? 's' : ''}.
                            {#if vmpCount > 100}
                                <span class="block mt-1 text-amber-600 font-medium">
                                    Warning: This is a large number of products. The analysis may take a long time. 
                                    Consider making your selection more specific.
                                </span>
                            {/if}
                        {/if}
                    </p>
                {/if}
            </div>
        {/if}
    </div>
</div>
