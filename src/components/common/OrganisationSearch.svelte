<svelte:options customElement={{
    tag: 'organisation-search',
    shadow: 'none'
  }} />

<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import '../../styles/styles.css';

    const dispatch = createEventDispatcher();

    export let source;
    export let overlayMode = false;
    export let disabled = false;
    export let maxItems = null;
    export let hideSelectAll = false;
    export let showTitle = true;

    $: placeholderText = disabled ? 
        'Selection disabled' :
        `Search and select ${
            $source.filterType === 'icb' ? 'Integrated Care Boards' : 
            $source.filterType === 'region' ? 'regions' : 
            'NHS Trusts'
        }...`;

    $: counterText = $source.filterType === 'icb' ? 'ICBs' :
                     $source.filterType === 'region' ? 'regions' :
                     'trusts';

    let isOpen = false;
    let searchTerm = '';
    let listContainer;
    let showScrollTop = false;

    $: items = $source.items || [];
    $: selectedItems = Array.from($source.selectedItems || []);
    $: availableItems = Array.from($source.availableItems || []);
    $: showOrganisationSelection = $source.usedOrganisationSelection;

    let selectedSectionCollapsed = false;
    let unselectedSectionCollapsed = false;
    let unselectableSectionCollapsed = true;

    $: if (!isOpen) {
        selectedSectionCollapsed = false;
        unselectedSectionCollapsed = false;
        unselectableSectionCollapsed = true;
    }

    $: groupedItems = (() => {
        const selected = [];
        const unselected = [];
        const unselectable = [];
        
        function countTotal(items) {
            return items.reduce((count, item) => count + 1 + (item.predecessors?.length || 0), 0);
        }
        
        filteredItems.forEach(item => {
            if (isItemSelected(item.name)) {
                selected.push(item);
            } else if (isItemAvailable(item.name)) {
                unselected.push(item);
            } else {
                unselectable.push(item);
            }
        });
        
        return { 
            selected, 
            unselected, 
            unselectable,
            selectedCount: countTotal(selected),
            unselectedCount: countTotal(unselected),
            unselectableCount: countTotal(unselectable)
        };
    })();
    
    function toggleSection(section) {
        if (section === 'selected') {
            selectedSectionCollapsed = !selectedSectionCollapsed;
        } else if (section === 'unselected') {
            unselectedSectionCollapsed = !unselectedSectionCollapsed;
        } else if (section === 'unselectable') {
            unselectableSectionCollapsed = !unselectableSectionCollapsed;
        }
    }

    function stripTrustSuffix(str) {
        if (str == null || typeof str !== 'string') return '';
        return str
            .replace(/\s+NHS\s+Foundation\s+Trust\s*$/i, '')
            .replace(/\s+NHS\s+Trust\s*$/i, '')
            .trim();
    }

    /** Words to skip when building initials (e.g. "Guy's and St Thomas" → "gst"). */
    const INITIALS_STOP_WORDS = new Set(['and']);

    /** Initial letters of each word (after normalising, excluding stop words), e.g. "Guy's and St Thomas" → "gst". */
    function getInitials(str) {
        const normalized = normalizeString(str);
        if (!normalized) return '';
        return normalized
            .split(/\s+/)
            .filter((w) => w && !INITIALS_STOP_WORDS.has(w))
            .map((w) => w[0])
            .join('');
    }

    function normalizeString(str) {
        if (str == null || typeof str !== 'string') return '';
        return str
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/['".,\/#!$%\^&\*;:{}=\-_`~()]/g, '')
            .replace(/\s+/g, ' ')
            .trim();
    }

    function tokenize(str) {
        const normalized = normalizeString(str);
        return normalized ? normalized.split(' ').filter(Boolean) : [];
    }

    /** True if search token matches an org token (exact or substring: "king" matches "kings"). */
    function tokenMatches(searchToken, orgTokens) {
        return orgTokens.some(
            ot => ot === searchToken || ot.includes(searchToken)
        );
    }

    /** Score 0..1: fraction of search tokens that appear in the org's searchable text. */
    function coverageScore(searchTokens, orgSearchableTokens) {
        if (searchTokens.length === 0) return 1;
        const matched = searchTokens.filter(t => tokenMatches(t, orgSearchableTokens));
        return matched.length / searchTokens.length;
    }

    $: hierarchicalItems = items.filter(item => {
        return !Array.from($source.predecessorMap.values()).flat().includes(item);
    }).map(item => ({
        name: item,
        predecessors: $source.predecessorMap.get(item) || []
    }));

    $: filteredItems = (() => {
        if (!searchTerm.trim()) return hierarchicalItems;

        const searchTokens = tokenize(searchTerm);
        if (searchTokens.length === 0) return hierarchicalItems;

        const itemsWithScore = hierarchicalItems.map(item => {
            const strippedName = stripTrustSuffix(item.name);
            const parts = [
                strippedName,
                getInitials(strippedName),
                source.getOrgCode?.(item.name),
                ...(item.predecessors || []).flatMap((p) => {
                    const strippedP = stripTrustSuffix(p);
                    return [strippedP, getInitials(strippedP), source.getOrgCode?.(p)];
                })
            ].filter(Boolean);
            const orgSearchableText = parts.join(' ');
            const orgTokens = [...new Set(tokenize(orgSearchableText))];
            const score = coverageScore(searchTokens, orgTokens);
            return { item, score };
        });

        const passing = itemsWithScore.filter(({ score }) => score > 0);

        return passing
            .sort((a, b) => {
                if (b.score !== a.score) return b.score - a.score;
                const x = a.item;
                const y = b.item;
                if (isItemSelected(x.name) !== isItemSelected(y.name)) {
                    return isItemSelected(x.name) ? -1 : 1;
                }
                const aSelectable = isItemAvailable(x.name);
                const bSelectable = isItemAvailable(y.name);
                if (!isItemSelected(x.name) && !isItemSelected(y.name) && aSelectable !== bSelectable) {
                    return aSelectable ? -1 : 1;
                }
                return x.name.localeCompare(y.name);
            })
            .map(({ item }) => item);
    })();

    $: isItemSelected = (item) => {
        const selected = $source.selectedItems || [];
        return Array.isArray(selected) ? (selected.includes(item) && isItemAvailable(item)) : false;
    };

    $: limitReached = maxItems && selectedItems.length >= maxItems;

    function toggleItem(item) {
        if (!isItemAvailable(item)) {
            return;
        }

        if (limitReached && !selectedItems.includes(item)) {
            dispatch('maxItemsReached', { maxItems });
            return;
        }

        const relatedOrgs = source.getRelatedOrgs(item);
        let newSelectedItems;

        if (selectedItems.includes(item)) {
            newSelectedItems = selectedItems.filter(i => !relatedOrgs.includes(i));
        } else {
            newSelectedItems = [...new Set([...selectedItems, ...relatedOrgs])];
        }
        
        source.updateSelection(newSelectedItems);
        dispatch('selectionChange', {
            selectedItems: newSelectedItems,
            source: 'search'
        });
        
        isOpen = true;
    }

    function deselectAll() {
        source.updateSelection([]);
        dispatch('selectionChange', {
            selectedItems: [],
            source: 'clearAll'
        });
    }

    function updateScrollButtonVisibility() {
        if (listContainer) {
            
            
            showScrollTop = listContainer.scrollHeight > listContainer.clientHeight && listContainer.scrollTop > 0;
        }
    }

    onMount(() => {
        const handleClickOutside = (event) => {
            const searchInput = document.querySelector('.dropdown input[type="text"]');
            const isSearchInputActive = searchInput === document.activeElement;
            
            if (isOpen && 
                !event.target.closest('.dropdown') && 
                !isSearchInputActive) {
                isOpen = false;
            }
        };

        document.addEventListener('click', handleClickOutside);

        setTimeout(() => {
            listContainer = document.querySelector('.dropdown .overflow-y-auto');
            if (listContainer) {
                listContainer.addEventListener('scroll', updateScrollButtonVisibility);
                updateScrollButtonVisibility();
            }
        }, 100);

        return () => {
            document.removeEventListener('click', handleClickOutside);
            if (listContainer) {
                listContainer.removeEventListener('scroll', updateScrollButtonVisibility);
            }
        };
    });

    $: if (filteredItems && listContainer) {
        setTimeout(updateScrollButtonVisibility, 50);
    }

    function isItemAvailable(item) {
        return source.isAvailable(item);
    }

    function selectAll() {
        const availableItems = Array.from($source.availableItems || []);
        
        const itemsToSelect = maxItems ? availableItems.slice(0, maxItems) : availableItems;
        
        dispatch('selectAll', {
            availableItems: availableItems,
            itemsToSelect: itemsToSelect
        });
        
        if (!hideSelectAll) {
            source.updateSelection(itemsToSelect);
            dispatch('selectionChange', {
                selectedItems: itemsToSelect,
                source: 'selectAll'
            });
        }
    }
</script>

<div class="dropdown relative w-full h-full flex flex-col">
    <div class="flex flex-col">
        <div class="flex flex-col">
            <div class="flex items-center justify-between mb-1">
                {#if showTitle}
                    <label class="text-sm font-medium {disabled ? 'text-gray-400' : 'text-gray-700'}">
                        Select {$source.filterType === 'icb' ? 'ICB' : 
                               $source.filterType === 'region' ? 'Region' : 
                               'NHS Trust'}
                        {#if maxItems}
                            <span class="text-xs text-gray-500 ml-1">(max {maxItems})</span>
                        {/if}
                    </label>
                {:else}
                    <span></span>
                {/if}
                <div class="flex items-center gap-1 text-sm">
                    {#if !hideSelectAll}
                        <button 
                            class="text-blue-600 hover:text-blue-800 font-medium {disabled ? 'opacity-50 cursor-not-allowed' : ''}" 
                            on:click={selectAll}
                            disabled={disabled}
                        >
                            Select All
                        </button>
                        <span class="text-gray-300">|</span>
                    {/if}
                    <button 
                        class="text-red-600 hover:text-red-800 font-medium {disabled ? 'opacity-50 cursor-not-allowed' : ''}" 
                        on:click={deselectAll}
                        disabled={disabled}
                    >
                        Clear All
                    </button>
                </div>
            </div>

            <div class="flex">
                <div class="relative flex-grow">
                    <input
                        type="text"
                        bind:value={searchTerm}
                        on:focus={() => !disabled && (isOpen = true)}
                        placeholder={placeholderText}
                        disabled={disabled}
                        class="w-full p-2 border border-gray-300 rounded-l-md
                               {isOpen ? 'rounded-bl-none' : ''} 
                               {disabled ? 'bg-gray-100 text-gray-500 cursor-not-allowed' : ''}"
                    />
                    <button
                        class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 
                               {disabled ? 'hidden' : ''}"
                        on:click|stopPropagation={() => searchTerm = ''}
                    >
                        {#if searchTerm}
                            <span>✕</span>
                        {/if}
                    </button>
                </div>

                <div class="flex items-center gap-2 bg-gray-50 px-3 border border-l-0 border-gray-300 
                            {isOpen ? 'rounded-tr-md' : 'rounded-r-md'} 
                            {disabled ? 'bg-gray-100' : ''} min-w-[70px]">
                    <div class="flex flex-col items-center text-xs text-gray-500 py-1 w-full">
                        <span class="font-medium">
                            {(() => {
                                const selectedCount = selectedItems.filter(item => isItemAvailable(item)).length;
                                const totalAvailable = Array.from($source.availableItems).length;
                                return `${selectedCount}/${totalAvailable}`;
                            })()}
                        </span>
                        <span>{counterText}</span>
                    </div>
                </div>
            </div>
        </div>

        {#if isOpen}
            <div class="absolute top-[calc(100%_-_1px)] left-0 right-0 bg-white border border-gray-300 
                        rounded-md rounded-t-none shadow-lg z-[996] flex flex-col max-h-72"
                 class:absolute={overlayMode}>
                <div class="flex-grow overflow-y-auto divide-y divide-gray-200"
                    bind:this={listContainer}
                    on:scroll={updateScrollButtonVisibility}>
                    
                    {#if groupedItems.selected.length > 0}
                        <div class="sticky top-0 z-10 bg-gray-50 border-b border-gray-200 shadow-sm">
                            <button 
                                class="w-full py-1.5 px-2 text-left flex justify-between items-center hover:bg-gray-100 transition-colors"
                                on:click={() => toggleSection('selected')}
                            >
                                <span class="text-sm font-medium text-gray-700">
                                    Selected {selectedItems.filter(item => isItemAvailable(item)).length === availableItems.length 
                                        ? "(all selected)" 
                                        : `(${groupedItems.selectedCount}/${groupedItems.selectedCount + groupedItems.unselectedCount})`}
                                </span>
                                <svg 
                                    class="w-3.5 h-3.5 text-gray-500 transform transition-transform duration-200 {selectedSectionCollapsed ? '' : 'rotate-180'}"
                                    fill="none" 
                                    stroke="currentColor" 
                                    viewBox="0 0 24 24"
                                >
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                                </svg>
                            </button>
                        </div>
                        
                        {#if !selectedSectionCollapsed}
                            {#each groupedItems.selected as item}
                                <div 
                                    role="button"
                                    tabindex="0"
                                    class="p-2 transition duration-150 ease-in-out relative cursor-pointer
                                          bg-oxford-100 text-oxford-500 hover:bg-oxford-200"
                                    on:click|stopPropagation={() => toggleItem(item.name)}
                                    on:keypress={(e) => e.key === 'Enter' && toggleItem(item.name)}
                                >
                                    <div class="flex items-center justify-between">
                                        <div class="flex items-center gap-2">
                                            <span>{source.getDisplayName(item.name)}</span>
                                        </div>
                                        <span class="ml-auto text-sm font-medium">Selected</span>
                                    </div>
                                    
                                    {#if item.predecessors.length > 0}
                                        {#each item.predecessors as predecessor}
                                            <div 
                                                role="button"
                                                tabindex="0"
                                                class="mt-1 pl-6 transition duration-150 ease-in-out relative text-sm
                                                      {!isItemAvailable(predecessor) ? 'text-gray-400 cursor-not-allowed' : ''}
                                                      {isItemSelected(predecessor) ? 'text-oxford-500' : ''}"
                                                on:click|stopPropagation={() => toggleItem(predecessor)}
                                                on:keypress|stopPropagation={(e) => e.key === 'Enter' && toggleItem(predecessor)}
                                            >
                                                <div class="flex items-center justify-between">
                                                    <div class="flex items-center">
                                                        <span class="mr-2">↳</span>
                                                        <span>{source.getDisplayName(predecessor)}</span>
                                                        <span class="mx-2 text-xs">(predecessor)</span>
                                                    </div>
                                                </div>
                                            </div>
                                        {/each}
                                    {/if}
                                </div>
                            {/each}
                        {/if}
                    {/if}
                    
                    {#if groupedItems.unselected.length > 0}
                        <div class="sticky top-0 z-10 bg-gray-50 border-b border-gray-200 shadow-sm">
                            <button 
                                class="w-full py-1.5 px-2 text-left flex justify-between items-center hover:bg-gray-100 transition-colors"
                                on:click={() => toggleSection('unselected')}
                            >
                                <span class="text-sm font-medium text-gray-700">
                                    Not selected ({groupedItems.unselectedCount}/{groupedItems.selectedCount + groupedItems.unselectedCount})
                                </span>
                                <svg 
                                    class="w-3.5 h-3.5 text-gray-500 transform transition-transform duration-200 {unselectedSectionCollapsed ? '' : 'rotate-180'}"
                                    fill="none" 
                                    stroke="currentColor" 
                                    viewBox="0 0 24 24"
                                >
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                                </svg>
                            </button>
                        </div>
                        
                        {#if !unselectedSectionCollapsed}
                            {#each groupedItems.unselected as item}
                                <div 
                                    role="button"
                                    tabindex="0"
                                    class="p-2 transition duration-150 ease-in-out relative 
                                           {limitReached ? 'cursor-not-allowed text-gray-400 bg-gray-50' : 'cursor-pointer hover:bg-gray-100'}"
                                    on:click|stopPropagation={() => toggleItem(item.name)}
                                    on:keypress={(e) => e.key === 'Enter' && toggleItem(item.name)}
                                    title={limitReached ? `Maximum of ${maxItems} NHS Trusts can be selected` : ''}
                                >
                                    <div class="flex items-center justify-between">
                                        <div class="flex items-center gap-2">
                                            <span>{source.getDisplayName(item.name)}</span>
                                        </div>
                                        {#if limitReached}
                                            <span class="text-xs text-gray-500">Max limit reached</span>
                                        {/if}
                                    </div>

                                    {#if item.predecessors.length > 0}
                                        {#each item.predecessors as predecessor}
                                            <div 
                                                role="button"
                                                tabindex="0"
                                                class="mt-1 pl-6 transition duration-150 ease-in-out relative text-sm
                                                      {!isItemAvailable(predecessor) || limitReached ? 'text-gray-400 cursor-not-allowed' : ''}
                                                      {isItemSelected(predecessor) ? 'text-oxford-500' : ''}"
                                                on:click|stopPropagation={() => toggleItem(predecessor)}
                                                on:keypress|stopPropagation={(e) => e.key === 'Enter' && toggleItem(predecessor)}
                                                title={limitReached ? `Maximum of ${maxItems} NHS Trusts can be selected` : ''}
                                            >
                                                <div class="flex items-center justify-between">
                                                    <div class="flex items-center">
                                                        <span class="mr-2">↳</span>
                                                        <span>{source.getDisplayName(predecessor)}</span>
                                                        <span class="mx-2 text-xs">(predecessor)</span>
                                                    </div>
                                                </div>
                                            </div>
                                        {/each}
                                    {/if}
                                </div>
                            {/each}
                        {/if}
                    {/if}
                    
                    {#if groupedItems.unselectable.length > 0}
                        <div class="sticky top-0 z-10 bg-gray-50 border-b border-gray-200 shadow-sm">
                            <button 
                                class="w-full py-1.5 px-2 text-left flex justify-between items-center hover:bg-gray-100 transition-colors"
                                on:click={() => toggleSection('unselectable')}
                            >
                                <span class="text-sm font-medium text-gray-700">
                                    Not included ({groupedItems.unselectableCount})
                                </span>
                                <svg 
                                    class="w-3.5 h-3.5 text-gray-500 transform transition-transform duration-200 {unselectableSectionCollapsed ? '' : 'rotate-180'}"
                                    fill="none" 
                                    stroke="currentColor" 
                                    viewBox="0 0 24 24"
                                >
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                                </svg>
                            </button>
                        </div>
                        
                        {#if !unselectableSectionCollapsed}
                            {#each groupedItems.unselectable as item}
                                <div class="p-2 transition duration-150 ease-in-out relative text-gray-400 cursor-not-allowed">
                                    <div class="flex items-center justify-between">
                                        <div class="flex items-center gap-2">
                                            <span>{source.getDisplayName(item.name)}</span>
                                        </div>
                                    </div>
       
                                    {#if item.predecessors.length > 0}
                                        {#each item.predecessors as predecessor}
                                            <div class="mt-1 pl-6 transition duration-150 ease-in-out relative text-sm text-gray-400 cursor-not-allowed">
                                                <div class="flex items-center justify-between">
                                                    <div class="flex items-center">
                                                        <span class="mr-2">↳</span>
                                                        <span>{source.getDisplayName(predecessor)}</span>
                                                        <span class="mx-2 text-xs">(predecessor)</span>
                                                    </div>
                                                </div>
                                            </div>
                                        {/each}
                                    {/if}
                                </div>
                            {/each}
                        {/if}
                    {/if}
                </div>
                
                <div class="py-2 px-3 border-t border-gray-200 flex bg-gray-50">
                    <div class="w-20"></div>
                    
                    <div class="flex-grow flex justify-center">
                        {#if showScrollTop}
                            <button
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
                    
                    <div class="w-20 flex justify-end">
                        <button
                            on:click={() => {
                                isOpen = false;
                            }}
                            class="inline-flex justify-center items-center px-3 py-1.5 bg-oxford-50 text-oxford-600 rounded-md hover:bg-oxford-100 transition-colors duration-200 font-medium text-sm border border-oxford-200"
                        >
                            Done
                        </button>
                    </div>
                </div>
            </div>
        {/if}
    </div>
</div>

