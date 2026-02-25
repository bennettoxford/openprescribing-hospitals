<svelte:options customElement={{
    tag: 'organisation-search-filtered',
    shadow: 'none'
  }} />

<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import { get } from 'svelte/store';
    import '../../styles/styles.css';

    const dispatch = createEventDispatcher();

    export let source;
    export let overlayMode = false;
    export let disabled = false;
    export let maxItems = null;
    export let hideSelectAll = false;
    export let showTitle = true;

    export let filterAutoSelectsAll = true;
    export let filterResetKey = undefined;

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
    let searchInputRef;
    let dropdownRef;
    let showScrollTop = false;
    $: items = $source.items || [];
    $: selectedItems = Array.from($source.selectedItems || []);
    $: availableItems = Array.from($source.availableItems || []);

    let selectedSectionCollapsed = false;
    let unselectedSectionCollapsed = false;
    let unselectableSectionCollapsed = true;

    $: if (!isOpen) {
        selectedSectionCollapsed = false;
        unselectedSectionCollapsed = false;
        unselectableSectionCollapsed = true;
    }

    $: groupedItems = (() => {
        availableItems;
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

    function stripNhsPrefix(str) {
        if (str == null || typeof str !== 'string') return '';
        return str.replace(/^NHS\s+/i, '').trim();
    }

    const INITIALS_STOP_WORDS = new Set(['and']);

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

    function tokenMatches(searchToken, orgTokens) {
        return orgTokens.some(
            ot => ot === searchToken || ot.includes(searchToken)
        );
    }

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
            const isInsideDropdown = dropdownRef && dropdownRef.contains(event.target);
            const isSearchInputActive = searchInputRef === document.activeElement;
            if (!isInsideDropdown && !isSearchInputActive) {
                if (filterDropdownOpen) filterDropdownOpen = false;
                if (isOpen) isOpen = false;
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

    $: trustTypes = ($source && typeof source.getTrustTypes === 'function' ? source.getTrustTypes() : []) || [];
    $: regionsHierarchy = ($source && typeof source.getRegionsHierarchy === 'function' ? source.getRegionsHierarchy() : []) || [];
    const ACUTE_PREFIX = 'Acute -';
    $: acuteTypes = trustTypes.filter((t) => t.startsWith(ACUTE_PREFIX));
    $: otherTypes = trustTypes.filter((t) => !t.startsWith(ACUTE_PREFIX));
    let filterDropdownOpen = false;
    let selectedTrustTypes = new Set();
    let selectedRegions = new Set();
    let selectedICBs = new Set();
    let expandedRegions = new Set();
    let expandedAcute = false;
    let acuteParentCheckbox;
    function toggleTrustType(type) {
        const next = new Set(selectedTrustTypes);
        if (next.has(type)) next.delete(type); else next.add(type);
        selectedTrustTypes = next;
        applyTrustTypeSelection();
    }
    function toggleAcuteParent() {
        const allAcuteSelected = acuteTypes.length > 0 && acuteTypes.every((t) => selectedTrustTypes.has(t));
        const next = new Set(selectedTrustTypes);
        if (allAcuteSelected) acuteTypes.forEach((t) => next.delete(t));
        else acuteTypes.forEach((t) => next.add(t));
        selectedTrustTypes = next;
        applyTrustTypeSelection();
    }
    $: allAcuteSelected = acuteTypes.length > 0 && acuteTypes.every((t) => selectedTrustTypes.has(t));
    $: someAcuteSelected = acuteTypes.length > 0 && acuteTypes.some((t) => selectedTrustTypes.has(t));
    $: acuteIndeterminate = someAcuteSelected && !allAcuteSelected;
    $: if (acuteParentCheckbox) acuteParentCheckbox.indeterminate = acuteIndeterminate;
    $: hasSelection = selectedItems.filter((item) => isItemAvailable(item)).length > 0;
    function clearAllFilters() {
        selectedTrustTypes = new Set();
        selectedRegions = new Set();
        selectedICBs = new Set();
        expandedRegions = new Set();
        applyTrustTypeSelection();
    }

    let _lastFilterResetKey;
    $: if (filterResetKey !== undefined && filterResetKey !== _lastFilterResetKey) {
        _lastFilterResetKey = filterResetKey;
        clearAllFilters();
    }
    function applyFilterAndSelection(itemList, setAvailableItemsFn) {
        if (typeof setAvailableItemsFn === 'function') setAvailableItemsFn(itemList);
        const currentSelected = Array.from(get(source).selectedItems || []);
        const itemSet = new Set(itemList);
        const newSelection = filterAutoSelectsAll
            ? itemList
            : currentSelected.filter((item) => itemSet.has(item));
        const currentSet = new Set(currentSelected);
        const selectionChanged = currentSet.size !== newSelection.length || newSelection.some((item) => !currentSet.has(item));
        if (selectionChanged) {
            source.updateSelection(newSelection);
            dispatch('selectionChange', { selectedItems: newSelection, source: 'filter' });
        }
    }

    function applyTrustTypeSelection() {
        const store = get(source);
        if (store.filterType === 'trust') {
            const allItems = Array.from(store.items || []);
            let orgList;
            if (selectedTrustTypes.size > 0 && typeof source.getTrustType === 'function') {
                orgList = allItems.filter((name) => selectedTrustTypes.has(source.getTrustType(name)));
            } else {
                orgList = allItems;
            }
            if (selectedRegions.size > 0 || selectedICBs.size > 0) {
                const byRegionIcb = typeof source.getOrgsByRegionsOrICBs === 'function' ? source.getOrgsByRegionsOrICBs(selectedRegions, selectedICBs) : [];
                orgList = orgList.filter((name) => new Set(byRegionIcb).has(name));
            }
            const hasFilters = selectedTrustTypes.size > 0 || selectedRegions.size > 0 || selectedICBs.size > 0;
            if (typeof source.setFiltersApplied === 'function') source.setFiltersApplied(hasFilters);
            applyFilterAndSelection(orgList, source.setAvailableItems);
        } else if (store.filterType === 'icb') {
            const icbList = selectedRegions.size > 0 && typeof source.getICBsByRegions === 'function' ? source.getICBsByRegions(selectedRegions) : Array.from(store.items || []);
            applyFilterAndSelection(icbList, source.setAvailableItems);
        }
    }
    function toggleRegionExpansion(regionName) {
        const next = new Set(expandedRegions);
        if (next.has(regionName)) next.delete(regionName); else next.add(regionName);
        expandedRegions = next;
    }
    function toggleRegion(regionName) {
        const nextR = new Set(selectedRegions);
        const nextI = new Set(selectedICBs);
        if (nextR.has(regionName)) nextR.delete(regionName);
        else {
            nextR.add(regionName);
            const regionData = regionsHierarchy.find((r) => r.region === regionName);
            if (regionData) (regionData.icbs || []).forEach((icb) => nextI.delete(icb.name));
        }
        selectedRegions = nextR;
        selectedICBs = nextI;
        applyTrustTypeSelection();
    }
    function toggleICB(icbName) {
        if (selectedRegions.size > 0) return;
        const next = new Set(selectedICBs);
        if (next.has(icbName)) next.delete(icbName);
        else next.add(icbName);
        selectedICBs = next;
        applyTrustTypeSelection();
    }
    $: filterBadgeCount = selectedTrustTypes.size + selectedRegions.size + selectedICBs.size;
    $: hasFilterSelection = selectedTrustTypes.size > 0 || selectedRegions.size > 0 || selectedICBs.size > 0;
    $: hasFilters = $source.filterType === 'trust' && (trustTypes.length > 0 || regionsHierarchy.length > 0);
    $: selectedAvailableCount = selectedItems.filter((item) => isItemAvailable(item)).length;
    $: totalAvailable = Array.from($source.availableItems || []).length;
    $: allSelected = totalAvailable > 0 && selectedAvailableCount >= totalAvailable;
</script>

<div class="dropdown relative w-full min-w-0 h-full flex flex-col" bind:this={dropdownRef}>
    <div class="flex flex-col min-w-0 {(isOpen && overlayMode) || filterDropdownOpen ? 'z-[1000] overflow-visible' : 'overflow-hidden'}">
        <div class="flex flex-col gap-0 min-w-0">
            <div class="flex flex-col sm:flex-row sm:flex-wrap sm:items-center sm:justify-between gap-0 sm:gap-2 min-h-6 mb-2">
                {#if showTitle}
                    <span class="text-sm font-semibold shrink-0 {disabled ? 'text-gray-400' : 'text-gray-800'}">
                        Select {$source.filterType === 'icb' ? 'ICB' : $source.filterType === 'region' ? 'Region' : 'NHS Trust'}
                        {#if maxItems}<span class="text-xs font-normal text-gray-500">(max {maxItems})</span>{/if}
                    </span>
                {/if}
                <div class="flex flex-wrap items-center gap-2 ml-auto min-w-0">
            {#if hasFilters}
                <div class="flex flex-wrap items-center gap-1 text-xs shrink-0">
                    {#if !hideSelectAll && !allSelected}
                        <button type="button" class="text-oxford-600 hover:text-oxford-800 font-medium py-1.5 px-2 sm:py-0.5 sm:px-1.5 rounded hover:bg-gray-100 transition-colors -my-1 sm:my-0 {disabled ? 'opacity-50 cursor-not-allowed' : ''}" on:click={selectAll} disabled={disabled}>Select All</button>
                        {#if hasSelection}<span class="text-gray-300 hidden sm:inline" aria-hidden="true">|</span>{/if}
                    {/if}
                    {#if hasSelection}
                        <button type="button" class="text-red-600 hover:text-red-800 font-medium py-1.5 px-2 sm:py-0.5 sm:px-1.5 rounded hover:bg-red-50 transition-colors -my-1 sm:my-0 {disabled ? 'opacity-50 cursor-not-allowed' : ''}" on:click={deselectAll} disabled={disabled}>Deselect all</button>
                    {/if}
                    {#if hasFilterSelection && (hasSelection || (!hideSelectAll && !allSelected))}<span class="text-gray-300 hidden sm:inline" aria-hidden="true">|</span>{/if}
                    {#if hasFilterSelection}
                        <button type="button" class="text-gray-600 hover:text-gray-800 font-medium py-1.5 px-2 sm:py-0.5 sm:px-1.5 rounded hover:bg-gray-100 transition-colors -my-1 sm:my-0 {disabled ? 'opacity-50 cursor-not-allowed' : ''}" on:click={clearAllFilters} disabled={disabled}>Clear filters</button>
                    {/if}
                </div>
                <div class="relative inline-block">
                    <button type="button" class="inline-flex items-center gap-1.5 h-8 px-2.5 rounded-md text-xs font-medium border transition-colors focus:outline-none focus:ring-2 focus:ring-oxford-500 focus:ring-offset-0 {disabled ? 'opacity-50 cursor-not-allowed bg-gray-100 text-gray-500 border-gray-200' : ''} {filterDropdownOpen || hasFilterSelection ? 'bg-oxford-50 text-oxford-700 border-oxford-200' : 'bg-white text-gray-700 border-gray-200 hover:bg-gray-50'}" on:click={() => !disabled && (filterDropdownOpen = !filterDropdownOpen)} disabled={disabled} aria-haspopup="listbox" aria-expanded={filterDropdownOpen} aria-label="Filter by trust type, region and ICB">
                        <svg class="w-3.5 h-3.5 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" /></svg>
                        <span>Filters</span>
                        {#if hasFilterSelection}<span class="inline-flex items-center justify-center min-w-[1rem] h-4 px-1 rounded-full text-[10px] font-medium leading-none bg-oxford-100 text-oxford-700">{filterBadgeCount}</span>{/if}
                        <svg class="w-3.5 h-3.5 shrink-0 transition-transform {filterDropdownOpen ? 'rotate-180' : ''}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>
                    </button>
                </div>
            {:else}
                <div class="flex flex-wrap items-center gap-1 text-xs shrink-0">
                    {#if !hideSelectAll && !allSelected}
                        <button type="button" class="text-oxford-600 hover:text-oxford-800 font-medium py-1.5 px-2 sm:py-0.5 sm:px-1.5 rounded hover:bg-gray-100 transition-colors -my-1 sm:my-0 {disabled ? 'opacity-50 cursor-not-allowed' : ''}" on:click={selectAll} disabled={disabled}>Select All</button>
                        {#if hasSelection}<span class="text-gray-300 hidden sm:inline" aria-hidden="true">|</span>{/if}
                    {/if}
                    {#if hasSelection}
                        <button type="button" class="text-red-600 hover:text-red-800 font-medium py-1.5 px-2 sm:py-0.5 sm:px-1.5 rounded hover:bg-red-50 transition-colors -my-1 sm:my-0 {disabled ? 'opacity-50 cursor-not-allowed' : ''}" on:click={deselectAll} disabled={disabled}>Deselect all</button>
                    {/if}
                </div>
            {/if}
                </div>
            </div>

            <div class="relative {hasFilters ? 'mt-2' : ''} min-w-0 {hasFilters && filterDropdownOpen && !overlayMode ? 'min-h-[min(70vh,400px)]' : ''}">
                {#if hasFilters && filterDropdownOpen}
                <div class="absolute top-0 left-0 right-0 {overlayMode ? 'z-[1000]' : 'z-10'} w-full min-w-[200px] max-w-full sm:min-w-[240px] max-h-[min(70vh,400px)] flex flex-col rounded-lg border border-gray-200 bg-white shadow-lg overflow-hidden" role="listbox">
                <div class="flex-1 overflow-y-auto py-2">
                    <div class="px-3 pb-2 mb-2 border-b border-gray-100 flex items-center justify-between">
                        <span class="text-xs font-semibold text-gray-600 uppercase tracking-wide">Filter by</span>
                        {#if hasFilterSelection}
                        <button type="button" class="text-xs font-medium text-oxford-600 hover:text-oxford-800 py-0.5 px-1.5 rounded hover:bg-oxford-50 transition-colors" on:click={clearAllFilters}>Clear all</button>
                        {/if}
                    </div>
                    {#if $source.filterType === 'trust' && trustTypes.length > 0}
                    <div class="px-2 pb-2">
                        <div class="px-2 py-1 text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">Trust type</div>
                        {#if acuteTypes.length > 0}
                        <div class="px-2 pt-1 pb-0.5">
                            <div class="flex items-center gap-2 rounded-lg px-2 py-1.5 text-sm font-medium text-gray-800 hover:bg-gray-50 transition-colors">
                                <button type="button" class="p-1 -m-1 hover:bg-gray-200/60 rounded-md transition-colors shrink-0 text-gray-400" on:click={() => expandedAcute = !expandedAcute} aria-label={expandedAcute ? 'Collapse' : 'Expand'}><svg class="w-3.5 h-3.5 transition-transform duration-200 {expandedAcute ? 'rotate-90' : ''}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" /></svg></button>
                                <label class="flex items-center gap-2.5 cursor-pointer flex-1 min-w-0"><input bind:this={acuteParentCheckbox} type="checkbox" checked={allAcuteSelected} on:change={toggleAcuteParent} class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0" /><span>Acute</span></label>
                            </div>
                            {#if expandedAcute}
                            <div class="ml-6 pl-3 mt-0.5 border-l-2 border-gray-200 space-y-0.5">
                                {#each acuteTypes as type}<label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-1 text-sm text-gray-600 transition-colors"><input type="checkbox" checked={selectedTrustTypes.has(type)} on:change={() => toggleTrustType(type)} class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0" /><span>{type.replace(ACUTE_PREFIX, '').trim()}</span></label>{/each}
                            </div>
                            {/if}
                        </div>
                        {/if}
                        {#each otherTypes as type}<label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-2 sm:py-1.5 text-sm text-gray-700 transition-colors mx-1 min-h-[44px] sm:min-h-0"><input type="checkbox" checked={selectedTrustTypes.has(type)} on:change={() => toggleTrustType(type)} class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0" /><span>{type}</span></label>{/each}
                    </div>
                    {/if}
                    {#if regionsHierarchy.length > 0}
                    <div class="px-2 pt-2 {$source.filterType === 'trust' && trustTypes.length > 0 ? 'border-t border-gray-100' : ''}">
                        <div class="px-2 py-1 text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">{$source.filterType === 'icb' ? 'Region' : 'Region & ICB'}</div>
                        {#each regionsHierarchy as region}
                        <div class="px-1">
                            <div class="flex items-center gap-2 rounded-lg px-2 py-2 sm:py-1.5 text-sm hover:bg-gray-50 transition-colors min-h-[44px] sm:min-h-0 {selectedRegions.has(region.region) ? 'text-oxford-700' : 'text-gray-700'}">
                                {#if $source.filterType === 'trust'}<button type="button" class="p-1.5 -m-1 hover:bg-gray-200/60 rounded-md transition-colors shrink-0 text-gray-400" on:click={() => toggleRegionExpansion(region.region)} aria-label={expandedRegions.has(region.region) ? 'Collapse' : 'Expand'}><svg class="w-3.5 h-3.5 transition-transform duration-200 {expandedRegions.has(region.region) ? 'rotate-90' : ''}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" /></svg></button>{/if}
                                <label class="flex items-center gap-2.5 cursor-pointer flex-1 min-w-0"><input type="checkbox" checked={selectedRegions.has(region.region)} on:change={() => toggleRegion(region.region)} class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0 w-4 h-4 shrink-0" /><span class="truncate" title={region.region + (region.region_code ? ' (' + region.region_code + ')' : '')}>{region.region}{#if region.region_code}&nbsp;({region.region_code}){/if}</span></label>
                                {#if $source.filterType === 'trust'}<span class="text-[10px] font-medium text-gray-400 shrink-0 tabular-nums bg-gray-100/80 px-1.5 py-0.5 rounded">{(region.icbs || []).length} ICBs</span>{/if}
                            </div>
                            {#if expandedRegions.has(region.region) && $source.filterType === 'trust'}
                            <div class="ml-6 pl-3 mt-0.5 border-l-2 border-gray-200 space-y-0.5">
                                {#each (region.icbs || []) as icb}<label class="flex items-center gap-2.5 cursor-pointer hover:bg-gray-50 rounded-lg px-2 py-1 text-sm transition-colors {selectedRegions.has(region.region) ? 'opacity-50' : ''} {selectedICBs.has(icb.name) ? 'text-oxford-700' : 'text-gray-600'}"><input type="checkbox" checked={selectedICBs.has(icb.name) || selectedRegions.has(region.region)} disabled={selectedRegions.has(region.region)} on:change={() => toggleICB(icb.name)} class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500 focus:ring-offset-0" /><span class="truncate" title={icb.name + (icb.code ? ' (' + icb.code + ')' : '')}>{stripNhsPrefix(icb.name)}{#if icb.code}&nbsp;({icb.code}){/if}</span></label>{/each}
                            </div>
                            {/if}
                        </div>
                        {/each}
                    </div>
                    {/if}
                </div>
                <div class="py-2 px-3 border-t border-gray-200 flex justify-end bg-gray-50 shrink-0">
                    <button
                        type="button"
                        on:click={() => filterDropdownOpen = false}
                        class="inline-flex justify-center items-center px-3 py-1.5 bg-oxford-50 text-oxford-600 rounded-md hover:bg-oxford-100 transition-colors duration-200 font-medium text-sm border border-oxford-200"
                    >
                        Done
                    </button>
                </div>
                </div>
                {/if}

            <div class="relative flex-1 min-w-0 w-full rounded-md border border-gray-200 {isOpen ? 'rounded-b-none' : ''} {isOpen && overlayMode ? 'overflow-visible' : 'overflow-hidden'} focus-within:ring-2 focus-within:ring-inset focus-within:ring-oxford-500 focus-within:border-oxford-500">
                <div class="flex rounded-t-md overflow-hidden">
                    <div class="relative flex-grow flex items-stretch min-w-0">
                        <input type="text" bind:this={searchInputRef} bind:value={searchTerm} on:focus={() => !disabled && (isOpen = true)} placeholder={placeholderText} disabled={disabled} class="w-full h-full min-h-0 py-2 pl-2.5 pr-8 border-0 focus:outline-none placeholder:text-gray-400 text-gray-900 text-sm {disabled ? 'bg-gray-100 text-gray-500 cursor-not-allowed' : 'bg-white'}" />
                        <button type="button" class="absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 rounded text-gray-400 hover:text-gray-600 hover:bg-gray-100 transition-colors {disabled ? 'hidden' : ''}" on:click|stopPropagation={() => searchTerm = ''} aria-label="Clear search">{#if searchTerm}<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>{/if}</button>
                    </div>
                    <div class="flex items-center justify-center min-w-[3.5rem] sm:min-w-[4.5rem] py-1.5 px-1.5 sm:px-2 border-l border-gray-200 text-center shrink-0 {hasSelection ? 'bg-oxford-50' : 'bg-gray-50'}">
                        <div class="flex flex-col items-center leading-tight min-w-0">
                            <span class="text-[10px] sm:text-xs font-semibold truncate max-w-full {hasSelection ? 'text-oxford-700' : 'text-gray-600'}">{selectedAvailableCount}/{totalAvailable}</span>
                            <span class="text-[9px] sm:text-[10px] text-gray-500">{counterText}</span>
                        </div>
                    </div>
                </div>

                {#if isOpen}
            <div class="bg-white border border-gray-200 border-t-0
                        rounded-b-lg shadow-lg flex flex-col max-h-72 min-w-0 overflow-x-hidden
                        {overlayMode ? 'absolute top-full left-0 right-0 -mt-px z-[996]' : 'w-full max-w-full'}">
                <div class="flex-grow overflow-y-auto overflow-x-hidden min-w-0 divide-y divide-gray-200 text-sm"
                    bind:this={listContainer}
                    on:scroll={updateScrollButtonVisibility}>
                    
                    {#if groupedItems.selected.length > 0}
                        <div class="sticky top-0 z-10 bg-gray-50 border-b border-gray-200 shadow-sm">
                            <button 
                                class="w-full py-1.5 px-2 text-left flex justify-between items-center hover:bg-gray-100 transition-colors"
                                on:click={() => toggleSection('selected')}
                            >
                                <span class="font-medium text-gray-700">
                                    Selected {selectedAvailableCount === totalAvailable 
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
                                    class="p-2 transition duration-150 ease-in-out relative cursor-pointer min-w-0
                                          bg-oxford-100 text-oxford-500 hover:bg-oxford-200"
                                    on:click|stopPropagation={() => toggleItem(item.name)}
                                    on:keypress={(e) => e.key === 'Enter' && toggleItem(item.name)}
                                >
                                    <div class="flex items-center justify-between gap-2">
                                        <div class="min-w-0">
                                            <div class="truncate" title={source.getDisplayName(item.name)}>{source.getDisplayName(item.name)}</div>
                                        </div>
                                        <span class="shrink-0 font-medium">Selected</span>
                                    </div>
                                    {#if item.predecessors.length > 0}
                                        {#each item.predecessors as predecessor}
                                            <div role="button" tabindex="0" class="mt-1 pl-6 transition duration-150 ease-in-out relative min-w-0 {!isItemAvailable(predecessor) ? 'text-gray-400 cursor-not-allowed' : ''} {isItemSelected(predecessor) ? 'text-oxford-500' : ''}" on:click|stopPropagation={() => toggleItem(predecessor)} on:keypress|stopPropagation={(e) => e.key === 'Enter' && toggleItem(predecessor)}>
                                                <div class="flex items-start gap-1.5 min-w-0"><span class="shrink-0 mt-0.5">↳</span><div class="min-w-0 flex-1 overflow-hidden flex items-center gap-1.5"><span class="truncate" title={source.getDisplayName(predecessor)}>{source.getDisplayName(predecessor)}</span><span class="text-gray-400 shrink-0">(predecessor)</span></div></div>
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
                                <span class="font-medium text-gray-700">
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
                                    class="p-2 transition duration-150 ease-in-out relative min-w-0
                                           {limitReached ? 'cursor-not-allowed text-gray-400 bg-gray-50' : 'cursor-pointer hover:bg-gray-100'}"
                                    on:click|stopPropagation={() => toggleItem(item.name)}
                                    on:keypress={(e) => e.key === 'Enter' && toggleItem(item.name)}
                                    title={limitReached ? `Maximum of ${maxItems} NHS Trusts can be selected` : ''}
                                >
                                    <div class="flex items-center justify-between gap-2">
                                        <div class="min-w-0 flex-1">
                                            <div class="truncate" title={source.getDisplayName(item.name)}>{source.getDisplayName(item.name)}</div>
                                        </div>
                                        {#if limitReached}<span class="shrink-0 text-gray-500">Max limit reached</span>{/if}
                                    </div>
                                    {#if item.predecessors.length > 0}
                                        {#each item.predecessors as predecessor}
                                            <div role="button" tabindex="0" class="mt-1 pl-6 transition duration-150 ease-in-out relative min-w-0 {!isItemAvailable(predecessor) || limitReached ? 'text-gray-400 cursor-not-allowed' : ''} {isItemSelected(predecessor) ? 'text-oxford-500' : ''}" on:click|stopPropagation={() => toggleItem(predecessor)} on:keypress|stopPropagation={(e) => e.key === 'Enter' && toggleItem(predecessor)} title={limitReached ? `Maximum of ${maxItems} NHS Trusts can be selected` : ''}>
                                                <div class="flex items-start gap-1.5 min-w-0"><span class="shrink-0 mt-0.5">↳</span><div class="min-w-0 flex-1 overflow-hidden flex items-center gap-1.5"><span class="truncate" title={source.getDisplayName(predecessor)}>{source.getDisplayName(predecessor)}</span><span class="text-gray-400 shrink-0">(predecessor)</span></div></div>
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
                                <span class="font-medium text-gray-700">
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
                                <div class="p-2 transition duration-150 ease-in-out relative text-gray-400 cursor-not-allowed min-w-0">
                                    <div class="min-w-0"><div class="truncate" title={source.getDisplayName(item.name)}>{source.getDisplayName(item.name)}</div></div>
                                    {#if item.predecessors.length > 0}
                                        {#each item.predecessors as predecessor}
                                            <div class="mt-1 pl-6 transition duration-150 ease-in-out relative text-gray-400 cursor-not-allowed min-w-0">
                                                <div class="flex items-start gap-1.5 min-w-0"><span class="shrink-0 mt-0.5">↳</span><div class="min-w-0 flex-1 overflow-hidden flex items-center gap-1.5"><span class="truncate" title={source.getDisplayName(predecessor)}>{source.getDisplayName(predecessor)}</span><span class="text-xs text-gray-400 shrink-0">(predecessor)</span></div></div>
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
        </div>
    </div>
</div>
