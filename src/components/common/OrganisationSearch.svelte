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

    let isOpen = false;
    let searchTerm = '';

    $: items = $source.items || [];
    $: selectedItems = $source.selectedItems || [];
    $: showOrganisationSelection = $source.usedOrganisationSelection;
    $: filterType = $source.filterType;

    $: filteredItems = items
        .filter(item => 
            item && 
            typeof item === 'string' && 
            item.toLowerCase().includes(searchTerm.toLowerCase())
        )
        .sort((a, b) => {
            if (isItemSelected(a) === isItemSelected(b)) {
                return 0;
            }
            return isItemSelected(a) ? -1 : 1;
        });

    $: maxSelected = selectedItems.length >= 10;

    function toggleDropdown() {
        isOpen = !isOpen;
        dispatch('dropdownToggle', { isOpen });
    }

    function toggleOrganisationSelection() {
        source.toggleSelection();
        if (!showOrganisationSelection) {
            isOpen = true;
        }
        dispatch('dropdownToggle', { isOpen });
    }

    $: isItemSelected = (item) => selectedItems.includes(item);

    function toggleItem(item) {
        let newSelectedItems;
        if (selectedItems.includes(item)) {
            newSelectedItems = selectedItems.filter(i => i !== item);
        } else if (selectedItems.length < 10) {
            newSelectedItems = [...selectedItems, item];
        } else {
            return;
        }
        
        source.updateSelection(newSelectedItems, showOrganisationSelection);
        dispatch('selectionChange', {
            selectedItems: newSelectedItems,
            usedOrganisationSelection: showOrganisationSelection
        });
    }

    function deselectAll() {
        source.updateSelection([], showOrganisationSelection);
        dispatch('selectionChange', {
            selectedItems: [],
            usedOrganisationSelection: showOrganisationSelection
        });
    }

    onMount(() => {
        const handleClickOutside = (event) => {
            if (isOpen && !event.target.closest('.dropdown')) {
                isOpen = false;
            }
        };

        document.addEventListener('click', handleClickOutside);

        return () => {
            document.removeEventListener('click', handleClickOutside);
        };
    });
</script>

<div class="dropdown relative w-full h-full flex flex-col">
    <div class="flex items-center h-8 flex-shrink-0">
        <input
            type="checkbox"
            id="showOrganisationSelection"
            checked={showOrganisationSelection}
            on:change={toggleOrganisationSelection}
            class="mr-2 w-4 h-4 cursor-pointer"
        />
        <label 
            for="showOrganisationSelection" 
            class="text-sm font-medium text-gray-700 cursor-pointer"
        >
            Filter by specific {filterType === 'icb' ? 'ICBs' : 'NHS Trusts'}
        </label>
    </div>

    {#if showOrganisationSelection}
        <button
            on:click={toggleDropdown}
            class="w-full p-2 border border-gray-300 rounded-md bg-white flex justify-between items-center flex-shrink-0 mt-1"
        >
            <span>{selectedItems.length} {filterType === 'icb' ? 'ICB' : 'NHS Trust'} name(s) selected</span>
            <span class="ml-2">{isOpen ? '▼' : '▲'}</span>
        </button>

        {#if isOpen}
            <div class="absolute top-[calc(100%_+_0px)] left-0 right-0 bg-white border border-gray-300 rounded-md shadow-lg z-50 flex flex-col max-h-96"
                 class:absolute={overlayMode}>
                <div class="p-2 flex-shrink-0">
                    <div class="relative">
                        <input
                            type="text"
                            bind:value={searchTerm}
                            placeholder="Search {filterType === 'icb' ? 'ICB' : 'NHS Trust'} names..."
                            class="w-full p-2 border border-gray-300 rounded-md mb-2 pr-8"
                        />
                        {#if searchTerm}
                            <button
                                class="absolute right-2 top-0 h-full flex items-center justify-center text-gray-400 hover:text-gray-600 w-5"
                                on:click|stopPropagation={() => searchTerm = ''}
                            >
                                ✕
                            </button>
                        {/if}
                    </div>
                    <div class="flex justify-between mb-2">
                        <button
                            on:click={deselectAll}
                            class="btn-red-sm"
                        >
                            Deselect All
                        </button>
                    </div>
                </div>
                <ul class="flex-grow overflow-y-auto divide-y divide-gray-200">
                    {#each filteredItems as item (item)}
                        <li 
                            class="p-2 cursor-pointer transition duration-150 ease-in-out relative {maxSelected && !isItemSelected(item) ? 'bg-gray-300 cursor-not-allowed' : ''}"
                            class:bg-oxford-100={isItemSelected(item)}
                            class:text-oxford-500={isItemSelected(item)}
                            class:hover:bg-oxford-200={isItemSelected(item)}
                            class:hover:bg-gray-100={!isItemSelected(item) && !maxSelected}
                            on:click={() => { if (!maxSelected || isItemSelected(item)) toggleItem(item); }}
                        >
                            <div class="flex items-center">
                                <span>{item}</span>
                                {#if isItemSelected(item)}
                                    <span class="ml-auto text-sm font-medium">Selected</span>
                                {/if}
                                {#if maxSelected && !isItemSelected(item)}
                                    <div class="absolute top-0 left-full ml-2 w-auto p-1 text-sm text-white bg-black rounded">
                                        Max selected
                                    </div>
                                {/if}
                            </div>
                        </li>
                    {/each}
                </ul>
                <button
                    on:click={() => {
                        const listContainer = document.querySelector('.dropdown .overflow-y-auto');
                        if (listContainer) {
                            listContainer.scrollTo({ top: 0 });
                        }
                    }}
                    class="p-2 bg-gray-100 border-t border-gray-200 flex items-center justify-center hover:bg-oxford-50 active:bg-oxford-100 cursor-pointer transition-colors duration-150 gap-2"
                >
                    <span class="text-sm font-medium text-gray-700">
                        {selectedItems.length} Trusts selected • Click to scroll to top
                    </span>
                    <svg class="w-4 h-4 text-gray-700" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
                    </svg>
                </button>
            </div>
        {/if}
    {/if}
</div>
