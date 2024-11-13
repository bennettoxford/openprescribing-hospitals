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
    let listContainer;
    let showScrollTop = false;

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

    function updateScrollButtonVisibility() {
        if (listContainer) {
            console.log({
                scrollHeight: listContainer.scrollHeight,
                clientHeight: listContainer.clientHeight,
                scrollTop: listContainer.scrollTop
            });
            
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
        <div class="flex flex-col gap-2 mt-1">
            <div class="flex">
                <div class="relative flex-grow">
                    <input
                        type="text"
                        bind:value={searchTerm}
                        on:focus={() => isOpen = true}
                        placeholder="Search and select up to 10 {filterType === 'icb' ? 'ICBs' : 'NHS Trusts'}..."
                        class="w-full p-2 border border-gray-300 rounded-l-md pr-8 {isOpen ? 'rounded-bl-none' : ''}"
                    />
                    <button
                        class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                        on:click|stopPropagation={() => searchTerm = ''}
                    >
                        {#if searchTerm}
                            <span>✕</span>
                        {/if}
                    </button>
                </div>

                <div class="flex items-center gap-2 bg-gray-50 px-3 border border-l-0 border-gray-300 {isOpen ? 'rounded-tr-md' : 'rounded-r-md'}">
                    {#if selectedItems.length > 0}
                        <button
                            on:click={deselectAll}
                            class="text-sm text-red-600 hover:text-red-700 hover:underline font-medium transition-colors duration-150"
                        >
                            Clear all
                        </button>
                        <div class="w-[1px] h-4 bg-gray-300"></div>
                    {/if}
                    <div class="text-sm text-gray-500">
                        {selectedItems.length}/10
                    </div>
                </div>
            </div>

            {#if isOpen}
                <div class="absolute top-[calc(100%_-_1px)] left-0 right-0 bg-white border border-gray-300 rounded-md rounded-t-none shadow-lg z-50 flex flex-col max-h-72"
                     class:absolute={overlayMode}>
                    <ul 
                        class="flex-grow overflow-y-auto divide-y divide-gray-200"
                        bind:this={listContainer}
                        on:scroll={updateScrollButtonVisibility}
                    >
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
                    {#if showScrollTop}
                        <button
                            on:click={() => {
                                if (listContainer) {
                                    listContainer.scrollTo({ top: 0 });
                                }
                            }}
                            class="p-2 bg-gray-100 border-t border-gray-200 flex items-center justify-center hover:bg-oxford-50 active:bg-oxford-100 cursor-pointer transition-colors duration-150 gap-2"
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
    {/if}
</div>
