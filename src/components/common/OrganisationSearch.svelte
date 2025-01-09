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

    $: placeholderText = disabled ? 
        'Selection disabled' :
        `Search and select ${
            $source.filterType === 'icb' ? 'Integrated Care Boards' : 
            $source.filterType === 'region' ? 'regions' : 
            'NHS Trusts'
        }...`;

    $: counterText = $source.filterType === 'icb' ? 'ICBs' :
                     $source.filterType === 'region' ? 'regions' :
                     'NHS Trusts';

    let isOpen = false;
    let searchTerm = '';
    let listContainer;
    let showScrollTop = false;

    $: items = $source.items || [];
    $: selectedItems = Array.from($source.selectedItems || []);
    $: availableItems = Array.from($source.availableItems || []);
    $: showOrganisationSelection = $source.usedOrganisationSelection;

    function normalizeString(str) {
        return str
            .toLowerCase()
            .replace(/['".,\/#!$%\^&\*;:{}=\-_`~()]/g, '') // Remove punctuation
            .replace(/\s+/g, ' ') // Normalize whitespace
            .trim();
    }

    $: filteredItems = items
        .filter(item => 
            item && 
            typeof item === 'string' && 
            normalizeString(item).includes(normalizeString(searchTerm))
        )
        .sort((a, b) => {
            if (isItemSelected(a) === isItemSelected(b)) {
                return 0;
            }
            return isItemSelected(a) ? -1 : 1;
        });


    $: isItemSelected = (item) => {
        const selected = $source.selectedItems || [];
        return Array.isArray(selected) ? (selected.includes(item) && isItemAvailable(item)) : false;
    };

    function toggleItem(item) {
        if (!isItemAvailable(item)) {
            return;
        }

        let newSelectedItems;
        if (selectedItems.includes(item)) {
            newSelectedItems = selectedItems.filter(i => i !== item);
        } else {
            newSelectedItems = [...selectedItems, item];
        }
        
        source.updateSelection(newSelectedItems);
        dispatch('selectionChange', {
            selectedItems: newSelectedItems,
            source: 'search'
        });
    }

    function deselectAll() {
        source.updateSelection([]);
        dispatch('clearAll');
        dispatch('selectionChange', {
            selectedItems: [],
            source: 'search',
            usedOrganisationSelection: showOrganisationSelection
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
        const availableItems = $source.availableItems || [];
        source.updateSelection(availableItems);
        dispatch('selectionChange', {
            selectedItems: availableItems,
            source: 'search'
        });
    }
</script>

<div class="dropdown relative w-full h-full flex flex-col">
    <div class="flex flex-col">
        <div class="flex flex-col gap-2">
            <div class="flex justify-between items-center">
                <label class="text-sm font-medium {disabled ? 'text-gray-400' : 'text-gray-700'}">
                    Select {$source.filterType === 'icb' ? 'ICB' : 
                           $source.filterType === 'region' ? 'Region' : 
                           'NHS Trust'}
                </label>
                <div class="flex items-center gap-2 text-sm">
                    <button 
                        class="text-blue-600 hover:text-blue-800 font-medium {disabled ? 'opacity-50 cursor-not-allowed' : ''}" 
                        on:click={selectAll}
                        disabled={disabled}
                    >
                        Select All
                    </button>
                    <span class="text-gray-300">|</span>
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
                        class="w-full p-2 border border-gray-300 rounded-l-md pr-8 
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
                            {disabled ? 'bg-gray-100' : ''} min-w-[120px]">
                    <div class="flex flex-col items-center text-xs text-gray-500 py-1 w-full">
                        <span class="font-medium">{selectedItems.filter(item => isItemAvailable(item)).length}/{availableItems.length}</span>
                        <span>{counterText}</span>
                    </div>
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
                    {#each filteredItems as item}
                        <div
                            class="p-2 transition duration-150 ease-in-out relative
                                   {!isItemAvailable(item) ? 'text-gray-400 cursor-not-allowed' : 'cursor-pointer'}
                                   {isItemSelected(item) ? 'bg-oxford-100 text-oxford-500' : ''}
                                   {isItemSelected(item) ? 'hover:bg-oxford-200' : isItemAvailable(item) ? 'hover:bg-gray-100' : ''}"
                            on:click={() => { 
                                if (isItemAvailable(item)) {
                                    toggleItem(item);
                                }
                            }}
                        >
                            <div class="flex items-center justify-between">
                                <span>{item}</span>
                                {#if isItemSelected(item)}
                                    <span class="ml-auto text-sm font-medium">Selected</span>
                                {:else if !isItemAvailable(item)}
                                    <span class="ml-auto text-sm italic">(excluded)</span>
                                {/if}
                            </div>
                        </div>
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
</div>

<style>
    .unavailable {
        color: #999;
        cursor: not-allowed;
    }

    .text-gray-400 {
        cursor: not-allowed;
    }
</style>
