<svelte:options customElement={{
    tag: 'organisation-search',
    shadow: 'none'
  }} />

<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import '../../styles/styles.css';

    const dispatch = createEventDispatcher();

    export let items = [];
    export let overlayMode = false;
    export let filterType = 'organisation';

    let isOpen = false;
    let searchTerm = '';
    let selectedItems = [];
    let showOrganisationSelection = false;
    let initialized = false;

    $: filteredItems = items.filter(item => 
        item && typeof item === 'string' && item.toLowerCase().includes(searchTerm.toLowerCase())
    );

    // Initialize selectedItems with all items on component creation
    $: if (items.length > 0 && !initialized) {
        selectedItems = showOrganisationSelection ? [...items] : [];
        initialized = true;
        dispatchSelectionChange();
    }

    $: if (!showOrganisationSelection) {
        selectedItems = [];
        dispatchSelectionChange();
    }

    $: maxSelected = selectedItems.length >= 10;

    function toggleDropdown() {
        if (showOrganisationSelection) {
            isOpen = !isOpen;
            dispatch('dropdownToggle', { isOpen });
        }
    }

    function toggleItem(item) {
        if (selectedItems.includes(item)) {
            selectedItems = selectedItems.filter(i => i !== item);
        } else if (selectedItems.length < 10) {
            selectedItems = [...selectedItems, item];
        }
        dispatchSelectionChange();
    }

    $: isItemSelected = (item) => selectedItems.includes(item);

    function deselectAll() {
        selectedItems = [];
        dispatchSelectionChange();
    }

    function dispatchSelectionChange() {
        dispatch('selectionChange', {
            selectedItems: selectedItems,
            usedOrganisationSelection: showOrganisationSelection
        });
    }

    function toggleOrganisationSelection() {
        showOrganisationSelection = !showOrganisationSelection;
        dispatchSelectionChange();
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
    <div class="flex items-center mb-2 flex-shrink-0">
        <input
            type="checkbox"
            id="showOrganisationSelection"
            checked={showOrganisationSelection}
            on:change={() => toggleOrganisationSelection()}
            class="mr-2 w-4 h-4"
        />
        <label for="showOrganisationSelection" class="text-sm font-medium text-gray-700">
            Filter by specific {filterType === 'organisation' ? 'organisations' : 
                                filterType === 'icb' ? 'ICBs' : 
                                'regions'}
        </label>
    </div>

    {#if showOrganisationSelection}
        <button
            on:click={toggleDropdown}
            class="w-full p-2 border border-gray-300 rounded-md bg-white flex justify-between items-center flex-shrink-0"
        >
            <span>{selectedItems.length} {filterType === 'organisation' ? 'ODS' : 
                                          filterType === 'icb' ? 'ICB' : 
                                          'region'} name(s) selected</span>
            <span class="ml-2">▼</span>
        </button>

        {#if isOpen}
            <div class="mt-1 bg-white border border-gray-300 rounded-md shadow-lg z-10 flex-grow overflow-hidden flex flex-col max-h-96"
                 class:absolute={overlayMode} class:top-full={overlayMode} class:left-0={overlayMode} class:right-0={overlayMode}>
                <div class="p-2 flex-shrink-0">
                    <input
                        type="text"
                        bind:value={searchTerm}
                        placeholder="Search {filterType === 'organisation' ? 'ODS' : 
                                              filterType === 'icb' ? 'ICB' : 
                                              'region'} names..."
                        class="w-full p-2 border border-gray-300 rounded-md mb-2"
                    />
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
            </div>
        {/if}
    {/if}
</div>