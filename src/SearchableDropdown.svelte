<svelte:options customElement={{
    tag: 'searchable-dropdown',
    shadow: 'none'
  }} />

<script>
    import { onMount, createEventDispatcher } from 'svelte';
    import './styles/styles.css';

    const dispatch = createEventDispatcher();

    export let items = [];
    let isOpen = false;
    let searchTerm = '';
    let selectedItems = [];
    let showOrganizationSelection = false;
    let initialized = false;

    $: filteredItems = items.filter(item => 
        item.toLowerCase().includes(searchTerm.toLowerCase())
    );

    // Initialize selectedItems with all items on component creation
    $: if (items.length > 0 && !initialized) {
        selectedItems = [...items];
        initialized = true;
        dispatchSelectionChange();
    }

    function toggleDropdown() {
        if (showOrganizationSelection) {
            isOpen = !isOpen;
        }
    }

    function toggleItem(item) {
        if (selectedItems.includes(item)) {
            selectedItems = selectedItems.filter(i => i !== item);
        } else {
            selectedItems = [...selectedItems, item];
        }
        selectedItems = [...selectedItems]; // Force reactivity
        dispatchSelectionChange();
    }

    $: isItemSelected = (item) => selectedItems.includes(item);

    function selectAll() {
        selectedItems = [...items];
        dispatchSelectionChange();
    }

    function deselectAll() {
        selectedItems = [];
        dispatchSelectionChange();
    }

    function dispatchSelectionChange() {
        dispatch('selectionChange', selectedItems);
    }

    function toggleOrganizationSelection() {
        showOrganizationSelection = !showOrganizationSelection;
        if (!showOrganizationSelection) {
            isOpen = false;
        }
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

<div class="dropdown relative w-full">
    <div class="flex items-center mb-2">
        <input
            type="checkbox"
            id="showOrganizationSelection"
            checked={showOrganizationSelection}
            on:change={() => toggleOrganizationSelection()}
            class="mr-2"
        />
        <label for="showOrganizationSelection" class="text-sm font-medium text-gray-700">
            Filter by specific organizations
        </label>
    </div>

    {#if showOrganizationSelection}
        <button
            on:click={toggleDropdown}
            class="w-full p-2 border border-gray-300 rounded-md bg-white flex justify-between items-center"
        >
            <span>{selectedItems.length} ODS name(s) selected</span>
            <span class="ml-2">▼</span>
        </button>

        {#if isOpen}
            <div class="absolute top-full left-0 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg z-10">
                <div class="p-2">
                    <input
                        type="text"
                        bind:value={searchTerm}
                        placeholder="Search ODS names..."
                        class="w-full p-2 border border-gray-300 rounded-md mb-2"
                    />
                    <div class="flex justify-between mb-2">
                        <button
                            on:click={selectAll}
                            class="btn-green-sm"
                        >
                            Select All
                        </button>
                        <button
                            on:click={deselectAll}
                            class="btn-red-sm"
                        >
                            Deselect All
                        </button>
                    </div>
                </div>
                <ul class="max-h-60 overflow-y-auto divide-y divide-gray-200">
                    {#each filteredItems as item (item)}
                        <li 
                            class="p-2 cursor-pointer transition duration-150 ease-in-out"
                            class:bg-blue-100={isItemSelected(item)}
                            class:text-blue-700={isItemSelected(item)}
                            class:hover:bg-gray-100={!isItemSelected(item)}
                            on:click={() => toggleItem(item)}
                        >
                            <div class="flex items-center">
                                <span>{item}</span>
                                {#if isItemSelected(item)}
                                    <span class="ml-auto text-sm font-medium">Selected</span>
                                {/if}
                            </div>
                        </li>
                    {/each}
                </ul>
            </div>
        {/if}
    {/if}
</div>