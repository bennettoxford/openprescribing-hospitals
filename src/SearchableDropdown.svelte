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

    $: filteredItems = items.filter(item => 
        item.toLowerCase().includes(searchTerm.toLowerCase())
    );

    $: allSelected = selectedItems.length === items.length;

    function toggleDropdown() {
        isOpen = !isOpen;
    }

    function toggleItem(item) {
        const index = selectedItems.findIndex(i => i === item);
        if (index === -1) {
            selectedItems = [...selectedItems, item];
        } else {
            selectedItems = selectedItems.filter(i => i !== item);
        }
        dispatchSelectionChange();
    }

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

<div class="dropdown relative w-64">
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
                        class="text-sm text-blue-500 hover:text-blue-700"
                    >
                        Select All
                    </button>
                    <button
                        on:click={deselectAll}
                        class="text-sm text-blue-500 hover:text-blue-700"
                    >
                        Deselect All
                    </button>
                </div>
            </div>
            <ul class="max-h-60 overflow-y-auto">
                {#each filteredItems as item}
                    <li class="p-2 hover:bg-gray-100">
                        <label class="flex items-center space-x-2">
                            <input
                                type="checkbox"
                                checked={selectedItems.includes(item)}
                                on:change={() => toggleItem(item)}
                            />
                            <span>{item}</span>
                        </label>
                    </li>
                {/each}
            </ul>
        </div>
    {/if}
</div>