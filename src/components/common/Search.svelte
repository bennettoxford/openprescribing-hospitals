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

    export let placeholder = "Search...";
    export let type = "vmp";

    let searchTerm = '';
    let filteredItems = [];
    let selectedItems = [];
    let vmpCount = 0;
    let isCalculating = false;

    const csrftoken = getCookie('csrftoken');

    $: {
        if (type === 'vmp') {
            filteredItems = filterItems($analyseOptions.vmpNames, searchTerm);
        } else if (type === 'vtm') {
            filteredItems = filterItems($analyseOptions.vtmNames, searchTerm);
        } else if (type === 'ingredient') {
            filteredItems = filterItems($analyseOptions.ingredientNames, searchTerm);
        } else if (type === 'atc') {
            filteredItems = filterItems($analyseOptions.atcNames, searchTerm);
        }
    }

    function filterItems(items, term) {
        if (!items) return [];
        if (type === 'atc') {
            return items.filter(item => 
                item.name.toLowerCase().includes(term.toLowerCase())
            );
        }
        return items.filter(item => 
            item.toLowerCase().includes(term.toLowerCase())
        );
    }

    function handleInput() {
        // Update filtered items based on search term
        if (type === 'vmp') {
            filteredItems = filterItems($analyseOptions.vmpNames, searchTerm);
        } else if (type === 'vtm') {
            filteredItems = filterItems($analyseOptions.vtmNames, searchTerm);
        } else if (type === 'ingredient') {
            filteredItems = filterItems($analyseOptions.ingredientNames, searchTerm);
        } else if (type === 'atc') {
            filteredItems = filterItems($analyseOptions.atcNames, searchTerm);
        }
    }

    async function fetchVmpCount() {
        if (selectedItems.length === 0) {
            vmpCount = 0;
            return;
        }

        isCalculating = true;
        try {
            const response = await fetch('/api/filtered-vmp-count/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrftoken,
                },
                body: JSON.stringify({
                    names: type === 'atc' ? selectedItems.map(i => i.code) : selectedItems,
                    search_type: type,
                }),
            });
            const data = await response.json();
            vmpCount = data.vmp_count;
        } catch (error) {
            console.error('Error fetching VMP count:', error);
        } finally {
            isCalculating = false;
        }
    }

    function handleSelect(item) {
        const selectedItem = type === 'atc' ? item.name : item;
        if (!selectedItems.includes(selectedItem)) {
            selectedItems = [...selectedItems, selectedItem];
            dispatch('selectionChange', { items: selectedItems, type });
            fetchVmpCount();
        } else {
            // Remove item if already selected
            selectedItems = selectedItems.filter(i => i !== selectedItem);
            dispatch('selectionChange', { items: selectedItems, type });
            fetchVmpCount();
        }
    }

    function handleRemove(item) {
        selectedItems = selectedItems.filter(i => i !== item);
        dispatch('selectionChange', { items: selectedItems, type });
        fetchVmpCount();
    }

    function handleTypeChange(newType) {
        type = newType;
        selectedItems = [];
        searchTerm = '';
        dispatch('selectionChange', { items: selectedItems, type });
        vmpCount = 0;
    }

    $: {
        if ($analyseOptions.selectedVMPs.length === 0 && selectedItems.length > 0) {
            selectedItems = [];
            searchTerm = '';
            vmpCount = 0;
        }
    }

    onMount(() => {
        // Initial filtering of items
        handleInput();
    });
</script>

<div class="w-full">
    <div class="flex space-x-2 mb-2">
        <button class="px-2 py-1 rounded {type === 'vmp' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('vmp')}>VMP</button>
        <button class="px-2 py-1 rounded {type === 'vtm' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('vtm')}>VTM</button>
        <button class="px-2 py-1 rounded {type === 'ingredient' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('ingredient')}>Ingredient</button>
        <button class="px-2 py-1 rounded {type === 'atc' ? 'bg-oxford-500 text-white' : 'bg-gray-200'}" on:click={() => handleTypeChange('atc')}>ATC</button>
    </div>
    <div class="relative">
        <input
            type="text"
            {placeholder}
            bind:value={searchTerm}
            on:input={handleInput}
            on:keydown={(e) => {
                if (e.key === 'Escape' && searchTerm) {
                    e.preventDefault();
                    searchTerm = '';
                }
            }}
            class="w-full mb-2 p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500 pr-8"
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
    {#if filteredItems.length > 0 && searchTerm.length > 0}
        <ul class="mt-2 border border-gray-300 rounded-md max-h-40 overflow-y-auto">
            {#each filteredItems as item}
                <li 
                    class="p-2 hover:bg-gray-100 cursor-pointer flex items-center justify-between"
                    class:bg-oxford-100={selectedItems.includes(type === 'atc' ? item.name : item)}
                    on:click={() => handleSelect(item)}
                >
                    <span>{type === 'atc' ? item.name : item}</span>
                    {#if selectedItems.includes(type === 'atc' ? item.name : item)}
                        <span class="text-sm font-medium text-oxford-500">Selected</span>
                    {/if}
                </li>
            {/each}
        </ul>
    {/if}
    {#if selectedItems.length > 0}
        <div>
            <h3 class="font-semibold mb-2 text-md text-gray-700">Selected {type.toUpperCase()} names:</h3>
            <ul class="border border-gray-200 rounded-md">
                {#each selectedItems as item}
                    <li class="flex items-center justify-between px-2 py-1">
                        <span class="text-gray-800">
                            {typeof item === 'string' ? item : `${item.code} | ${item.name}`}
                        </span>
                        <button 
                            on:click={() => handleRemove(item)}
                            class="btn-red-sm"
                        >
                            Remove
                        </button>
                    </li>
                {/each}
            </ul>
            <p class="mt-2 text-sm text-gray-600">
                {#if isCalculating}
                    Calculating number of individual products analysed...
                {:else if vmpCount > 0}
                    This selection will analyse {vmpCount} unique VMP{vmpCount !== 1 ? 's' : ''}.
                    {#if vmpCount > 100}
                        <span class="text-amber-600 font-semibold">
                            Warning: This is a large number of products. The analysis may take a long time. 
                            Consider making your selection more specific.
                        </span>
                    {/if}
                {/if}
            </p>
        </div>
    {/if}
</div>
