<script>
    import { createEventDispatcher, onMount } from 'svelte';
    import { slide } from 'svelte/transition';
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';

    export let vmps = [];
    
    const dispatch = createEventDispatcher();

    let checkedVMPs = {};

    $: quantityType = $resultsStore.quantityType;
    $: isAdvancedMode = $analyseOptions.isAdvancedMode;

    onMount(() => {
        initializeCheckedVMPs();
    });

    function initializeCheckedVMPs() {
        checkedVMPs = Object.fromEntries(vmps.map(vmp => [vmp.vmp, vmp.unit !== 'nan']));
    }

    $: {
        if (vmps.length > 0) {
            initializeCheckedVMPs();
        }
    }

    $: hasIngredients = vmps.some(vmp => vmp.ingredients);
    
    $: selectedCount = Object.values(checkedVMPs).filter(Boolean).length;

    $: uniqueUnits = [...new Set(vmps.filter(vmp => vmp.unit !== 'nan').map(vmp => vmp.unit))];
    $: showUnitWarning = uniqueUnits.length > 1;

    $: uniqueUnitIngredientPairs = [...new Set(vmps.filter(vmp => vmp.unit !== 'nan').map(vmp => `${vmp.unit}-${vmp.ingredient || ''}`))]
    $: showUnitIngredientWarning = quantityType === 'Ingredient' && hasIngredients && uniqueUnitIngredientPairs.length > 1;

    let sortColumn = 'vmp';
    let sortDirection = 1;

    function sortBy(column) {
        if (sortColumn === column) {
            sortDirection *= -1;
        } else {
            sortColumn = column;
            sortDirection = 1;
        }
    }

    function getSortIndicator(column) {
        if (sortColumn === column) {
            return sortDirection > 0 ? '↑↓' : '↓↑';
        }
        return '↑↓';
    }


    $: sortedVMPs = [...vmps].sort((a, b) => {
        // Always keep 'nan' units at the top
        if (a.unit === 'nan' && b.unit !== 'nan') return -1;
        if (a.unit !== 'nan' && b.unit === 'nan') return 1;
        if (a.unit === 'nan' && b.unit === 'nan') return 0;

        // For non-'nan' units, apply the regular sorting
        if (sortColumn === 'selected') {
            return sortDirection * (checkedVMPs[b.vmp] - checkedVMPs[a.vmp]);
        }
        
        let aValue = a[sortColumn] || '';
        let bValue = b[sortColumn] || '';
        return sortDirection * aValue.localeCompare(bValue, undefined, {numeric: true, sensitivity: 'base'});
    });

    function updateCheckedVMPs(vmp) {
        if (vmp && vmp.vmp) {
            checkedVMPs[vmp.vmp] = !checkedVMPs[vmp.vmp];
            checkedVMPs = {...checkedVMPs};
            updateFilteredData();
        }
    }

    function updateFilteredData() {
        const selectedVMPs = vmps.filter(vmp => {
            if (vmp && vmp.vmp) {
                return checkedVMPs[vmp.vmp] ?? false;
            }
            return false;
        });

        dispatch('dataFiltered', selectedVMPs);

        resultsStore.update(store => {
            const originalData = store.analysisData?.data || [];
            
            const filteredData = originalData.filter(item => {
                const isSelectedVMP = selectedVMPs.some(vmp => vmp.vmp === item.vmp_name);
                return isSelectedVMP && item.unit !== 'nan';
            });

            return {
                ...store,
                filteredData: filteredData
            };
        });
    }

    $: {
        if (vmps.length > 0 && Object.keys(checkedVMPs).length > 0) {
            updateFilteredData();
        }
    }

    $: missingVMPs = vmps.filter(vmp => vmp.unit === 'nan').map(vmp => vmp.vmp);
    $: hasMissingVMPs = missingVMPs.length > 0;

    $: hasMultipleIngredients = vmps.some(vmp => vmp.ingredients && vmp.ingredients.length > 1);
    
    $: hasWarnings = isAdvancedMode && (showUnitWarning || showUnitIngredientWarning || hasMultipleIngredients);

    let showWarnings = false;
</script>

<div class="p-4">
    <h3 class="text-xl font-semibold mb-4">
        Products included in analysis
    </h3>
    
    <div class="mb-4 text-sm text-gray-700">
        <p class="mb-2">This table shows all products returned in your analysis. Use the checkboxes to select or deselect products.</p>
        <p>Only selected products with valid values quantity values will be used in the analysis.</p>
    </div>

    <p class="mb-2 text-sm text-gray-600">
        Selected: <span class="font-semibold">{selectedCount}</span> out of <span class="font-semibold">{vmps.length}</span>
    </p>
    
    <div class="overflow-visible">
        <div class="relative max-h-96 overflow-y-auto">
            <div class="overflow-x-auto">
                <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
                    <thead class="bg-gray-200 text-gray-600 text-sm leading-normal sticky top-0 z-10">
                        <tr>
                            <th class="py-3 px-6 text-left" class:cursor-pointer={isAdvancedMode} on:click={() => isAdvancedMode && sortBy('vmp')}>
                                Product {#if isAdvancedMode}<span class="text-gray-400">{getSortIndicator('vmp')}</span>{/if}
                            </th>
                            <th class="py-3 px-6 text-left" class:cursor-pointer={isAdvancedMode} on:click={() => isAdvancedMode && sortBy('vtm')}>
                                Product Group {#if isAdvancedMode}<span class="text-gray-400">{getSortIndicator('vtm')}</span>{/if}
                            </th>
                            <th class="py-3 px-6 text-left" class:cursor-pointer={isAdvancedMode} on:click={() => isAdvancedMode && sortBy('ingredients')}>
                                Ingredient {#if isAdvancedMode}<span class="text-gray-400">{getSortIndicator('ingredients')}</span>{/if}
                            </th>
                            <th class="py-3 px-6 text-left" class:cursor-pointer={isAdvancedMode} on:click={() => isAdvancedMode && sortBy('unit')}>
                                Unit {#if isAdvancedMode}<span class="text-gray-400">{getSortIndicator('unit')}</span>{/if}
                            </th>
                            {#if isAdvancedMode}
                            <th class="py-3 px-6 text-left cursor-pointer" on:click={() => sortBy('selected')}>
                                Select <span class="text-gray-400">{getSortIndicator('selected')}</span>
                            </th>
                            {/if}
                        </tr>
                    </thead>
                    <tbody class="text-gray-600 text-sm">
                        {#each sortedVMPs as vmp}
                            <tr class="border-b border-gray-200" 
                                class:hover:bg-gray-100={vmp.unit !== 'nan'}
                                class:bg-red-100={vmp.unit === 'nan'}
                            >
                                <td class="py-3 px-6 text-left">{vmp.vmp}</td>
                                <td class="py-3 px-6 text-left">{vmp.vtm || '-'}</td>
                                <td class="py-3 px-6 text-left">
                                    {#if vmp.ingredients && vmp.ingredients.length > 0}
                                        {vmp.ingredients.join(', ')}
                                    {:else}
                                        <span class="text-gray-400">-</span>
                                    {/if}
                                </td>
                                <td class="py-3 px-6 text-left">{vmp.unit === 'nan' ? '-' : vmp.unit}</td>
                                {#if isAdvancedMode}
                                <td class="py-3 px-6 text-left">
                                    <div class="relative inline-block group">
                                        <input 
                                            type="checkbox" 
                                            checked={checkedVMPs[vmp.vmp] ?? false}
                                            on:change={() => updateCheckedVMPs(vmp)}
                                            disabled={vmp.unit === 'nan' || (checkedVMPs[vmp.vmp] && Object.values(checkedVMPs).filter(Boolean).length === 1)}
                                            class="form-checkbox h-5 w-5 text-blue-600"
                                            class:cursor-not-allowed={vmp.unit === 'nan' || (checkedVMPs[vmp.vmp] && Object.values(checkedVMPs).filter(Boolean).length === 1)}
                                        >
                                        {#if checkedVMPs[vmp.vmp] && Object.values(checkedVMPs).filter(Boolean).length === 1}
                                            <div class="absolute z-[9999] scale-0 transition-all duration-100 origin-right transform 
                                                        group-hover:scale-100 w-[200px] right-[calc(100%+0.5rem)] top-1/2 -translate-y-1/2
                                                        rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 p-4">
                                                <p class="text-sm text-gray-500">
                                                    At least one product must remain selected for analysis
                                                </p>
                                            </div>
                                        {/if}
                                    </div>
                                </td>
                                {/if}
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    {#if hasMissingVMPs}
        <div class="mt-4 p-3 bg-red-100 border border-red-200 rounded-lg text-red-700 text-sm">
            Products shaded in red have no quantity data and will be excluded from the analysis.
            <a href="/faq#missing-quantities" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                Find out why in the FAQs
            </a>
        </div>
    {/if}

    {#if hasWarnings}
        <div class="mt-4">
            <button
                class="flex items-center justify-between w-full p-4 bg-yellow-100 text-yellow-800 border border-yellow-200 transition-all duration-300 ease-in-out"
                class:rounded-lg={!showWarnings}
                class:rounded-t-lg={showWarnings}
                on:click={() => showWarnings = !showWarnings}
            >
                <span class="font-semibold">Warnings</span>
                <svg class="w-5 h-5 transition-transform" class:rotate-180={showWarnings} xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                    <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                </svg>
            </button>
            {#if showWarnings}
                <div transition:slide class="p-4 bg-yellow-50 border border-yellow-200 rounded-b-lg">
                    <ul class="list-disc list-inside space-y-2">
                        {#if showUnitWarning}
                            <li class="text-yellow-700">
                                This list contains multiple units. Please review carefully.
                            </li>
                        {/if}
                        {#if showUnitIngredientWarning}
                            <li class="text-yellow-700">
                                This list contains multiple unit-ingredient combinations. Please review carefully.
                            </li>
                        {/if}
                        {#if hasMultipleIngredients}
                            <li class="text-yellow-700">
                                Some products have multiple ingredients. When viewing by ingredient, quantities will be split equally between each ingredient for these products:
                                <ul class="list-disc list-inside ml-4 mt-1">
                                    {#each vmps.filter(vmp => vmp.ingredients && vmp.ingredients.length > 1) as vmp}
                                        <li>{vmp.vmp}: {vmp.ingredients.join(', ')}</li>
                                    {/each}
                                </ul>
                            </li>
                        {/if}
                    </ul>
                </div>
            {/if}
        </div>
    {/if}
</div>
