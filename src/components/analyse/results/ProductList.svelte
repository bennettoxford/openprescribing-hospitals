<script>
    import { createEventDispatcher, onMount } from 'svelte';

    export let vmps = [];
    export let currentSearchType = 'vmp';

    const dispatch = createEventDispatcher();

    let checkedVMPs = {};

    onMount(() => {
        console.log('VMPs received:', vmps);
        initializeCheckedVMPs();
    });

    function initializeCheckedVMPs() {
        checkedVMPs = Object.fromEntries(vmps.map(vmp => [vmp.vmp, vmp.unit !== 'nan']));
        console.log('Initialized checkedVMPs:', checkedVMPs);
    }

    $: {
        if (vmps.length > 0) {
            initializeCheckedVMPs();
        }
    }

    $: hasIngredients = vmps.some(vmp => vmp.ingredient);
    $: hasVTMs = vmps.some(vmp => vmp.vtm);
    
    $: selectedCount = Object.values(checkedVMPs).filter(Boolean).length;

    $: uniqueUnits = [...new Set(vmps.filter(vmp => vmp.unit !== 'nan').map(vmp => vmp.unit))];
    $: uniqueUnitIngredientPairs = [...new Set(vmps.filter(vmp => vmp.unit !== 'nan').map(vmp => `${vmp.unit}-${vmp.ingredient || ''}`))]
    $: showUnitWarning = uniqueUnits.length > 1;
    $: showUnitIngredientWarning = hasIngredients && uniqueUnitIngredientPairs.length > 1;

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

    $: displayField = currentSearchType === 'vmp' ? 'vmp' : (currentSearchType === 'vtm' ? 'vtm' : 'ingredient');

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
            checkedVMPs = {...checkedVMPs}; // Trigger reactivity
            console.log('Updated checkedVMPs:', checkedVMPs);
            updateFilteredData();
        } else {
            console.error('Invalid VMP object:', vmp);
        }
    }

    function updateFilteredData() {
        const selectedVMPs = vmps.filter(vmp => {
            if (vmp && vmp.vmp) {
                return checkedVMPs[vmp.vmp] ?? false;
            } else {
                console.error('Invalid VMP object in filter:', vmp);
                return false;
            }
        });
        console.log('Filtered VMPs:', selectedVMPs);
        dispatch('dataFiltered', selectedVMPs);
    }

    // Initial dispatch
    $: {
        if (Object.keys(checkedVMPs).length > 0) {
            updateFilteredData();
        }
    }

    $: missingVMPs = vmps.filter(vmp => vmp.unit === 'nan').map(vmp => vmp.vmp);
    $: hasMissingVMPs = missingVMPs.length > 0;
</script>

<div class="p-4">
    <h3 class="text-xl font-semibold mb-4">Products included</h3>
    <p class="mb-2 text-sm text-gray-600">
        Selected: <span class="font-semibold">{selectedCount}</span> out of <span class="font-semibold">{vmps.length}</span>
    </p>
    <div class="overflow-x-auto">
        <div class="max-h-96 overflow-y-auto relative">
            <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
                <thead class="bg-gray-200 text-gray-600 uppercase text-sm leading-normal sticky top-0 z-10">
                    <tr>
                        <th class="py-3 px-6 text-left cursor-pointer" on:click={() => sortBy(displayField)}>
                            {currentSearchType.toUpperCase()} Name <span class="text-gray-400">{getSortIndicator(displayField)}</span>
                        </th>
                        <th class="py-3 px-6 text-left cursor-pointer" on:click={() => sortBy('unit')}>
                            Unit <span class="text-gray-400">{getSortIndicator('unit')}</span>
                        </th>
                        {#if currentSearchType !== 'vmp'}
                            <th class="py-3 px-6 text-left cursor-pointer" on:click={() => sortBy('vmp')}>
                                VMP <span class="text-gray-400">{getSortIndicator('vmp')}</span>
                            </th>
                        {/if}
                        <th class="py-3 px-6 text-left cursor-pointer" on:click={() => sortBy('selected')}>
                            Select <span class="text-gray-400">{getSortIndicator('selected')}</span>
                        </th>
                    </tr>
                </thead>
                <tbody class="text-gray-600 text-sm">
                    {#each sortedVMPs as vmp}
                        <tr class="border-b border-gray-200 hover:bg-gray-100" class:bg-red-100={vmp.unit === 'nan'}>
                            <td class="py-3 px-6 text-left">{vmp[displayField] || vmp.vmp || 'N/A'}</td>
                            <td class="py-3 px-6 text-left">{vmp.unit === 'nan' ? '-' : vmp.unit}</td>
                            {#if currentSearchType !== 'vmp'}
                                <td class="py-3 px-6 text-left">{vmp.vmp}</td>
                            {/if}
                            <td class="py-3 px-6 text-left">
                                <input 
                                    type="checkbox" 
                                    checked={checkedVMPs[vmp.vmp] ?? false}
                                    on:change={() => updateCheckedVMPs(vmp)}
                                    disabled={vmp.unit === 'nan'}
                                    class="form-checkbox h-5 w-5 text-blue-600"
                                    class:cursor-not-allowed={vmp.unit === 'nan'}
                                >
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </div>
    </div>

    {#if showUnitWarning}
        <div class="mt-4 p-4 bg-yellow-100 border-l-4 border-yellow-500 text-yellow-700">
            <p class="font-bold">Warning</p>
            <p>This list contains multiple units. Please review carefully.</p>
        </div>
    {/if}

    {#if showUnitIngredientWarning}
        <div class="mt-4 p-4 bg-orange-100 border-l-4 border-orange-500 text-orange-700">
            <p class="font-bold">Warning</p>
            <p>This list contains multiple unit-ingredient combinations. Please review carefully.</p>
        </div>
    {/if}

    {#if hasMissingVMPs}
        <div class="mt-4 p-4 bg-red-50 border-l-4 border-red-500 text-red-700">
            <p class="font-bold">Warning: Missing quantities</p>
            <p class="mb-2">The chosen quantity for the following VMPs can't be calculated and are excluded from the analysis:</p>
            <ul class="list-disc list-inside">
                {#each missingVMPs as vmp}
                    <li>{vmp}</li>
                {/each}
            </ul>
        </div>
    {/if}
</div>