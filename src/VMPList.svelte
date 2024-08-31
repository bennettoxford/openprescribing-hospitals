<script>
    import { createEventDispatcher } from 'svelte';

    export let vmps = [];

    const dispatch = createEventDispatcher();

    $: hasIngredients = vmps.some(vmp => vmp.ingredient);
    
    let checkedVMPs = vmps.map(() => true);

    $: selectedCount = checkedVMPs.filter(Boolean).length;

    $: uniqueUnits = [...new Set(vmps.map(vmp => vmp.unit))];
    $: uniqueUnitIngredientPairs = [...new Set(vmps.map(vmp => `${vmp.unit}-${vmp.ingredient || ''}`))]
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

    $: sortedVMPs = [...vmps].sort((a, b) => {
        if (sortColumn === 'selected') {
            return sortDirection * (checkedVMPs[vmps.indexOf(b)] - checkedVMPs[vmps.indexOf(a)]);
        }
        let aValue = a[sortColumn] || '';
        let bValue = b[sortColumn] || '';
        return sortDirection * aValue.localeCompare(bValue, undefined, {numeric: true, sensitivity: 'base'});
    });

    $: {
        const selectedVMPs = vmps.filter((_, index) => checkedVMPs[index]);
        dispatch('dataFiltered', selectedVMPs);
    }
</script>

<div class="p-4">
    <h3 class="text-xl font-semibold mb-4">Products included</h3>
    <p class="mb-2 text-sm text-gray-600">
        Selected: <span class="font-semibold">{selectedCount}</span> out of <span class="font-semibold">{vmps.length}</span>
    </p>
    <div class="overflow-x-auto">
        <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
            <thead class="bg-gray-100">
                <tr>
                    <th class="px-4 py-2 text-left cursor-pointer" on:click={() => sortBy('vmp')}>
                        Name <span class="text-gray-400">{getSortIndicator('vmp')}</span>
                    </th>
                    <th class="px-4 py-2 text-left cursor-pointer" on:click={() => sortBy('unit')}>
                        Unit <span class="text-gray-400">{getSortIndicator('unit')}</span>
                    </th>
                    {#if hasIngredients}
                        <th class="px-4 py-2 text-left cursor-pointer" on:click={() => sortBy('ingredient')}>
                            Ingredient <span class="text-gray-400">{getSortIndicator('ingredient')}</span>
                        </th>
                    {/if}
                    <th class="px-4 py-2 text-left cursor-pointer" on:click={() => sortBy('selected')}>
                        Select <span class="text-gray-400">{getSortIndicator('selected')}</span>
                    </th>
                </tr>
            </thead>
            <tbody>
                {#each sortedVMPs as vmp, index}
                    <tr class="border-t border-gray-200">
                        <td class="px-4 py-2">{vmp.vmp}</td>
                        <td class="px-4 py-2">{vmp.unit}</td>
                        {#if hasIngredients}
                            <td class="px-4 py-2">{vmp.ingredient || '-'}</td>
                        {/if}
                        <td class="px-4 py-2">
                            <input 
                                type="checkbox" 
                                bind:checked={checkedVMPs[vmps.indexOf(vmp)]}
                                class="form-checkbox h-5 w-5 text-blue-600"
                            >
                        </td>
                    </tr>
                {/each}
            </tbody>
        </table>
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
</div>