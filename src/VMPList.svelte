<script>
    export let vmps = [];
    $: hasIngredients = vmps.some(vmp => vmp.ingredient);
    
    $: uniqueUnits = [...new Set(vmps.map(vmp => vmp.unit))];
    $: uniqueUnitIngredientPairs = [...new Set(vmps.map(vmp => `${vmp.unit}-${vmp.ingredient || ''}`))]
    $: showUnitWarning = uniqueUnits.length > 1;
    $: showUnitIngredientWarning = hasIngredients && uniqueUnitIngredientPairs.length > 1;
</script>

<div class="p-4">
    <h3 class="text-xl font-semibold mb-4">Products included</h3>
    <div class="overflow-x-auto">
        <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
            <thead class="bg-gray-100">
                <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Product</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Unit</th>
                    {#if hasIngredients}
                        <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Ingredient</th>
                    {/if}
                </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
                {#each vmps as { vmp, unit, ingredient }}
                    <tr class="hover:bg-gray-50">
                        <td class="px-6 py-4 whitespace-nowrap">{vmp}</td>
                        <td class="px-6 py-4 whitespace-nowrap">{unit}</td>
                        {#if hasIngredients}
                            <td class="px-6 py-4 whitespace-nowrap">{ingredient || ''}</td>
                        {/if}
                    </tr>
                {/each}
            </tbody>
        </table>
    </div>

    {#if showUnitWarning}
        <div class="mt-4 p-4 bg-yellow-100 border-l-4 border-yellow-500 text-yellow-700">
            <p class="font-bold">Warning</p>
            <p>The products included have more than one unit of measurement. The aggregate data below may be misleading. Consider analysing by ingredient quantity instead.</p>
        </div>
    {/if}

    {#if showUnitIngredientWarning}
        <div class="mt-4 p-4 bg-orange-100 border-l-4 border-orange-500 text-orange-700">
            <p class="font-bold">Warning</p>
            <p>This products included have more than one unit of measure and ingredient combination. The aggregate data below may be misleading.</p>
        </div>
    {/if}
</div>