<script>
    export let vmps = [];
    $: hasIngredients = vmps.some(vmp => vmp.ingredient);
</script>

<div class="p-4">
    <h3 class="text-xl font-semibold mb-4">VMPs:</h3>
    <div class="overflow-x-auto">
        <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
            <thead class="bg-gray-100">
                <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">VMP</th>
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
</div>