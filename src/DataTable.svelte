<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    export let data = [];
    export let quantityType = 'Dose';

    $: groupedData = processData(data, quantityType);

    function processData(data, quantityType) {
        const groupedByGroup = data.reduce((acc, item) => {
            const groupKey = quantityType === 'Dose' ? item.unit : `${item.unit}-${item.ingredient_name || ''}`;
            
            if (!acc[groupKey]) {
                acc[groupKey] = 0;
            }
            
            acc[groupKey] += parseFloat(item.quantity);
            
            return acc;
        }, {});

        const sortedGroups = Object.entries(groupedByGroup)
            .sort(([aKey, aValue], [bKey, bValue]) => bValue - aValue)
            .map(([key, value]) => ({ key, value }));

        return sortedGroups;
    }
</script>

<div class="overflow-x-auto">
    <table class="min-w-full bg-white">
        <thead>
            <tr class="bg-gray-200 text-gray-600 uppercase text-sm leading-normal">
                <th class="py-3 px-6 text-left">Unit{quantityType === 'Dose' ? '' : '-Ingredient'}</th>
                <th class="py-3 px-6 text-right">Total {quantityType}</th>
            </tr>
        </thead>
        <tbody class="text-gray-600 text-sm font-light">
            {#each groupedData as group}
                <tr class="border-b border-gray-200 hover:bg-gray-100">
                    <td class="py-3 px-6 text-left whitespace-nowrap">
                        {group.key}
                    </td>
                    <td class="py-3 px-6 text-right font-bold">
                        {group.value.toFixed(2)}
                    </td>
                </tr>
            {/each}
        </tbody>
    </table>
</div>