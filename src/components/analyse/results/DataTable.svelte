<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    import { resultsStore } from '../../../stores/resultsStore';

    export let data = [];
    export let quantityType = 'Dose';
    export let searchType = 'vmp';

    $: data = $resultsStore.filteredData || [];
    $: quantityType = $resultsStore.quantityType;
    $: searchType = $resultsStore.searchType;

    $: groupedData = processData(data, quantityType, searchType);

    function processData(data, quantityType, searchType) {
        const groupedByGroup = data.reduce((acc, item) => {
            let groupKey;
            if (searchType === 'vmp') {
                groupKey = `${item.vmp_name}-${item.unit}`;
            } else if (searchType === 'vtm') {
                groupKey = item.vtm_name || 'Unknown VTM';
            } else if (searchType === 'ingredient') {
                groupKey = item.ingredient_names ? item.ingredient_names[0] : 'Unknown Ingredient';
            } else if (searchType === 'atc') {
                groupKey = `${item.atc_code} | ${item.atc_name}` || 'Unknown ATC';
            }
            
            if (!acc[groupKey]) {
                acc[groupKey] = {
                    total: 0,
                    units: {}
                };
            }
            
            acc[groupKey].total += parseFloat(item.quantity);
            
            if (!acc[groupKey].units[item.unit]) {
                acc[groupKey].units[item.unit] = 0;
            }
            acc[groupKey].units[item.unit] += parseFloat(item.quantity);
            
            return acc;
        }, {});

        const sortedGroups = Object.entries(groupedByGroup)
            .sort(([aKey, aValue], [bKey, bValue]) => bValue.total - aValue.total)
            .map(([key, value]) => ({ 
                key, 
                total: value.total,
                units: Object.entries(value.units)
                    .sort(([aKey, aValue], [bKey, bValue]) => bValue - aValue)
                    .map(([unit, quantity]) => ({ unit, quantity }))
            }));

        return sortedGroups;
    }

    function formatNumber(number) {
        return Math.round(number).toLocaleString('en-US');
    }
</script>

<div class="p-4">
    <h3 class="text-xl font-semibold mb-4">Total {quantityType} by {searchType.toUpperCase()}</h3>
    <div class="overflow-x-auto">
        <div class="max-h-96 overflow-y-auto relative">
            <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
                <thead class="bg-gray-200 text-gray-600 uppercase text-sm leading-normal sticky top-0 z-10">
                    <tr>
                        <th class="py-3 px-6 text-left">{searchType.toUpperCase()} Name</th>
                        {#if searchType !== 'vmp'}
                            <th class="py-3 px-6 text-left">Unit</th>
                        {/if}
                        <th class="py-3 px-6 text-right">Total {quantityType}</th>
                    </tr>
                </thead>
                <tbody class="text-gray-600 text-sm">
                    {#each groupedData as group}
                        <tr class="border-b border-gray-200 hover:bg-gray-100 font-bold">
                            <td class="py-3 px-6 text-left" rowspan={group.units.length + 1}>
                                {group.key}
                            </td>
                            {#if searchType !== 'vmp'}
                                <td class="py-3 px-6 text-left">All Units</td>
                            {/if}
                            <td class="py-3 px-6 text-right">
                                {formatNumber(group.total)}
                            </td>
                        </tr>
                        {#each group.units as unitData}
                            {#if searchType !== 'vmp'}
                                <tr class="border-b border-gray-200 hover:bg-gray-100">
                                    <td class="py-3 px-6 text-left">{unitData.unit}</td>
                                    <td class="py-3 px-6 text-right">
                                        {formatNumber(unitData.quantity)}
                                    </td>
                                </tr>
                            {/if}
                        {/each}
                    {/each}
                </tbody>
            </table>
        </div>
    </div>
</div>
