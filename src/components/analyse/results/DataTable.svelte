<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    import { analyseOptions } from '../../../stores/analyseOptionsStore';

    export let data = [];
    export let quantityType = 'VMP Quantity';
    export let searchType = 'vmp';

    let selectedPeriod = 'all';
    let latestMonth = '';
    let latestYear = '';
    let currentFY = '';

    $: dateRange = $analyseOptions.dateRange;

    // Calculate available periods when data changes
    $: {
        if (data?.length > 0) {
            // Get all dates from all items
            const allDates = data.flatMap(item => 
                item.data.map(([date]) => new Date(date))
            ).sort((a, b) => b - a); // Sort descending

            if (allDates.length > 0) {
                const latest = allDates[0];
                const earliest = allDates[allDates.length - 1];
                
                const startDate = earliest.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                const endDate = latest.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                
                latestMonth = latest.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                latestYear = latest.getFullYear().toString();
                
                const fyStart = latest.getMonth() >= 3 ? latest.getFullYear() : latest.getFullYear() - 1;
                currentFY = 'FYTD';
        
                dateRange = `${startDate}-${endDate}`;
            }
        }
    }

    function processData(data, quantityType, searchType, period) {
        if (!data?.length) return [];

        let filteredData = data.map(item => ({
            vtm_name: item.vmp__vtm__name || 'Unknown VTM',
            vmp_name: item.vmp__name || 'Unknown VMP',
            ingredient_name: item.vmp__vtm__name || 'Unknown Ingredient',
            data: item.data || []
        }));

        // Determine the latest month from the data
        const allDates = filteredData.flatMap(item => 
            item.data.map(([date]) => new Date(date))
        ).sort((a, b) => b - a); // Sort descending

        const latestDate = allDates[0];
        const latestMonth = latestDate ? latestDate.getMonth() : null;
        const latestYear = latestDate ? latestDate.getFullYear() : null;

        // Filter data based on selected period
        filteredData = filteredData.map(item => ({
            ...item,
            data: item.data.filter(([date]) => {
                const dataDate = new Date(date);

                switch (period) {
                    case 'latest_month':
                        return dataDate.getMonth() === latestMonth &&
                               dataDate.getFullYear() === latestYear;
                    case 'latest_year':
                        return dataDate.getFullYear() === latestYear;
                    case 'current_fy':
                        const fyStart = new Date(latestYear, 3, 1); // April 1st
                        if (latestDate < fyStart) {
                            fyStart.setFullYear(fyStart.getFullYear() - 1);
                        }
                        return dataDate >= fyStart && dataDate <= latestDate;
                    default: // 'all'
                        return true;
                }
            })
        }));

        // Group by the appropriate type
        const groupedData = filteredData.reduce((acc, item) => {
            const key = searchType === 'product' ? item.vtm_name : 
                       searchType === 'vmp' ? item.vmp_name : 
                       item.ingredient_name;
            
            if (!acc[key]) {
                acc[key] = {
                    total: 0,
                    units: {}
                };
            }

            // Process all data points
            item.data.forEach(([date, quantity, unit]) => {
                const parsedQuantity = parseFloat(quantity) || 0;
                acc[key].total += parsedQuantity;

                if (!acc[key].units[unit]) {
                    acc[key].units[unit] = 0;
                }
                acc[key].units[unit] += parsedQuantity;
            });

            return acc;
        }, {});

        return Object.entries(groupedData)
            .sort(([, a], [, b]) => b.total - a.total)
            .map(([key, value]) => ({
                key,
                total: value.total,
                units: Object.entries(value.units)
                    .sort(([, a], [, b]) => b - a)
                    .map(([unit, quantity]) => ({ unit, quantity }))
            }));
    }

    $: groupedData = processData(data, quantityType, searchType, selectedPeriod);

    function formatNumber(number) {
        return Math.round(number).toLocaleString('en-GB');
    }

    function formatDate(dateStr) {
        if (!dateStr) return '';
        return new Date(dateStr).toLocaleDateString('en-GB', { 
            year: 'numeric', 
            month: 'short'
        });
    }
</script>

<div class="p-4">
    <div class="flex flex-col mb-4">
        <div class="flex justify-between items-center">
            <div>
                <h3 class="text-xl font-semibold">
                    Total {quantityType} by {searchType === 'vmp' || searchType === 'vtm' ? 'product' : searchType}
                </h3>
            </div>
            <div class="flex flex-wrap gap-2">
                <button
                    class="px-3 py-1 rounded-full text-sm font-medium transition-colors
                        {selectedPeriod === 'all' 
                            ? 'bg-oxford-600 text-white' 
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
                    on:click={() => selectedPeriod = 'all'}
                >
                    {dateRange}
                </button>
                <button
                    class="px-3 py-1 rounded-full text-sm font-medium transition-colors
                        {selectedPeriod === 'latest_month' 
                            ? 'bg-oxford-600 text-white' 
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
                    on:click={() => selectedPeriod = 'latest_month'}
                >
                    {latestMonth}
                </button>
                <button
                    class="px-3 py-1 rounded-full text-sm font-medium transition-colors
                        {selectedPeriod === 'latest_year' 
                            ? 'bg-oxford-600 text-white' 
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
                    on:click={() => selectedPeriod = 'latest_year'}
                >
                    {latestYear}
                </button>
                <div class="relative inline-block group">
                    <button
                        class="px-3 py-1 rounded-full text-sm font-medium transition-colors
                            {selectedPeriod === 'current_fy' 
                                ? 'bg-oxford-600 text-white' 
                                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
                        on:click={() => selectedPeriod = 'current_fy'}
                    >
                        {currentFY}
                    </button>
                    <div class="absolute z-10 scale-0 transition-all duration-100 origin-bottom transform 
                                group-hover:scale-100 w-[200px] -translate-x-full left-full bottom-8 mb-1 
                                rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 p-4
                                max-w-[calc(100vw-2rem)]">
                        <p class="text-sm text-gray-500">
                            Financial Year to Date: April {latestMonth ? 
                                new Date(latestMonth).getMonth() >= 3 ? 
                                    new Date(latestMonth).getFullYear() : 
                                    new Date(latestMonth).getFullYear() - 1
                                : ''} - {latestMonth}
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div class="overflow-x-auto">
        <div class="max-h-96 overflow-y-auto relative">
            <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
                <thead class="bg-gray-200 text-gray-600 text-sm leading-normal sticky top-0 z-10">
                    <tr>
                        <th class="py-3 px-6 text-left">
                            {#if searchType === 'product'}
                                Product group
                            {:else if searchType === 'ingredient'}
                                Ingredient
                            {:else}
                                Product
                            {/if}
                        </th>
                        <th class="py-3 px-6 text-left">Unit</th>
                        <th class="py-3 px-6 text-right">Quantity</th>
                    </tr>
                </thead>
                <tbody class="text-gray-600 text-sm">
                    {#each groupedData as group}
                        {#if quantityType === 'DDD'}
                            <tr class="border-b border-gray-200 hover:bg-gray-100">
                                <td class="py-3 px-6 text-left">
                                    {group.key}
                                </td>
                                <td class="py-3 px-6 text-left">
                                    {group.units[0]?.unit || 'DDD'}
                                </td>
                                <td class="py-3 px-6 text-right">
                                    {formatNumber(group.total)}
                                </td>
                            </tr>
                        {:else}
                            <tr class="border-b border-gray-200 hover:bg-gray-100 font-bold">
                                <td class="py-3 px-6 text-left" rowspan={group.units.length + 1}>
                                    {group.key}
                                </td>
                                <td class="py-3 px-6 text-left">All units</td>
                                <td class="py-3 px-6 text-right">
                                    {formatNumber(group.total)}
                                </td>
                            </tr>
                            {#each group.units as unitData}
                                <tr class="border-b border-gray-200 hover:bg-gray-100">
                                    <td class="py-3 px-6 text-left">{unitData.unit}</td>
                                    <td class="py-3 px-6 text-right">
                                        {formatNumber(unitData.quantity)}
                                    </td>
                                </tr>
                            {/each}
                        {/if}
                    {/each}
                </tbody>
            </table>
        </div>
    </div>
</div>
