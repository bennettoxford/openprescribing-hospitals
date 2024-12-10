<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';

    export let data = [];
    export let quantityType = 'Dose';
    export let searchType = 'vmp';

    let selectedPeriod = 'all'; // 'all', 'year', 'month', or 'fytd'
    let latestYearAvailable = false;
    let latestMonthAvailable = false;
    let currentFYAvailable = false;

    $: data = $resultsStore.filteredData || [];
    $: quantityType = $resultsStore.quantityType;
    $: searchType = $resultsStore.searchType;
    $: dateRange = $analyseOptions.dateRange;

    $: {
        if (dateRange?.startDate && dateRange?.endDate) {
            const start = new Date(dateRange.startDate);
            const end = new Date(dateRange.endDate);
            
            // Check for year availability
            const monthsDiff = (end.getFullYear() - start.getFullYear()) * 12 + 
                             (end.getMonth() - start.getMonth());
            latestYearAvailable = monthsDiff >= 12;

            // Check for month availability
            latestMonthAvailable = monthsDiff >= 1;

            // Check for financial year to date availability
            const currentDate = new Date(end);
            const currentFYStart = new Date(currentDate.getFullYear(), 3, 1); // April 1st
            if (currentDate < currentFYStart) {
                currentFYStart.setFullYear(currentFYStart.getFullYear() - 1);
            }
            
            // Only show FYTD option if:
            // 1. The selected period includes at least one April
            // 2. The start date is before or equal to the current FY start
            const hasApril = Array.from({ length: monthsDiff + 1 }, (_, i) => {
                const date = new Date(start);
                date.setMonth(date.getMonth() + i);
                return date.getMonth() === 3; // 3 is April (0-based months)
            }).some(Boolean);
            
            currentFYAvailable = hasApril && start <= currentFYStart;
        }
    }

    $: groupedData = processData(data, quantityType, searchType, selectedPeriod);

    function processData(data, quantityType, searchType, period) {
        if (!data.length) return [];

        let filteredData = data;
        
        if (period !== 'all') {
            const endDate = new Date(dateRange.endDate);
            let startDate;

            switch (period) {
                case 'year':
                    startDate = new Date(endDate);
                    startDate.setFullYear(endDate.getFullYear() - 1);
                    break;
                case 'month':
                    startDate = new Date(endDate);
                    startDate.setMonth(endDate.getMonth() - 1);
                    break;
                case 'fytd':
                    startDate = new Date(endDate.getFullYear(), 3, 1); // April 1st
                    if (endDate < startDate) {
                        startDate.setFullYear(startDate.getFullYear() - 1);
                    }
                    break;
            }

            filteredData = data.filter(item => {
                const itemDate = new Date(item.year_month);
                return itemDate >= startDate && itemDate <= endDate;
            });
        }

        if (searchType === 'product') {
            const groupedByVTM = filteredData.reduce((acc, item) => {
                const vtmKey = item.vtm_name || 'Unknown Product group';
                if (!acc[vtmKey]) {
                    acc[vtmKey] = {
                        total: 0,
                        units: {}
                    };
                }

                const quantity = parseFloat(item.quantity) || 0;
                acc[vtmKey].total += quantity;

                if (!acc[vtmKey].units[item.unit]) {
                    acc[vtmKey].units[item.unit] = 0;
                }
                acc[vtmKey].units[item.unit] += quantity;

                return acc;
            }, {});

            return Object.entries(groupedByVTM)
                .sort(([, a], [, b]) => b.total - a.total)
                .map(([vtmName, data]) => ({
                    key: vtmName,
                    total: data.total,
                    units: Object.entries(data.units)
                        .sort(([, a], [, b]) => b - a)
                        .map(([unit, quantity]) => ({ unit, quantity }))
                }));
        }
        
        // For other search types (ingredient), use original grouping
        const groupedByType = filteredData.reduce((acc, item) => {
            let groupKey;
            groupKey = item.ingredient_name || 'Unknown Ingredient';

            if (!acc[groupKey]) {
                acc[groupKey] = {
                    total: 0,
                    units: {}
                };
            }

            const quantity = parseFloat(item.quantity) || 0;
            acc[groupKey].total += quantity;

            if (!acc[groupKey].units[item.unit]) {
                acc[groupKey].units[item.unit] = 0;
            }
            acc[groupKey].units[item.unit] += quantity;

            return acc;
        }, {});

        return Object.entries(groupedByType)
            .sort(([, a], [, b]) => b.total - a.total)
            .map(([key, value]) => ({
                key,
                total: value.total,
                units: Object.entries(value.units)
                    .sort(([, a], [, b]) => b - a)
                    .map(([unit, quantity]) => ({ unit, quantity }))
            }));
    }

    function formatNumber(number) {
        return Math.round(number).toLocaleString('en-US');
    }

    function formatDate(dateStr) {
        if (!dateStr) return '';
        return new Date(dateStr).toLocaleDateString('en-GB', { 
            year: 'numeric', 
            month: 'short'
        });
    }

   

    $: {
        if (data.length > 0) {
            console.log('Sample of incoming data:', {
                total: data.length,
                firstItem: data[0],
                lastItem: data[data.length - 1],
                dateRange
            });
        }
    }
</script>

<div class="p-4">
    <div class="flex flex-col mb-4">
        <div class="flex justify-between items-center">
            <div>
                <h3 class="text-xl font-semibold">
                    Total {quantityType} by {searchType === 'vmp' || searchType === 'vtm' ? 'product' : searchType}
                </h3>
                {#if dateRange?.startDate && dateRange?.endDate}
                    <p class="text-sm text-gray-500 mt-1">
                        {#if selectedPeriod === 'all'}
                            Totals calculated between {formatDate(dateRange.startDate)} and {formatDate(dateRange.endDate)}
                        {:else if selectedPeriod === 'year'}
                            Totals calculated between {formatDate(new Date(dateRange.endDate).setFullYear(new Date(dateRange.endDate).getFullYear() - 1))} and {formatDate(dateRange.endDate)}
                        {:else if selectedPeriod === 'month'}
                            Total for {formatDate(dateRange.endDate)}
                        {:else if selectedPeriod === 'fytd'}
                            {#if new Date(dateRange.endDate).getMonth() >= 3}
                                Totals calculated between {formatDate(new Date(new Date(dateRange.endDate).getFullYear(), 3, 1))} and {formatDate(dateRange.endDate)}
                            {:else}
                                Totals calculated between {formatDate(new Date(new Date(dateRange.endDate).getFullYear() - 1, 3, 1))} and {formatDate(dateRange.endDate)}
                            {/if}
                        {/if}
                    </p>
                {/if}
            </div>
            <div class="flex items-center space-x-2">
                <select 
                    id="period-select"
                    bind:value={selectedPeriod}
                    class="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-oxford-500"
                >
                    <option value="all">Entire period</option>
                    {#if latestMonthAvailable}
                        <option value="month">Latest month</option>
                    {/if}
                    {#if latestYearAvailable}
                        <option value="year">Latest year</option>
                    {/if}
                    {#if currentFYAvailable}
                        <option value="fytd">Financial year to date</option>
                    {/if}
                </select>
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
                                
                            {/if}
                        </th>
                        <th class="py-3 px-6 text-left">Unit</th>
                        <th class="py-3 px-6 text-right">Quantity</th>
                    </tr>
                </thead>
                <tbody class="text-gray-600 text-sm">
                    {#each groupedData as group}
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
                    {/each}
                </tbody>
            </table>
        </div>
    </div>
</div>
