<svelte:options customElement={{
    tag: 'totals-table',
    shadow: 'none'
  }} />

<script>
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { resultsStore } from '../../../stores/resultsStore';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { formatNumber } from '../../../utils/utils';
    import { processTableDataByMode, getModeDisplayName, getTableExplainerText } from '../../../utils/analyseUtils';

    export let data = [];
    export let quantityType = 'SCMD Quantity';
    export let searchType = 'vmp';

    let selectedPeriod = 'all';
    let latestMonth = '';
    let latestYear = '';
    let currentFY = '';
    let last12MonthsRange = '';
    let latestDateFromData = null;
    let dateRange = '';
    $: selectedMode = $modeSelectorStore.selectedMode || searchType;

    $: {
        if (data?.length > 0) {
            const months = $resultsStore.analysisMonths || [];
            const allDates = months.map(m => new Date(m)).sort((a, b) => b - a);

            if (allDates.length > 0) {
                const latest = allDates[0];
                const earliest = allDates[allDates.length - 1];
                
                latestDateFromData = latest;
                
                const startDate = earliest.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                const endDate = latest.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                
                latestMonth = latest.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                latestYear = latest.getFullYear().toString();
                
                currentFY = 'FYTD';
                dateRange = `${startDate}-${endDate}`;
                
                const twelveMonthsAgo = new Date(latest);
                twelveMonthsAgo.setMonth(twelveMonthsAgo.getMonth() - 11);
                const start12Months = twelveMonthsAgo.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
                last12MonthsRange = `${start12Months} - ${latestMonth}`;
            }
        }
    }

    $: groupedData = processTableDataByMode(
        data, 
        selectedMode, 
        selectedPeriod, 
        $resultsStore.aggregatedData,
        latestDateFromData,
        $analyseOptions.selectedOrganisations || [],
        $organisationSearchStore.items || [],
        $resultsStore.analysisMonths || [],
        $organisationSearchStore.regionsHierarchy || []
    );

    $: titleText = selectedMode === 'national' 
        ? 'National total' 
        : `Total quantity by ${getModeDisplayName(selectedMode)}`;

    $: hasSelectedTrusts = $analyseOptions.selectedOrganisations && $analyseOptions.selectedOrganisations.length > 0;
    
    $: quantityColumnHeader = (() => {
        if (hasSelectedTrusts && selectedMode !== 'trust') {
            return 'Quantity (selected trusts)';
        } else {
            return 'Quantity';
        }
    })();

    $: tableExplainerText = getTableExplainerText(selectedMode, {
        hasSelectedTrusts: hasSelectedTrusts,
        selectedTrustsCount: $analyseOptions.selectedOrganisations?.length || 0,
        selectedPeriod: selectedPeriod,
        latestMonth: latestMonth,
        latestYear: latestYear,
        dateRange: dateRange
    });

</script>

<div class="p-4">
    <div class="flex flex-col mb-4">
        <div class="flex justify-between items-center">
            <div>
                <h3 class="text-xl font-semibold">
                    {titleText}
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
                            {selectedPeriod === 'last_12_months' 
                                ? 'bg-oxford-600 text-white' 
                                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
                        on:click={() => selectedPeriod = 'last_12_months'}
                    >
                        {last12MonthsRange}
                    </button>
                    <div class="absolute z-10 scale-0 transition-all duration-100 origin-bottom transform 
                                group-hover:scale-100 w-[200px] -translate-x-full left-full bottom-8 mb-1 
                                rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 p-4
                                max-w-[calc(100vw-2rem)]">
                        <p class="text-sm text-gray-500">
                            Last 12 months: {last12MonthsRange}
                        </p>
                    </div>
                </div>
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
    
    <div class="mb-4">
        <p class="text-sm text-gray-700">
            {tableExplainerText}
        </p>
    </div>
    
    <div class="overflow-x-auto">
        <div class="max-h-96 overflow-y-auto relative">
            <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
                <thead class="bg-gray-200 text-gray-600 text-sm leading-normal sticky top-0 z-10">
                    <tr>
                        {#if selectedMode !== 'national'}
                            <th class="py-3 px-6 text-left">
                                {getModeDisplayName(selectedMode)}
                            </th>
                        {/if}
                        {#if selectedMode !== 'unit'}
                            <th class="py-3 px-6 text-left">Unit</th>
                        {/if}
                        <th class="py-3 px-6 text-right">
                            {quantityColumnHeader}
                        </th>
                    </tr>
                </thead>
                <tbody class="text-gray-600 text-sm">
                    {#each groupedData as group}
                        {@const nonZeroUnits = group.units?.filter(u => (u.quantity || 0) > 0) ?? []}
                        {#if group.isNote}
                            <tr class="border-b border-gray-200">
                                {#if selectedMode !== 'national'}
                                    <td class="py-3 px-6 text-left text-gray-500 italic pl-12">
                                        {group.key}
                                    </td>
                                {/if}
                                {#if selectedMode !== 'unit'}
                                    <td class="py-3 px-6 text-left">-</td>
                                {/if}
                                <td class="py-3 px-6 text-right">-</td>
                            </tr>
                        {:else if quantityType === 'DDD'}
                            <tr class="border-b border-gray-200 hover:bg-gray-100 
                                {group.isSubtotal ? (group.section === 'selected' ? 'bg-green-50 font-semibold' : 'bg-blue-50 font-semibold') : ''}">
                                {#if selectedMode !== 'national'}
                                    <td class="py-3 px-6 text-left">
                                        <span>{group.key}</span>
                                    </td>
                                {/if}
                                {#if selectedMode !== 'unit'}
                                    <td class="py-3 px-6 text-left">
                                        {group.units[0]?.unit || 'DDD'}
                                    </td>
                                {/if}
                                <td class="py-3 px-6 text-right">
                                    {formatNumber(group.total)}
                                </td>
                            </tr>
                        {:else if nonZeroUnits.length <= 1}
                            {@const displayUnit = (nonZeroUnits[0] ?? group.units[0])?.unit || '-'}
                            <tr class="border-b border-gray-200 hover:bg-gray-100 
                                {group.isSubtotal ? (group.section === 'selected' ? 'bg-green-50 font-semibold' : 'bg-blue-50 font-semibold') : ''}">
                                {#if selectedMode !== 'national'}
                                    <td class="py-3 px-6 text-left">
                                        <span>{group.key}</span>
                                    </td>
                                {/if}
                                {#if selectedMode !== 'unit'}
                                    <td class="py-3 px-6 text-left">
                                        {displayUnit}
                                    </td>
                                {/if}
                                <td class="py-3 px-6 text-right">
                                    {formatNumber(nonZeroUnits[0]?.quantity ?? group.total, displayUnit)}
                                </td>
                            </tr>
                        {:else}
                            <tr class="border-b border-gray-200 hover:bg-gray-100 font-bold 
                                {group.isSubtotal ? (group.section === 'selected' ? 'bg-green-50 font-semibold' : 'bg-blue-50 font-semibold') : ''}">
                                {#if selectedMode !== 'national'}
                                    <td class="py-3 px-6 text-left" rowspan={nonZeroUnits.length + 1}>
                                        <span>{group.key}</span>
                                    </td>
                                {/if}
                                {#if selectedMode !== 'unit'}
                                    <td class="py-3 px-6 text-left">All units</td>
                                {/if}
                                <td class="py-3 px-6 text-right">
                                    {formatNumber(group.total)}
                                </td>
                            </tr>
                            {#each nonZeroUnits as unitData}
                                <tr class="border-b border-gray-200 hover:bg-gray-100">
                                    {#if selectedMode !== 'unit'}
                                        <td class="py-3 px-6 text-left">{unitData.unit}</td>
                                    {/if}
                                    <td class="py-3 px-6 text-right">
                                        {formatNumber(unitData.quantity, unitData.unit)}
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
