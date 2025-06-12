<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { formatNumber } from '../../../utils/utils';

    export let data = [];
    export let quantityType = 'VMP Quantity';
    export let selectedOrganisations = [];

    let selectedPeriod = 'all';
    let latestMonth = '';
    let latestYear = '';
    let currentFY = '';
    let expandedTrusts = new Set();

    $: dateRange = $analyseOptions.dateRange;
    $: selectedOrgs = selectedOrganisations || [];
    $: currentMode = $modeSelectorStore.selectedMode || 'product';

    $: {
        if (data?.length > 0) {
            const allDates = data.flatMap(item => 
                item.data.map(([date]) => new Date(date))
            ).sort((a, b) => b - a);

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

    // For non-organisation modes, filter to selected trusts
    $: filteredData = currentMode !== 'organisation' && selectedOrgs.length > 0
        ? data.filter(item => selectedOrgs.includes(item.organisation__ods_name))
        : [];

    function toggleTrustExpansion(trustKey) {
        expandedTrusts = new Set(expandedTrusts);
        if (expandedTrusts.has(trustKey)) {
            expandedTrusts.delete(trustKey);
        } else {
            expandedTrusts.add(trustKey);
        }
    }

    const DISPLAY_LABELS = {
        'total': 'national',
        'organisation': 'NHS trust', 
        'product': 'product',
        'productGroup': 'product group',
        'ingredient': 'ingredient',
        'unit': 'unit',
        'region': 'region',
        'icb': 'ICB'
    };

    function getDisplayLabel(mode) {
        return DISPLAY_LABELS[mode] || 'item';
    }

    function getGroupingKey(item, mode) {
        const mappings = {
            'total': () => 'Total',
            'organisation': () => item.organisation__ods_name || 'Unknown Trust',
            'product': () => item.vmp__name || 'Unknown Product',
            'productGroup': () => item.vmp__vtm__name || 'Unknown Product Group',
            'ingredient': () => {
                if (Array.isArray(item.ingredient_names) && item.ingredient_names.length > 0) {
                    return item.ingredient_names[0];
                }
                return item.vmp__vtm__name || 'Unknown Ingredient';
            },
            'unit': () => {
                if (Array.isArray(item.data) && item.data.length > 0) {
                    return item.data[0][2] || 'Unknown Unit';
                }
                return 'Unknown Unit';
            },
            'region': () => item.organisation__region || 'Unknown Region',
            'icb': () => item.organisation__icb || 'Unknown ICB'
        };
        
        return mappings[mode] ? mappings[mode]() : item.vmp__name || 'Unknown';
    }

    function processTrustData(data, mode, period) {
        if (!data?.length) return {};

        let filteredData = data.map(item => ({
            ...item,
            vtm_name: item.vmp__vtm__name || 'Unknown VTM',
            vmp_name: item.vmp__name || 'Unknown VMP',
            ingredient_name: item.vmp__vtm__name || 'Unknown Ingredient',
            trust_name: item.organisation__ods_name || 'Unknown Trust',
            data: item.data || []
        }));

        const allDates = filteredData.flatMap(item => 
            item.data.map(([date]) => new Date(date))
        ).sort((a, b) => b - a);

        const latestDate = allDates[0];
        const latestMonth = latestDate ? latestDate.getMonth() : null;
        const latestYear = latestDate ? latestDate.getFullYear() : null;

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
                        const fyStart = new Date(latestYear, 3, 1);
                        if (latestDate < fyStart) {
                            fyStart.setFullYear(fyStart.getFullYear() - 1);
                        }
                        return dataDate >= fyStart && dataDate <= latestDate;
                    default:
                        return true;
                }
            })
        }));

        // Group by the selected mode and then by trust
        const groupedData = filteredData.reduce((acc, item) => {
            const groupKey = getGroupingKey(item, mode);
            
            if (!acc[groupKey]) {
                acc[groupKey] = {};
            }
            
            if (!acc[groupKey][item.trust_name]) {
                acc[groupKey][item.trust_name] = {
                    total: 0,
                    units: {}
                };
            }

            item.data.forEach(([date, quantity, unit]) => {
                const parsedQuantity = parseFloat(quantity) || 0;
                acc[groupKey][item.trust_name].total += parsedQuantity;

                if (!acc[groupKey][item.trust_name].units[unit]) {
                    acc[groupKey][item.trust_name].units[unit] = 0;
                }
                acc[groupKey][item.trust_name].units[unit] += parsedQuantity;
            });

            return acc;
        }, {});

        const result = {};
        Object.entries(groupedData).forEach(([groupKey, trusts]) => {
            result[groupKey] = Object.entries(trusts)
                .sort(([, a], [, b]) => b.total - a.total)
                .map(([trustName, data]) => ({
                    trustName,
                    total: data.total,
                    units: Object.entries(data.units)
                        .sort(([, a], [, b]) => b - a)
                        .map(([unit, quantity]) => ({ unit, quantity }))
                }));
        });

        return result;
    }

    function processData(data, mode, period) {
        if (!data?.length) return [];

        let filteredData = data.map(item => ({
            ...item,
            vtm_name: item.vmp__vtm__name || 'Unknown VTM',
            vmp_name: item.vmp__name || 'Unknown VMP',
            ingredient_name: item.vmp__vtm__name || 'Unknown Ingredient',
            data: item.data || []
        }));

        const allDates = filteredData.flatMap(item => 
            item.data.map(([date]) => new Date(date))
        ).sort((a, b) => b - a);

        const latestDate = allDates[0];
        const latestMonth = latestDate ? latestDate.getMonth() : null;
        const latestYear = latestDate ? latestDate.getFullYear() : null;

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
                        const fyStart = new Date(latestYear, 3, 1);
                        if (latestDate < fyStart) {
                            fyStart.setFullYear(fyStart.getFullYear() - 1);
                        }
                        return dataDate >= fyStart && dataDate <= latestDate;
                    default:
                        return true;
                }
            })
        }));

        const groupedData = filteredData.reduce((acc, item) => {
            const key = getGroupingKey(item, mode);
            
            if (!acc[key]) {
                acc[key] = {
                    total: 0,
                    units: {},
                    isSelected: false
                };
            }

            if (mode === 'organisation') {
                acc[key].isSelected = selectedOrgs.includes(key);
            }

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

        let result = Object.entries(groupedData)
            .map(([key, value]) => ({
                key,
                total: value.total,
                isSelected: value.isSelected || false,
                units: Object.entries(value.units)
                    .sort(([, a], [, b]) => b - a)
                    .map(([unit, quantity]) => ({ unit, quantity }))
            }));

        // For organisation mode, sort selected trusts to the top
        if (mode === 'organisation') {
            result = result.sort((a, b) => {
                // First sort by selection status (selected first)
                if (a.isSelected && !b.isSelected) return -1;
                if (!a.isSelected && b.isSelected) return 1;
                // Then sort by total quantity within each group
                return b.total - a.total;
            });
        } else {
            // For other modes, just sort by total
            result = result.sort((a, b) => b.total - a.total);
        }

        return result;
    }

    function processTrustModeData(data, period) {
        if (!data?.length) return { selectedTrusts: [], otherTrusts: [] };

        let filteredData = data.map(item => ({
            ...item,
            trust_name: item.organisation__ods_name || 'Unknown Trust',
            data: item.data || []
        }));

        const allDates = filteredData.flatMap(item => 
            item.data.map(([date]) => new Date(date))
        ).sort((a, b) => b - a);

        const latestDate = allDates[0];
        const latestMonth = latestDate ? latestDate.getMonth() : null;
        const latestYear = latestDate ? latestDate.getFullYear() : null;

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
                        const fyStart = new Date(latestYear, 3, 1);
                        if (latestDate < fyStart) {
                            fyStart.setFullYear(fyStart.getFullYear() - 1);
                        }
                        return dataDate >= fyStart && dataDate <= latestDate;
                    default:
                        return true;
                }
            })
        }));

        const groupedData = filteredData.reduce((acc, item) => {
            const trustName = item.trust_name;
            
            if (!acc[trustName]) {
                acc[trustName] = {
                    total: 0,
                    units: {},
                    isSelected: selectedOrgs.includes(trustName)
                };
            }

            item.data.forEach(([date, quantity, unit]) => {
                const parsedQuantity = parseFloat(quantity) || 0;
                acc[trustName].total += parsedQuantity;

                if (!acc[trustName].units[unit]) {
                    acc[trustName].units[unit] = 0;
                }
                acc[trustName].units[unit] += parsedQuantity;
            });

            return acc;
        }, {});

        const allAvailableOrgs = Array.from($organisationSearchStore.availableItems || []);
        
        allAvailableOrgs.forEach(orgName => {
            if (!groupedData[orgName]) {
                groupedData[orgName] = {
                    total: 0,
                    units: { '-': 0 },
                    isSelected: selectedOrgs.includes(orgName)
                };
            }
        });

        const predecessorMap = $organisationSearchStore.predecessorMap;
        const processedTrusts = new Set();
        const trustsWithRelationships = [];

        Object.entries(groupedData).forEach(([trustName, trustData]) => {
            if (processedTrusts.has(trustName)) return;

            const predecessors = predecessorMap.get(trustName) || [];
            const relatedOrgs = [trustName, ...predecessors];
            
            let combinedTotal = 0;
            let combinedUnits = {};
            const predecessorData = [];

            relatedOrgs.forEach(orgName => {
                if (groupedData[orgName]) {
                    const orgData = groupedData[orgName];
                    combinedTotal += orgData.total;

                    Object.entries(orgData.units).forEach(([unit, quantity]) => {
                        if (!combinedUnits[unit]) {
                            combinedUnits[unit] = 0;
                        }
                        combinedUnits[unit] += quantity;
                    });

                    if (orgName !== trustName && predecessors.includes(orgName)) {
                        predecessorData.push({
                            name: orgName,
                            total: orgData.total,
                            units: Object.entries(orgData.units)
                                .sort(([, a], [, b]) => b - a)
                                .map(([unit, quantity]) => ({ unit, quantity }))
                        });
                    }

                    processedTrusts.add(orgName);
                }
            });

            trustsWithRelationships.push({
                key: trustName,
                total: combinedTotal,
                isSelected: trustData.isSelected,
                units: Object.entries(combinedUnits)
                    .sort(([, a], [, b]) => b - a)
                    .map(([unit, quantity]) => ({ unit, quantity })),
                predecessors: predecessorData
            });
        });

        const selectedTrusts = trustsWithRelationships
            .filter(trust => trust.isSelected)
            .sort((a, b) => b.total - a.total);

        const otherTrusts = trustsWithRelationships
            .filter(trust => !trust.isSelected)
            .sort((a, b) => b.total - a.total);

        return { selectedTrusts, otherTrusts };
    }

    $: groupedDataAll = processData(data, currentMode, selectedPeriod);

    $: groupedDataSelected = currentMode === 'organisation' 
        ? []
        : processData(filteredData, currentMode, selectedPeriod);

    $: trustLevelData = currentMode === 'organisation' 
        ? {}
        : selectedOrgs.length > 0 
        ? processTrustData(filteredData, currentMode, selectedPeriod) 
        : {};

    $: trustModeData = currentMode === 'organisation' 
        ? processTrustModeData(data, selectedPeriod)
        : { selectedTrusts: [], otherTrusts: [] };

    $: mergedData = currentMode === 'organisation' 
        ? groupedDataAll.map(row => ({
            ...row,
            selectedTotal: null,
            selectedUnits: null,
            trustData: []
        }))
        : groupedDataAll.map(allRow => {
            const selectedRow = groupedDataSelected.find(sel => sel.key === allRow.key);
            return {
                ...allRow,
                selectedTotal: selectedRow ? selectedRow.total : null,
                selectedUnits: selectedRow ? selectedRow.units : null,
                trustData: trustLevelData[allRow.key] || []
            };
        });

    $: selectedTrustsTotal = currentMode === 'organisation' && trustModeData.selectedTrusts.length > 0
        ? trustModeData.selectedTrusts.reduce((sum, trust) => sum + trust.total, 0)
        : 0;

    $: selectedTrustsMainUnit = currentMode === 'organisation' && trustModeData.selectedTrusts.length > 0
        ? trustModeData.selectedTrusts[0]?.units[0]?.unit || '-'
        : '-';

    $: selectedTrustsCountWithPredecessors = currentMode === 'organisation' && trustModeData.selectedTrusts.length > 0
        ? trustModeData.selectedTrusts.reduce((count, trust) => count + 1 + trust.predecessors.length, 0)
        : 0;

    $: otherTrustsTotal = currentMode === 'organisation' && trustModeData.otherTrusts.length > 0
        ? trustModeData.otherTrusts.reduce((sum, trust) => sum + trust.total, 0)
        : 0;

    $: otherTrustsMainUnit = currentMode === 'organisation' && trustModeData.otherTrusts.length > 0
        ? trustModeData.otherTrusts[0]?.units[0]?.unit || '-'
        : '-';

    $: otherTrustsCountWithPredecessors = currentMode === 'organisation' && trustModeData.otherTrusts.length > 0
        ? trustModeData.otherTrusts.reduce((count, trust) => count + 1 + trust.predecessors.length, 0)
        : 0;

</script>

<div class="p-4">
    <div class="flex flex-col mb-4">
        <div class="flex justify-between items-center">
            <div>
                <h3 class="text-xl font-semibold">
                    {#if currentMode === 'total'}
                        National total
                    {:else}
                        Total quantity by {getDisplayLabel(currentMode)}
                    {/if}
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
    
    <div class="mb-8">
        <div class="overflow-x-auto">
            <div class="max-h-96 overflow-y-auto relative">
                <table class="min-w-full bg-white border border-gray-300 shadow-sm rounded-lg overflow-hidden">
                    <thead class="bg-gray-200 text-gray-600 text-sm leading-normal sticky top-0 z-10">
                        <tr>
                            {#if currentMode !== 'total'}
                                <th class="py-3 px-6 text-left">
                                    {DISPLAY_LABELS[currentMode]?.charAt(0).toUpperCase() + DISPLAY_LABELS[currentMode]?.slice(1) || 'Product'}
                                </th>
                            {/if}
                            <th class="py-3 px-6 text-left">Unit</th>
                            {#if ['product', 'productGroup', 'ingredient', 'unit'].includes(currentMode) && selectedOrgs.length > 0}
                                <th class="py-3 px-6 text-right">Selected Trusts</th>
                            {/if}
                            <th class="py-3 px-6 text-right">
                                {#if currentMode === 'organisation'}
                                    Total Quantity
                                {:else}
                                    {#if ['product', 'productGroup', 'ingredient', 'unit'].includes(currentMode) && selectedOrgs.length > 0}
                                        Total Quantity
                                    {:else}
                                        Quantity
                                    {/if}
                                {/if}
                            </th>
                        </tr>
                    </thead>
                    <tbody class="text-gray-600 text-sm">
                        {#if currentMode === 'organisation'}
                            {#if trustModeData.selectedTrusts.length > 0}
                                <tr class="bg-blue-100 border-b border-blue-200">
                                    <td class="py-2 px-6 text-left font-semibold text-blue-800">
                                        Selected Trusts ({selectedTrustsCountWithPredecessors})
                                    </td>
                                    <td class="py-2 px-6 text-left font-semibold text-blue-800">{selectedTrustsMainUnit}</td>
                                    <td class="py-2 px-6 text-right font-bold text-blue-900">
                                        {#if quantityType === 'DDD'}
                                            {formatNumber(selectedTrustsTotal)}
                                        {:else if !$analyseOptions.isAdvancedMode}
                                            {formatNumber(selectedTrustsTotal, selectedTrustsMainUnit)}
                                        {:else}
                                            {formatNumber(selectedTrustsTotal)}
                                        {/if}
                                    </td>
                                </tr>
                                
                                {#each trustModeData.selectedTrusts as trust}
                                    {#if quantityType === 'DDD'}
                                        <tr class="bg-blue-50 border-b {expandedTrusts.has(trust.key) ? 'border-transparent' : 'border-blue-200'}">
                                            <td class="py-3 px-6 text-left pl-8">
                                                <div>
                                                    <div>{trust.key}</div>
                                                    {#if trust.predecessors.length > 0}
                                                        <div class="flex items-center gap-2 mt-1">
                                                            <button
                                                                class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                                on:click={() => toggleTrustExpansion(trust.key)}
                                                                title="Show/hide predecessors"
                                                            >
                                                                <svg 
                                                                    class="w-3 h-3 transition-transform duration-200 {expandedTrusts.has(trust.key) ? 'rotate-90' : ''}" 
                                                                    fill="none" 
                                                                    stroke="currentColor" 
                                                                    viewBox="0 0 24 24"
                                                                >
                                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                                                                </svg>
                                                            </button>
                                                            <span class="text-xs text-gray-500">
                                                                {trust.predecessors.length} predecessor{trust.predecessors.length !== 1 ? 's' : ''}
                                                            </span>
                                                        </div>
                                                    {/if}
                                                </div>
                                            </td>
                                            <td class="py-3 px-6 text-left">{trust.units[0]?.unit || 'DDD'}</td>
                                            <td class="py-3 px-6 text-right">
                                                {formatNumber(trust.total)}
                                            </td>
                                        </tr>
                                        {#if expandedTrusts.has(trust.key)}
                                            {#each trust.predecessors as predecessor, index}
                                                <tr class="bg-blue-50 border-b {index === trust.predecessors.length - 1 ? 'border-blue-200' : 'border-transparent'}">
                                                    <td class="py-2 px-6 text-left pl-12 text-sm text-gray-600">
                                                        <div class="flex items-center">
                                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                                            <span class="italic">{predecessor.name}</span>
                                                        </div>
                                                    </td>
                                                    <td class="py-2 px-6 text-left text-sm"></td>
                                                    <td class="py-2 px-6 text-right text-sm"></td>
                                                </tr>
                                            {/each}
                                        {/if}
                                        
                                    {:else if !$analyseOptions.isAdvancedMode}
                                        <tr class="bg-blue-50 border-b {expandedTrusts.has(trust.key) ? 'border-transparent' : 'border-blue-200'}">
                                            <td class="py-3 px-6 text-left pl-8">
                                                <div>
                                                    <div>{trust.key}</div>
                                                    {#if trust.predecessors.length > 0}
                                                        <div class="flex items-center gap-2 mt-1">
                                                            <button
                                                                class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                                on:click={() => toggleTrustExpansion(trust.key)}
                                                                title="Show/hide predecessors"
                                                            >
                                                                <svg 
                                                                    class="w-3 h-3 transition-transform duration-200 {expandedTrusts.has(trust.key) ? 'rotate-90' : ''}" 
                                                                    fill="none" 
                                                                    stroke="currentColor" 
                                                                    viewBox="0 0 24 24"
                                                                >
                                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                                                                </svg>
                                                            </button>
                                                            <span class="text-xs text-gray-500">
                                                                {trust.predecessors.length} predecessor{trust.predecessors.length !== 1 ? 's' : ''}
                                                            </span>
                                                        </div>
                                                    {/if}
                                                </div>
                                            </td>
                                            <td class="py-3 px-6 text-left">{trust.units[0]?.unit || '-'}</td>
                                            <td class="py-3 px-6 text-right">
                                                {formatNumber(trust.units[0]?.quantity || 0, trust.units[0]?.unit)}
                                            </td>
                                        </tr>
                                        
                                        {#if expandedTrusts.has(trust.key)}
                                            {#each trust.predecessors as predecessor, index}
                                                <tr class="bg-blue-50 border-b {index === trust.predecessors.length - 1 ? 'border-blue-200' : 'border-transparent'}">
                                                    <td class="py-2 px-6 text-left pl-12 text-sm text-gray-600">
                                                        <div class="flex items-center">
                                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                                            <span class="italic">{predecessor.name}</span>
                                                        </div>
                                                    </td>
                                                    <td class="py-2 px-6 text-left text-sm"></td>
                                                    <td class="py-2 px-6 text-right text-sm"></td>
                                                </tr>
                                            {/each}
                                        {/if}
                                        
                                    {:else}
                                        <tr class="bg-blue-50 border-b {expandedTrusts.has(trust.key) && trust.predecessors.length > 0 ? 'border-transparent' : 'border-blue-200'} font-bold">
                                            <td class="py-3 px-6 text-left pl-8" rowspan={trust.units.length + 1 + (expandedTrusts.has(trust.key) ? trust.predecessors.length : 0)}>
                                                <div>
                                                    <div>{trust.key}</div>
                                                    {#if trust.predecessors.length > 0}
                                                        <div class="flex items-center gap-2 mt-1">
                                                            <button
                                                                class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                                on:click={() => toggleTrustExpansion(trust.key)}
                                                                title="Show/hide predecessors"
                                                            >
                                                                <svg 
                                                                    class="w-3 h-3 transition-transform duration-200 {expandedTrusts.has(trust.key) ? 'rotate-90' : ''}" 
                                                                    fill="none" 
                                                                    stroke="currentColor" 
                                                                    viewBox="0 0 24 24"
                                                                >
                                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                                                                </svg>
                                                            </button>
                                                            <span class="text-xs text-gray-500">
                                                                {trust.predecessors.length} predecessor{trust.predecessors.length !== 1 ? 's' : ''}
                                                            </span>
                                                        </div>
                                                    {/if}
                                                </div>
                                            </td>
                                            <td class="py-3 px-6 text-left">All units</td>
                                            <td class="py-3 px-6 text-right">
                                                {formatNumber(trust.total)}
                                            </td>
                                        </tr>
                                        {#each trust.units as unitData}
                                            <tr class="bg-blue-50 border-b {expandedTrusts.has(trust.key) && trust.predecessors.length > 0 ? 'border-transparent' : 'border-blue-200'}">
                                                <td class="py-3 px-6 text-left">{unitData.unit}</td>
                                                <td class="py-3 px-6 text-right">
                                                    {formatNumber(unitData.quantity, unitData.unit)}
                                                </td>
                                            </tr>
                                        {/each}
                                        
                                        {#if expandedTrusts.has(trust.key)}
                                            {#each trust.predecessors as predecessor, index}
                                                <tr class="bg-blue-50 border-b {index === trust.predecessors.length - 1 ? 'border-blue-200' : 'border-transparent'}">
                                                    <td class="py-2 px-6 text-left text-sm text-gray-600">
                                                        <div class="flex items-center">
                                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                                            <span class="italic">{predecessor.name}</span>
                                                        </div>
                                                    </td>
                                                    <td class="py-2 px-6 text-right text-sm"></td>
                                                </tr>
                                            {/each}
                                        {/if}
                                    {/if}
                                {/each}
                            {/if}

                            {#if trustModeData.otherTrusts.length > 0}
                                <tr class="bg-gray-100 border-b border-gray-200">
                                    <td class="py-2 px-6 text-left font-semibold text-gray-700">
                                        {trustModeData.selectedTrusts.length > 0 ? 'All Other Trusts' : 'All Trusts'} ({otherTrustsCountWithPredecessors})
                                    </td>
                                    <td class="py-2 px-6 text-left font-semibold text-gray-700">{otherTrustsMainUnit}</td>
                                    <td class="py-2 px-6 text-right font-bold text-gray-900">
                                        {#if quantityType === 'DDD'}
                                            {formatNumber(otherTrustsTotal)}
                                        {:else if !$analyseOptions.isAdvancedMode}
                                            {formatNumber(otherTrustsTotal, otherTrustsMainUnit)}
                                        {:else}
                                            {formatNumber(otherTrustsTotal)}
                                        {/if}
                                    </td>
                                </tr>
                                
                                {#each trustModeData.otherTrusts as trust}
                                    {#if quantityType === 'DDD'}
                                        <tr class="border-b border-gray-200 hover:bg-gray-100 {expandedTrusts.has(trust.key) ? 'border-transparent' : ''}">
                                            <td class="py-3 px-6 text-left pl-8">
                                                <div>
                                                    <div>{trust.key}</div>
                                                    {#if trust.predecessors.length > 0}
                                                        <div class="flex items-center gap-2 mt-1">
                                                            <button
                                                                class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                                on:click={() => toggleTrustExpansion(trust.key)}
                                                                title="Show/hide predecessors"
                                                            >
                                                                <svg 
                                                                    class="w-3 h-3 transition-transform duration-200 {expandedTrusts.has(trust.key) ? 'rotate-90' : ''}" 
                                                                    fill="none" 
                                                                    stroke="currentColor" 
                                                                    viewBox="0 0 24 24"
                                                                >
                                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                                                                </svg>
                                                            </button>
                                                            <span class="text-xs text-gray-500">
                                                                {trust.predecessors.length} predecessor{trust.predecessors.length !== 1 ? 's' : ''}
                                                            </span>
                                                        </div>
                                                    {/if}
                                                </div>
                                            </td>
                                            <td class="py-3 px-6 text-left">{trust.units[0]?.unit || 'DDD'}</td>
                                            <td class="py-3 px-6 text-right">{formatNumber(trust.total)}</td>
                                        </tr>
                                        
                                        {#if expandedTrusts.has(trust.key)}
                                            {#each trust.predecessors as predecessor, index}
                                                <tr class="border-b {index === trust.predecessors.length - 1 ? 'border-gray-200' : 'border-transparent'} hover:bg-gray-100">
                                                    <td class="py-2 px-6 text-left pl-12 text-sm text-gray-600">
                                                        <div class="flex items-center">
                                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                                            <span class="italic">{predecessor.name}</span>
                                                        </div>
                                                    </td>
                                                    <td class="py-2 px-6 text-left text-sm"></td>
                                                    <td class="py-2 px-6 text-right text-sm"></td>
                                                </tr>
                                            {/each}
                                        {/if}
                                        
                                    {:else if !$analyseOptions.isAdvancedMode}
                                        <tr class="border-b border-gray-200 hover:bg-gray-100 {expandedTrusts.has(trust.key) ? 'border-transparent' : ''}">
                                            <td class="py-3 px-6 text-left pl-8">
                                                <div>
                                                    <div>{trust.key}</div>
                                                    {#if trust.predecessors.length > 0}
                                                        <div class="flex items-center gap-2 mt-1">
                                                            <button
                                                                class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                                on:click={() => toggleTrustExpansion(trust.key)}
                                                                title="Show/hide predecessors"
                                                            >
                                                                <svg 
                                                                    class="w-3 h-3 transition-transform duration-200 {expandedTrusts.has(trust.key) ? 'rotate-90' : ''}" 
                                                                    fill="none" 
                                                                    stroke="currentColor" 
                                                                    viewBox="0 0 24 24"
                                                                >
                                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                                                                </svg>
                                                            </button>
                                                            <span class="text-xs text-gray-500">
                                                                {trust.predecessors.length} predecessor{trust.predecessors.length !== 1 ? 's' : ''}
                                                            </span>
                                                        </div>
                                                    {/if}
                                                </div>
                                            </td>
                                            <td class="py-3 px-6 text-left">{trust.units[0]?.unit || '-'}</td>
                                            <td class="py-3 px-6 text-right">
                                                {formatNumber(trust.units[0]?.quantity || 0, trust.units[0]?.unit)}
                                            </td>
                                        </tr>
                                        
                                        {#if expandedTrusts.has(trust.key)}
                                            {#each trust.predecessors as predecessor, index}
                                                <tr class="border-b {index === trust.predecessors.length - 1 ? 'border-gray-200' : 'border-transparent'} hover:bg-gray-100">
                                                    <td class="py-2 px-6 text-left pl-12 text-sm text-gray-600">
                                                        <div class="flex items-center">
                                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                                            <span class="italic">{predecessor.name}</span>
                                                        </div>
                                                    </td>
                                                    <td class="py-2 px-6 text-left text-sm"></td>
                                                    <td class="py-2 px-6 text-right text-sm"></td>
                                                </tr>
                                            {/each}
                                        {/if}
                                        
                                    {:else}
                                        <tr class="border-b border-gray-200 hover:bg-gray-100 font-bold {expandedTrusts.has(trust.key) && trust.predecessors.length > 0 ? 'border-transparent' : ''}">
                                            <td class="py-3 px-6 text-left pl-8" rowspan={trust.units.length + 1 + (expandedTrusts.has(trust.key) ? trust.predecessors.length : 0)}>
                                                <div>
                                                    <div>{trust.key}</div>
                                                    {#if trust.predecessors.length > 0}
                                                        <div class="flex items-center gap-2 mt-1">
                                                            <button
                                                                class="p-0.5 hover:bg-gray-200 rounded transition-colors"
                                                                on:click={() => toggleTrustExpansion(trust.key)}
                                                                title="Show/hide predecessors"
                                                            >
                                                                <svg 
                                                                    class="w-3 h-3 transition-transform duration-200 {expandedTrusts.has(trust.key) ? 'rotate-90' : ''}" 
                                                                    fill="none" 
                                                                    stroke="currentColor" 
                                                                    viewBox="0 0 24 24"
                                                                >
                                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                                                                </svg>
                                                            </button>
                                                            <span class="text-xs text-gray-500">
                                                                {trust.predecessors.length} predecessor{trust.predecessors.length !== 1 ? 's' : ''}
                                                            </span>
                                                        </div>
                                                    {/if}
                                                </div>
                                            </td>
                                            <td class="py-3 px-6 text-left">All units</td>
                                            <td class="py-3 px-6 text-right">{formatNumber(trust.total)}</td>
                                        </tr>
                                        {#each trust.units as unitData}
                                            <tr class="border-b border-gray-200 hover:bg-gray-100 {expandedTrusts.has(trust.key) && trust.predecessors.length > 0 ? 'border-transparent' : ''}">
                                                <td class="py-3 px-6 text-left">{unitData.unit}</td>
                                                <td class="py-3 px-6 text-right">
                                                    {formatNumber(unitData.quantity, unitData.unit)}
                                                </td>
                                            </tr>
                                        {/each}
                                        
                                        {#if expandedTrusts.has(trust.key)}
                                            {#each trust.predecessors as predecessor, index}
                                                <tr class="border-b {index === trust.predecessors.length - 1 ? 'border-gray-200' : 'border-transparent'} hover:bg-gray-100">
                                                    <td class="py-2 px-6 text-left text-sm text-gray-600">
                                                        <div class="flex items-center">
                                                            <span class="w-1 h-1 bg-gray-400 rounded-full mr-2.5"></span>
                                                            <span class="italic">{predecessor.name}</span>
                                                        </div>
                                                    </td>
                                                    <td class="py-2 px-6 text-right text-sm"></td>
                                                </tr>
                                            {/each}
                                        {/if}
                                    {/if}
                                {/each}
                            {/if}
                        {:else}

                            {#each mergedData as group, groupIndex}
                                {#if quantityType === 'DDD'}
                                    <tr class="border-b border-gray-200 hover:bg-gray-100">
                                        {#if currentMode !== 'total'}
                                            <td class="py-3 px-6 text-left">{group.key}</td>
                                        {/if}
                                        <td class="py-3 px-6 text-left">{group.units[0]?.unit || 'DDD'}</td>
                                        {#if ['product', 'productGroup', 'ingredient', 'unit'].includes(currentMode) && selectedOrgs.length > 0}
                                            <td class="py-3 px-6 text-right">
                                                {group.selectedTotal ? formatNumber(group.selectedTotal) : '0'}
                                            </td>
                                        {/if}
                                        <td class="py-3 px-6 text-right">{formatNumber(group.total)}</td>
                                    </tr>
                                {:else if !$analyseOptions.isAdvancedMode}
                                    <tr class="border-b border-gray-200 hover:bg-gray-100">
                                        {#if currentMode !== 'total'}
                                            <td class="py-3 px-6 text-left">{group.key}</td>
                                        {/if}
                                        <td class="py-3 px-6 text-left">{group.units[0]?.unit || '-'}</td>
                                        {#if ['product', 'productGroup', 'ingredient', 'unit'].includes(currentMode) && selectedOrgs.length > 0}
                                            <td class="py-3 px-6 text-right">
                                                {#if group.selectedUnits && group.selectedUnits[0]}
                                                    {formatNumber(group.selectedUnits[0].quantity || 0, group.selectedUnits[0].unit)}
                                                {:else}
                                                    0
                                                {/if}
                                            </td>
                                        {/if}
                                        <td class="py-3 px-6 text-right">
                                            {formatNumber(group.units[0]?.quantity || 0, group.units[0]?.unit)}
                                        </td>
                                    </tr>
                                {:else}

                                    <tr class="border-b border-gray-200 hover:bg-gray-100 font-bold">
                                        {#if currentMode !== 'total'}
                                            <td class="py-3 px-6 text-left" rowspan={group.units.length + 1}>
                                                {group.key}
                                            </td>
                                        {/if}
                                        <td class="py-3 px-6 text-left">All units</td>
                                        {#if ['product', 'productGroup', 'ingredient', 'unit'].includes(currentMode) && selectedOrgs.length > 0}
                                            <td class="py-3 px-6 text-right">
                                                {group.selectedTotal ? formatNumber(group.selectedTotal) : '0'}
                                            </td>
                                        {/if}
                                        <td class="py-3 px-6 text-right">{formatNumber(group.total)}</td>
                                    </tr>
                                    
                                    {#each group.units as unitData, unitIndex}
                                        <tr class="border-b border-gray-200 hover:bg-gray-100">
                                            <td class="py-3 px-6 text-left">{unitData.unit}</td>
                                            {#if ['product', 'productGroup', 'ingredient', 'unit'].includes(currentMode) && selectedOrgs.length > 0}
                                                <td class="py-3 px-6 text-right">
                                                    {#if group.selectedUnits && group.selectedUnits[unitIndex]}
                                                        {formatNumber(group.selectedUnits[unitIndex].quantity, group.selectedUnits[unitIndex].unit)}
                                                    {:else}
                                                        0
                                                    {/if}
                                                </td>
                                            {/if}
                                            <td class="py-3 px-6 text-right">
                                                {formatNumber(unitData.quantity, unitData.unit)}
                                            </td>
                                        </tr>
                                    {/each}
                                {/if}
                            {/each}
                        {/if}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>