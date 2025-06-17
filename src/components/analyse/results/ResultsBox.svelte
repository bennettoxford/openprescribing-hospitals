<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import DataTable from './DataTable.svelte';
    import ProductList from './ProductList.svelte';
    import ModeSelector from '../../common/ModeSelector.svelte';
    import Chart from '../../common/Chart.svelte';
    import { createChartStore } from '../../../stores/chartStore';
    import { resultsStore } from '../../../stores/resultsStore';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { formatNumber } from '../../../utils/utils';
    import { calculatePercentiles } from '../../../utils/percentileCalculation';
    import { 
        processChartData, 
        calculateUnits,
        updateTrustCountBreakdown
    }  from '../../../utils/analyseUtils';
    import { VIEW_MODES, DEFAULT_ANALYSIS_MODE } from '../../../stores/analyseOptionsStore.js';
    import { createAnalysisData } from '../../../utils/analyseUtils';

    export let className = '';
    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let vmps = [];
    let filteredData = [];
    let completeAnalysisData = [];
    let originalChartData = null;
    let processingError = null;

    let viewModes = [...VIEW_MODES];

    const resultsChartStore = createChartStore({
        mode: 'trust',
        yAxisLabel: 'units',
        yAxisRange: [0, 100],
        visibleItems: new Set(),
        yAxisBehavior: {
            forceZero: true,
            padTop: 1.1,
            resetToInitial: true
        },
        yAxisTickFormat: (value) => formatNumber(value)
    });

    $: if (analysisData) {
        handleUpdateData(analysisData);
    }

    $: {
        resultsStore.update(store => ({
            ...store,
            isAnalysisRunning,
            showResults,
            analysisData
        }));
    }

    $: defaultMode = DEFAULT_ANALYSIS_MODE;

    let showTrustsWithNoData = false;
    let showIncludedTrusts = false;
    let showTrustCountDetails = false;

    $: {
        const validModes = viewModes.map(mode => mode.value);
        const currentMode = $modeSelectorStore.selectedMode;
        
        if (!currentMode || !validModes.includes(currentMode)) {
            console.warn(`Invalid mode '${currentMode}', resetting to default`);
            modeSelectorStore.resetToDefault(DEFAULT_ANALYSIS_MODE);
        }
    }

    function processCompleteChartData(data) {
        if (!Array.isArray(data)) {
            return { labels: [], datasets: [] };
        }

        const currentMode = $modeSelectorStore.selectedMode || 'organisation';
        const storeItems = $organisationSearchStore.items || [];
        const storePredecessorMap = $organisationSearchStore.predecessorMap || new Map();
        const storeShowPercentiles = $resultsStore.showPercentiles !== false;
        const selectedOrgs = $organisationSearchStore.selectedItems || [];

        const dataForPercentiles = filteredData.length > 0 ? filteredData : data;
        const percentilesResult = calculatePercentiles(dataForPercentiles, storePredecessorMap);

        const units = calculateUnits(data, filteredData);

        const chartResult = processChartData(
            data,
            filteredData,
            currentMode,
            selectedOrgs,
            storePredecessorMap,
            storeItems,
            storeShowPercentiles,
            percentilesResult.percentiles
        );

        resultsChartStore.setConfig({
            ...($resultsChartStore.config || {}),
            yAxisLabel: units,
            yAxisTickFormat: (value) => formatNumber(value)
        });

        const trustCountBreakdown = updateTrustCountBreakdown(
            percentilesResult, 
            storePredecessorMap
        );

        resultsStore.update(store => ({
            ...store,
            percentiles: percentilesResult.percentiles,
            trustCount: percentilesResult.trustCount,
            excludedTrusts: percentilesResult.excludedTrusts,
            trustsWithNoData: chartResult.trustsWithNoData,
            trustCountBreakdown
        }));

        return {
            labels: chartResult.labels,
            datasets: chartResult.datasets,
            maxValue: chartResult.maxValue
        };
    }

    function handleUpdateData(analysisResult) {
        processingError = null;
        
        showIncludedTrusts = false;
        showTrustsWithNoData = false;
        showTrustCountDetails = false;
        
        try {
            const analysisData = createAnalysisData(analysisResult);
            
            if (!analysisData.data || analysisData.data.length === 0) {
                console.warn('No valid analysis data received');
                return;
            }

            const data = analysisData.data;
            const metadata = {
                percentiles: analysisData.percentiles,
                searchType: analysisData.searchType,
                quantityType: analysisData.quantityType,
                selectedOrganisations: analysisData.selectedOrganisations
            };
      
        completeAnalysisData = data;
        selectedData = data;

        modeSelectorStore.resetToDefault('organisation');

        resultsStore.update(store => ({
            ...store,
            analysisData: {
                ...store.analysisData,
                ...metadata
            }
        }));

   
        const uniqueVmps = {};
        data.forEach(item => {
            if (item.vmp__code && !uniqueVmps[item.vmp__code]) {
                let primaryUnit = 'nan';
                if (Array.isArray(item.data)) {
                    const firstValidEntry = item.data.find(([_, __, unit]) => unit && unit !== 'nan');
                    if (firstValidEntry) {
                        primaryUnit = firstValidEntry[2];
                    }
                }

                uniqueVmps[item.vmp__code] = {
                    code: item.vmp__code,
                    name: item.vmp__name,
                    vmp: item.vmp__name, 
                    vtm: item.vmp__vtm__name,
                    ingredients: item.ingredient_names || [],
                    ingredient_codes: item.ingredient_codes || [],
                    unit: primaryUnit,
                    units: new Set()
                };
            }
            
            if (Array.isArray(item.data)) {
                item.data.forEach(([_, __, unit]) => {
                    if (unit && uniqueVmps[item.vmp__code]) {
                        uniqueVmps[item.vmp__code].units.add(unit);
                    }
                });
            }
        });

        vmps = Object.values(uniqueVmps);

            const chartData = processCompleteChartData(data);
            originalChartData = chartData;
            resultsChartStore.setData(chartData);
        } catch (error) {
            console.error('Error processing analysis data:', error);
            processingError = error.message || 'Failed to process analysis data';
        }
    }

    function handleProductFilter(event) {
        const selectedVMPs = event.detail;
    
        if (!Array.isArray(selectedVMPs)) {

            filteredData = [];
            const chartData = processCompleteChartData(completeAnalysisData);
        resultsChartStore.setData(chartData);
            return;
        }
        
        if (selectedVMPs.length === 0) {
            filteredData = [];
            const chartData = processCompleteChartData(completeAnalysisData);
            resultsChartStore.setData(chartData);
            return;
        }

        filteredData = completeAnalysisData.filter(item => {
            return selectedVMPs.some(selectedVMP => 
                selectedVMP.vmp === item.vmp__name || 
                selectedVMP.name === item.vmp__name ||
                selectedVMP.code === item.vmp__code
            );
        });

        const chartData = processCompleteChartData(completeAnalysisData);
        resultsChartStore.setData(chartData);
    }

    $: if (selectedData.length > 0 && 
           ($modeSelectorStore.selectedMode || 
            $resultsStore.showPercentiles !== undefined || 
            filteredData)) {
        const chartData = processCompleteChartData(selectedData);
        resultsChartStore.setData(chartData);
    }

    $: if (analysisData) {
        handleUpdateData(analysisData);
        
        const initialData = selectedData.length > 0 ? selectedData : completeAnalysisData;
        processCompleteChartData(
            initialData,
        );
    }

    $: {
        viewModes = [...VIEW_MODES];

        if (vmps.length > 1) {
            viewModes.push({ value: 'product', label: 'Product' });
        }

        const uniqueVtms = new Set(vmps.map(vmp => vmp.vtm).filter(vtm => vtm && vtm !== '-' && vtm !== 'nan'));
        if (uniqueVtms.size > 1) {
            viewModes.push({ value: 'productGroup', label: 'Product Group' });
        }

        const uniqueIngredients = new Set(
            vmps.flatMap(vmp => (vmp.ingredients || []))
                .filter(ing => ing && ing !== '-' && ing !== 'nan')
        );
        if (uniqueIngredients.size > 1) {
            viewModes.push({ value: 'ingredient', label: 'Ingredient' });
        }

        const uniqueUnits = new Set(
            vmps.flatMap(vmp => 
                vmp.units && vmp.units instanceof Set 
                    ? Array.from(vmp.units)
                    : []
            ).filter(unit => unit && unit !== '-' && unit !== 'nan')
        );
        if (uniqueUnits.size > 1) {
            viewModes.push({ value: 'unit', label: 'Unit' });
        }
    }

    $: if (analysisData) {
        handleUpdateData(analysisData);
    }

    function formatDate(date) {
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const d = new Date(date);
        return `${months[d.getMonth()]} ${d.getFullYear()}`;
    }

    function customTooltipFormatter(d) {
        const label = d.dataset.label || 'No label';
        const date = formatDate(d.date);
        const value = d.value;
        const chartConfig = $resultsChartStore.config;
        
        let unit;
        if (d.dataset.isProduct && vmps.length > 0) {
            const matchingVmp = vmps.find(vmp => vmp.vmp === d.dataset.label);
            const baseUnit = matchingVmp?.unit || chartConfig?.yAxisLabel || 'unit';
            
            if (value !== 1) {
                if (baseUnit.endsWith('y')) {
                    unit = baseUnit.slice(0, -1) + 'ies';
                } else if (!baseUnit.endsWith('s')) {
                    unit = baseUnit + 's';
                } else {
                    unit = baseUnit;
                }
            } else {
                unit = baseUnit;
            }
        } else {
            unit = chartConfig?.yAxisLabel || 'units';
        }

        const tooltipContent = [
            { text: label, class: 'font-medium' },
            { label: 'Date', value: date },
            { label: 'Value', value: `${formatNumber(value)} ${unit}` }
        ];

        if (d.dataset.isOrganisation || d.dataset.isTrust) {
            if (d.dataset.numerator !== undefined && d.dataset.denominator !== undefined) {
                tooltipContent.push(
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator[d.index], { addCommas: true }) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator[d.index], { addCommas: true }) }
                );
            }
        }

        return tooltipContent;
    }

    function hasChartableData(data) {
        if (!Array.isArray(data) || data.length === 0) {
            return false;
        }

        const hasChartable = data.some(item => 
            Array.isArray(item.data) && 
            item.data.length > 0 &&
            item.data.some(([_, value]) => value && !isNaN(parseFloat(value)))
        );

        return hasChartable;
    }

    function handlePercentileToggle() {
        resultsStore.update(store => ({
            ...store,
            showPercentiles: !store.showPercentiles
        }));
    }

    function getIncludedTrusts() {
        if (!selectedData || selectedData.length === 0) {
            return [];
        }

        const includedTrusts = new Set();
        const storePredecessorMap = $organisationSearchStore.predecessorMap || new Map();

        selectedData.forEach(item => {
            if (item && item.organisation__ods_name) {
                includedTrusts.add(item.organisation__ods_name);
            }
        });

        if (storePredecessorMap instanceof Map) {
            for (const [successor, predecessors] of storePredecessorMap.entries()) {
                if (includedTrusts.has(successor) && Array.isArray(predecessors)) {
                    predecessors.forEach(predecessor => {
                        includedTrusts.add(predecessor);
                    });
                }
            }
        }

        return Array.from(includedTrusts).sort().map(trust => ({ name: trust, code: 'Unknown' }));
    }


</script>

{#if showResults}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col {className}">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if processingError}
                <div class="p-4 bg-red-50 border border-red-200 rounded-lg m-6">
                    <div class="flex items-center">
                        <svg class="h-5 w-5 text-red-400 mr-3" viewBox="0 0 20 20" fill="currentColor">
                            <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
                        </svg>
                        <div>
                            <h3 class="text-sm font-medium text-red-800">Analysis Processing Error</h3>
                            <p class="text-sm text-red-700 mt-1">{processingError}</p>
                        </div>
                    </div>
                </div>
            {:else if isAnalysisRunning}
                <div class="flex items-center justify-center h-[500px] p-16">
                    <div class="animate-spin rounded-full h-32 w-32 border-t-4 border-b-4 border-oxford-500"></div>
                </div>
            {:else if selectedData.length > 0}
                <div class="space-y-6 p-6">
                    <section class="bg-white rounded-lg p-4 border-2 border-oxford-300 shadow-sm">
                        <ProductList {vmps} on:dataFiltered={handleProductFilter} />
                    </section>
                    {#if hasChartableData(selectedData)}
                    
                    <div>
                        <h3 class="text-xl font-semibold">Time Series Chart</h3> 
                        {#if $modeSelectorStore.selectedMode === 'organisation'}
                            <p class="text-sm text-gray-700 mb-4">
                                Showing a percentiles chart calculated across all NHS trusts who have issued any of the selected products.
                                {#if $resultsStore.analysisData?.selectedOrganisations?.length > 0}
                                    Selected NHS Trusts are shown as individual lines.
                                {:else}
                                    Select individual trusts in the analysis builder to see them on this chart.
                                {/if}
                                See <a href="/faq/#percentiles" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret this chart.
                            </p>
                    {#if $resultsStore.trustCount > 0}
                    <section class="bg-blue-50 border border-blue-200 rounded-lg">
                        <button
                            type="button"
                            class="w-full px-4 py-3 text-left flex items-center justify-between focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-inset"
                            on:click={() => showTrustCountDetails = !showTrustCountDetails}
                        >
                            <div class="flex items-center">
                                <svg class="h-5 w-5 text-blue-400 mr-3 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor">
                                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                                </svg>
                                <span class="text-sm text-blue-700 font-medium">
                                    {$resultsStore.trustCount}/{$organisationSearchStore.items.length} trusts included in percentile calculation
                                </span>
                            </div>
                            <svg 
                                class="h-5 w-5 text-blue-400 transform transition-transform duration-200 {showTrustCountDetails ? 'rotate-180' : ''}" 
                                viewBox="0 0 20 20" 
                                fill="currentColor"
                            >
                                <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                            </svg>
                        </button>
                        
                        {#if showTrustCountDetails}
                        <div class="px-4 pb-3 space-y-3 border-t border-blue-200 pt-3">
                            <p class="text-sm text-blue-700">
                                Trusts are only included if they have issued any of the selected products during the time period.
                            </p>
                            
                            <div>
                                <button
                                    type="button"
                                    class="text-sm text-blue-600 hover:text-blue-800 underline font-medium"
                                    on:click={() => showIncludedTrusts = !showIncludedTrusts}
                                >
                                    {showIncludedTrusts ? 'Hide' : 'Show'} {$resultsStore.trustCount} trusts included in percentiles calculation
                                </button>
                                
                                {#if showIncludedTrusts}
                                <div class="mt-2 p-3 bg-gray-100 rounded border border-gray-200">
                                    <p class="text-xs text-gray-800 font-medium mb-2">
                                        Trusts included in percentile calculation:
                                    </p>
                                    <div class="max-h-32 overflow-y-auto">
                                        <ul class="text-xs text-gray-700 space-y-1">
                                            {#each getIncludedTrusts() as trust}
                                            <li class="flex items-center">
                                                <span class="w-1.5 h-1.5 bg-gray-400 rounded-full mr-2 flex-shrink-0"></span>
                                                <span>{trust.name}</span>
                                            </li>
                                            {/each}
                                        </ul>
                                    </div>
                                </div>
                                {/if}
                            </div>
                            
                            {#if $resultsStore.trustsWithNoData && $resultsStore.trustsWithNoData.length > 0}
                            <div>
                                <button
                                    type="button"
                                    class="text-sm text-blue-600 hover:text-blue-800 underline font-medium"
                                    on:click={() => showTrustsWithNoData = !showTrustsWithNoData}
                                >
                                    {showTrustsWithNoData ? 'Hide' : 'Show'} {$resultsStore.trustsWithNoData.length} trusts excluded from percentiles calculation
                                </button>
                                
                                {#if showTrustsWithNoData}
                                <div class="mt-2 p-3 bg-gray-100 rounded border border-gray-200">
                                    <p class="text-xs text-gray-800 font-medium mb-2">
                                        Trusts with no data for selected products:
                                    </p>
                                    <div class="max-h-32 overflow-y-auto">
                                        <ul class="text-xs text-gray-700 space-y-1">
                                            {#each $resultsStore.trustsWithNoData as trust}
                                            <li class="flex items-center">
                                                <span class="w-1.5 h-1.5 bg-gray-400 rounded-full mr-2 flex-shrink-0"></span>
                                                <span>{trust.name}</span>
                                            </li>
                                            {/each}
                                        </ul>
                                    </div>
                                </div>
                                {/if}
                            </div>
                            {/if}
                        </div>
                        {/if}
                    </section>
                    {/if}
                    {:else if $modeSelectorStore.selectedMode === 'region'}
                        <p class="text-sm text-gray-700 mb-4">
                            Showing total quantity issued for all selected products aggregated by NHS Region. Each line represents the total quantity across all trusts within a region.
                        </p>
                    {:else if $modeSelectorStore.selectedMode === 'icb'}
                        <p class="text-sm text-gray-700 mb-4">
                            Showing total quantity issued for all selected products aggregated by Integrated Care Board (ICB). Each line represents the total quantity across all trusts within an ICB.
                        </p>
                    {:else if $modeSelectorStore.selectedMode === 'total'}
                        <p class="text-sm text-gray-700 mb-4">
                            Showing the national total quantity issued for all selected products across all NHS Trusts in England.
                        </p>
                    {:else if $modeSelectorStore.selectedMode === 'product'}
                        <p class="text-sm text-gray-700 mb-4">
                            Showing total quantity issued for each selected product. Each line represents a different product's total across all NHS Trusts in England.
                        </p>
                    {:else if $modeSelectorStore.selectedMode === 'productGroup'}
                        <p class="text-sm text-gray-700 mb-4">
                            Showing total quantity issued for each product group. Each line represents the total for all products within a group across all NHS Trusts in England.
                        </p>
                    {:else if $modeSelectorStore.selectedMode === 'ingredient'}
                        <p class="text-sm text-gray-700 mb-4">
                            Showing total quantity issued for each active ingredient. Each line represents the total for all products containing that ingredient across all NHS Trusts in England.
                        </p>
                    {/if}
                </div>
            
                <section class="pb-4">
                    <div class="mb-4 flex justify-between items-center">
                        <ModeSelector 
                            options={viewModes}
                            initialMode={defaultMode}
                            label="View Mode"
                            variant="pill"
                        />

                        {#if $modeSelectorStore.selectedMode === 'organisation'}
                        <div class="flex flex-col items-center gap-2">
                            {#if analysisData?.selectedOrganisations?.length > 0}
                                <span class="text-sm text-gray-600 leading-tight text-center">
                                    Show percentiles
                                </span>
                                <div class="flex items-center gap-2">
                                    <label class="inline-flex items-center cursor-pointer">
                                        <input
                                            type="checkbox"
                                            class="sr-only peer"
                                            checked={$resultsStore.showPercentiles}
                                            on:change={handlePercentileToggle}
                                        />
                                        <div class="relative w-9 h-5 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600"></div>
                                    </label>
                                    <div class="relative inline-block group">
                                        <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                                            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                                <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                                            </svg>
                                        </button>
                                        <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                                                    group-hover:scale-100 w-[280px] -translate-x-full left-0 top-8 mt-1 rounded-md shadow-lg bg-white 
                                                    ring-1 ring-black ring-opacity-5 p-4">
                                            <p class="text-sm text-gray-500">
                                                Percentiles show variation in product quantity across NHS Trusts and allow easy comparison of Trust activity relative to the median Trust level. See <a href="/faq/#percentiles" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret them.
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            {/if}
                        </div>
                        {/if}
                        
                    </div>

                    <div class="grid grid-cols-1 gap-4">
                        <div class="relative h-[550px] mb-6 sm:mb-0">
                            <Chart 
                                store={resultsChartStore} 
                                data={filteredData.length > 0 ? filteredData : selectedData}
                                formatTooltipContent={customTooltipFormatter}
                                mode={$modeSelectorStore.selectedMode}
                            />
                        </div>
                    </div>
                </section>

                <div class="space-y-4">
                    <section class="bg-amber-50 border-l-4 border-amber-400 p-4 relative z-10">
                        <div class="flex flex-col sm:flex-row">
                            <div class="flex-shrink-0 mb-2 sm:mb-0">
                                <svg class="h-5 w-5 text-amber-400" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                    <path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v4.5a.75.75 0 01-1.5 0v-4.5A.75.75 0 0110 5zm0 10a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" />
                                </svg>
                            </div>
                            <div class="sm:ml-3">
                                <p class="text-sm text-amber-700">
                                    Individual NHS Trust data submissions may be incomplete or inconsistent. 
                                    <a href="/submission-history/" class="font-medium underline hover:text-amber-800">
                                        View Submission History
                                    </a> to understand data quality issues for individual NHS Trusts.
                                </p>
                            </div>
                        </div>
                    </section>
                </div>

                <section>
                    <DataTable 
                        data={filteredData} 
                        quantityType={$analyseOptions.quantityType} 
                        searchType={$analyseOptions.searchType} 
                    />
                </section>
                {:else}
                    <div class="flex items-center justify-center h-[500px] p-6">
                        <div class="text-center space-y-6">
                            <div>
                                <p class="text-oxford-600 text-xl font-medium mb-3">No chartable data found</p>
                            </div>
                        </div>
                    </div>
                {/if}
            </div>
            {:else}
                <div class="flex items-center justify-center h-[500px] p-6">
                    <div class="text-center space-y-6">
                        <div>
                            <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                        </div>
                    </div>
                </div>
            {/if}
        </div>
    </div>
{:else}
    <div class="flex items-center justify-center h-[500px] p-6 {className}">
        <div class="text-center space-y-6">
            <div>
                <p class="text-oxford-600 text-xl font-medium mb-3">No analysis results to show</p>
            </div>
        </div>
    </div>
{/if}
