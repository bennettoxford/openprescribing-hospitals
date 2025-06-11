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
    import pluralize from 'pluralize';
    import { 
        processChartData, 
        calculateUnits
    }  from '../../../utils/analyseUtils';

    export let className = '';
    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let vmps = [];
    let filteredData = [];
    let completeAnalysisData = [];
    let originalChartData = null;

    let viewModes = [
        { value: 'total', label: 'National Total' },
        { value: 'region', label: 'Region' },
        { value: 'icb', label: 'ICB' }
    ];
 
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

    $: hasSelectedTrusts = analysisData?.selectedOrganisations?.length > 0;

    $: defaultMode = hasSelectedTrusts ? 'organisation' : 'total';

    $: {
        if (!$modeSelectorStore.selectedMode || 
            !viewModes.some(mode => mode.value === $modeSelectorStore.selectedMode)) {
            modeSelectorStore.resetToDefault(defaultMode);
        }
    }

    function processCompleteChartData(data) {
        if (!Array.isArray(data)) {
            return { labels: [], datasets: [] };
        }

        const uniqueUnits = new Set();
        data.forEach(item => {
                if (unit) uniqueUnits.add(unit);
            });
        });
        
        const formattedUnits = Array.from(uniqueUnits).map(unit => {
            const count = data.reduce((acc, item) => {
                return acc + item.data.filter(([_, __, u]) => u === unit).length;
            }, 0);
            
            if (count > 1) {
                return pluralize(unit);
            }
            return unit;
        });
        
        const combinedUnits = formattedUnits.join('/');
        
        const currentMode = $modeSelectorStore.selectedMode || 'organisation';
        const storeItems = $organisationSearchStore.items || [];
        const selectedOrgs = $organisationSearchStore.selectedItems || [];

        const units = calculateUnits(data, filteredData);

        const chartResult = processChartData(
            data,
            filteredData,
            currentMode,
            selectedOrgs,
            storePredecessorMap,
            storeItems,
        );

        resultsChartStore.setConfig({
            ...($resultsChartStore.config || {}),
            yAxisTickFormat: (value) => formatNumber(value)
        });

        return {
            labels: chartResult.labels,
            datasets: chartResult.datasets,
            maxValue: chartResult.maxValue
        };
    }

    function handleUpdateData(analysisResult) {
        let data;
        let metadata = {};

        if (analysisResult.data && Array.isArray(analysisResult.data)) {
            data = analysisResult.data;
            metadata = {
                searchType: analysisResult.searchType,
                quantityType: analysisResult.quantityType,
                selectedOrganisations: analysisResult.selectedOrganisations || []
            };
        } else {
            return;
        }
      
        completeAnalysisData = data;
        selectedData = data;

        const newDefaultMode = hasSelectedTrusts ? 'organisation' : 'total';
        modeSelectorStore.resetToDefault(newDefaultMode);

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
        viewModes = [
            { value: 'total', label: 'National Total' }
        ];

        if (hasSelectedTrusts) {
            viewModes.unshift({ value: 'organisation', label: 'NHS Trust' });
        }

        viewModes.push(
            { value: 'icb', label: 'ICB' },
            { value: 'region', label: 'Region' }
        );

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
            
            unit = pluralize(baseUnit, value);
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


</script>

{#if showResults}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col {className}">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if isAnalysisRunning}
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
