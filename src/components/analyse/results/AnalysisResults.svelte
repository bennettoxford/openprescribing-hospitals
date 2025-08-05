<svelte:options customElement={{
    tag: 'analysis-results',
    shadow: 'none'
  }} />

<script>
    import TotalsTable from './TotalsTable.svelte';
    import ProductsTable from './ProductsTable.svelte';
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import Chart from '../../common/Chart.svelte';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { chartConfig } from '../../../utils/chartConfig.js';
    import ModeSelector from '../../common/ModeSelector.svelte';
    import { createChartStore } from '../../../stores/chartStore';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { formatNumber } from '../../../utils/utils';
    import pluralize from 'pluralize';
    import { ChartDataProcessor, ViewModeCalculator, selectDefaultMode, processTableDataByMode } from '../../../utils/analyseUtils.js';

    export let className = '';
    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let vmps = [];
    let filteredData = [];
    let viewModes = [];
    let viewModeCalculator;

    const resultsChartStore = createChartStore({
        mode: 'trust',
        yAxisLabel: 'units',
        yAxisRange: [0, 100],
        visibleItems: new Set(),
        yAxisBehavior: {
            forceZero: true,
            padTop: 1.1,
            resetToInitial: true
        }
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

    let colorMappings = new Map();

    function getOrganisationColor(index) {
        const allColors = chartConfig.allColours;
        return allColors[index % allColors.length];
    }

    function getConsistentColor(key, index) {
        if (!colorMappings.has(key)) {
            colorMappings.set(key, getOrganisationColor(colorMappings.size));
        }
        return colorMappings.get(key);
    }

    let datasets = [];

    function processChartData(data) {
        const processor = new ChartDataProcessor(
            data,
            $resultsStore.aggregatedData,
            {
                selectedOrganisations: $analyseOptions.selectedOrganisations,
                predecessorMap: $organisationSearchStore.predecessorMap
            }
        );

        const { datasets, maxValue } = processor.processMode($modeSelectorStore.selectedMode);
        const combinedUnits = processor.getCombinedUnits();

        let yAxisLabel;
        if (processor.uniqueUnits.length === 1) {
            yAxisLabel = pluralize(processor.uniqueUnits[0]);
        } else if (processor.uniqueUnits.length > 1) {
            yAxisLabel = combinedUnits;
        } else {
            yAxisLabel = 'units';
        }

        const chartConfig = {
            mode: $modeSelectorStore.selectedMode,
            yAxisLabel: yAxisLabel,
            visibleItems: new Set(datasets.map(d => d.label)),
            yAxisRange: [0, maxValue],
            yAxisBehavior: {
                forceZero: true,
                padTop: 1.1,
                resetToInitial: true
            },
            yAxisTickFormat: value => {
                const range = maxValue;
                let decimals = 1;
                if (typeof value === 'number' && !isNaN(value)) {
                    const step = range / 10;
                    if (step > 0) {
                        decimals = Math.min(
                            Math.max(1, Math.ceil(Math.abs(Math.log10(step))) + 1),
                            5
                        );
                    }
                }
                return formatNumber(value, { maxDecimals: decimals });
            },
            tooltipValueFormat: value => {
                const baseUnit = combinedUnits || 'units';
                const pluralizedUnit = pluralize(baseUnit, value);
                return formatNumber(value, { showUnit: true, unit: pluralizedUnit });
            }
        };

        const chartData = {
            labels: processor.allDates,
            datasets: datasets
        };

        resultsChartStore.setData(chartData);
        resultsChartStore.setConfig(chartConfig);

        return chartData;
    }

    function handleUpdateData(data) {
        selectedData = Array.isArray(data.data) ? data.data : [];
        
        try {
            const vmpGroups = selectedData.reduce((acc, item) => {
                const key = item.vmp__name;
                if (!acc[key]) {
                    acc[key] = {
                        vmp: item.vmp__name,
                        code: item.vmp__code,
                        vtm: item.vmp__vtm__name,
                        ingredients: item.ingredient_names || [],
                        units: new Set(),
                        searchType: data.searchType || $analyseOptions.searchType
                    };
                }
                if (item.data) {
                    item.data.forEach(dataPoint => {
                        if (dataPoint[2]) {
                            acc[key].units.add(dataPoint[2]);
                        }
                    });
                }
                return acc;
            }, {});

            vmps = Object.values(vmpGroups)
                .filter(vmp => vmp.vmp) // Filter out undefined VMPs
                .map(vmp => ({
                    ...vmp,
                    unit: vmp.units.size > 0 ? Array.from(vmp.units).join(', ') : 'nan',
                }));

            resultsStore.update(store => ({
                ...store,
                analysisData: selectedData,
                showResults: true,
                searchType: data.searchType || $analyseOptions.searchType,
                quantityType: data.quantityType || $analyseOptions.quantityType
            }));

            viewModeCalculator = new ViewModeCalculator(
                $resultsStore,
                $analyseOptions,
                $organisationSearchStore,
                vmps
            );

            viewModes = viewModeCalculator.calculateAvailableModes();

            if (viewModes.length > 0) {
                const hasSelectedOrganisations = $analyseOptions.selectedOrganisations && 
                                               $analyseOptions.selectedOrganisations.length > 0;
                const defaultMode = selectDefaultMode(viewModes, hasSelectedOrganisations);
                
                modeSelectorStore.setSelectedMode(defaultMode);
            }
        } catch (error) {
            console.error("Error processing data:", error);
        }
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;

        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp__name)
        );

        const chartData = processChartData(
            filteredData
        );

        resultsChartStore.setData({
            ...chartData,
            forceUpdate: Date.now()
        });

        resultsStore.update(store => ({
            ...store,
            filteredData
        }));
    }

    $: if ($modeSelectorStore.selectedMode && selectedData.length > 0) {
        
        let dataToProcess;
        
        if ($modeSelectorStore.selectedMode === 'organisation' || 
            $modeSelectorStore.selectedMode === 'product' || $modeSelectorStore.selectedMode === 'productGroup' ||
            $modeSelectorStore.selectedMode === 'unit' || $modeSelectorStore.selectedMode === 'ingredient') {
            
            dataToProcess = $resultsStore.filteredData && $resultsStore.filteredData.length > 0 ? 
                $resultsStore.filteredData : selectedData;
        } else {
            dataToProcess = null;
        }
        
        processChartData(dataToProcess);
    }

    $: if (analysisData) {
        handleUpdateData(analysisData);
    }

    $: if (analysisData) {
        colorMappings.clear();
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
            const baseUnit = chartConfig?.yAxisLabel || 'units';
            unit = pluralize(baseUnit, value);
        }

        const tooltipContent = [
            { text: label, class: 'font-medium' },
            { label: 'Date', value: date },
            { label: 'Value', value: `${formatNumber(value)} ${unit}` }
        ];

        if (d.dataset.isOrganisation || d.dataset.isProduct || d.dataset.isProductGroup) {
            if (d.dataset.numerator !== undefined && d.dataset.denominator !== undefined) {
                tooltipContent.push(
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator[d.index], { addCommas: true }) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator[d.index], { addCommas: true }) }
                );
            }
        }

        return tooltipContent;
    }


    function hasChartableDataForMode(data, mode) {

        if (mode === 'organisation') {
            if (!Array.isArray(data) || data.length === 0) return false;
            
            const selectedOrgNames = new Set($analyseOptions.selectedOrganisations || []);
            return data.filter(item => selectedOrgNames.has(item.organisation__ods_name))
                .some(item => 
                    Array.isArray(item.data) && 
                    item.data.length > 0 &&
                    item.data.some(([_, value]) => value && !isNaN(parseFloat(value)))
                );
        }
        
        const tableData = processTableDataByMode(
            data, 
            mode, 
            'all',
            $resultsStore.aggregatedData,
            null,
            $analyseOptions.selectedOrganisations || [],
            $organisationSearchStore.items || [],
            $organisationSearchStore.predecessorMap || new Map(),
            new Set()
        );
        
        return tableData && tableData.length > 0 && tableData.some(entry => entry.total > 0);
    }

    $: currentModeHasData = hasChartableDataForMode(selectedData, $modeSelectorStore.selectedMode);

    $: isInTrustModeWithNoData = $modeSelectorStore.selectedMode === 'organisation' && 
           $analyseOptions.selectedOrganisations?.length > 0 &&
           !currentModeHasData;
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
                        <ProductsTable {vmps} on:dataFiltered={handleFilteredData} />
                    </section>
                    
                    {#if viewModes.length > 0}
                    <section class="p-4">
                        <div class="mb-4">
                            <ModeSelector 
                                options={viewModes}
                                initialMode={selectDefaultMode(viewModes, $analyseOptions.selectedOrganisations?.length > 0)}
                                label="View Mode"
                                variant="pill"
                            />
                        </div>
                        
                        {#if isInTrustModeWithNoData}
                            <div class="flex items-center justify-center h-[550px] p-6">
                                <div class="text-center space-y-6">
                                    <div>
                                        <p class="text-oxford-600 text-xl font-medium mb-3">No data for selected trusts</p>
                                        <p class="text-gray-600 text-base max-w-md">
                                            The selected NHS Trusts have no data for the chosen products and quantity type. 
                                            <a href="/faq/#why-is-there-no-quantity-for-some-products" class="link-oxford" target="_blank">
                                                Learn more about why quantities might be missing
                                            </a>.
                                        </p>
                                    </div>
                                </div>
                            </div>
                        {:else if currentModeHasData}
                            <div class="grid grid-cols-1 gap-4">
                                <div class="relative h-[550px] mb-6 sm:mb-0">
                                    <Chart 
                                        store={resultsChartStore} 
                                        data={filteredData.length > 0 ? filteredData : selectedData}
                                        formatTooltipContent={customTooltipFormatter}
                                    />
                                </div>
                            </div>
                        {:else}
                            <div class="flex items-center justify-center h-[550px] p-6">
                                <div class="text-center space-y-6">
                                    <div>
                                        <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                                        <p class="text-oxford-400 text-base max-w-md">
                                            No data was returned for the selected view mode. 
                                            <a href="/faq/#missing-quantities" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                                                Learn more about why quantities might be missing
                                            </a>.
                                        </p>
                                    </div>
                                </div>
                            </div>
                        {/if}
                    </section>

                    <section class="bg-amber-50 border-l-4 border-amber-400 p-4 mx-4 mb-4 mt-2 relative z-10">
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

                    <section class="p-4">
                        <TotalsTable 
                            data={filteredData} 
                            quantityType={$analyseOptions.quantityType} 
                            searchType={$analyseOptions.searchType} 
                        />
                    </section>
                    {:else}
                        <div class="flex items-center justify-center h-[500px] p-6">
                            <div class="text-center space-y-6">
                                <div>
                                    <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                                    <p class="text-oxford-400 text-base max-w-md">
                                        No data was returned for the selected quantity type of the chosen products. 
                                        <a href="/faq/#missing-quantities" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                                            Learn more about why quantities might be missing
                                        </a>.
                                    </p>
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
                            <p class="text-oxford-400 text-base max-w-md">The analysis returned no chartable data.</p>
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
                <p class="text-oxford-400 text-base max-w-md">Please run an analysis to see results here.</p>
            </div>
        </div>
    </div>
{/if}
