<svelte:options customElement={{
    tag: 'analysis-results',
    shadow: 'none'
  }} />

<script>
    import { onDestroy } from 'svelte';
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
    import { formatNumber, getCurrentUrl, copyToClipboard, getUrlParams } from '../../../utils/utils';
    import { normaliseMode } from '../../../utils/analyseUtils.js';
    import pluralize from 'pluralize';
    import { ChartDataProcessor, ViewModeCalculator, selectDefaultMode, processTableDataByMode, calculatePercentiles, getTrustCount, getChartExplainerText } from '../../../utils/analyseUtils.js';

  export let className = '';
  export let isAnalysisRunning;
  export let analysisData;
  export let showResults;
  export let urlValidationErrors = [];

    let selectedData = [];
    let vmps = [];
    let filteredData = [];
    let viewModes = [];
    let viewModeCalculator;
    let isCopyingShareLink = false;
    let showShareToast = false;
    let shareToastMessage = '';
    let shareToastVariant = 'success';
    let shareToastTimeout;
    let excludedVmps = [];
    let showTrustCountDetails = false;
    let modeFromUrl = null;
    let isModeFromUrlApplied = false;

    if (typeof window !== 'undefined') {
        const params = getUrlParams();
        const modeParam = params.get('mode');
        modeFromUrl = normaliseMode(modeParam);
    }

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

    $: excludedVmps = Array.isArray($resultsStore.excludedVmps) ? $resultsStore.excludedVmps : [];

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

    $: if (selectedData && selectedData.length > 0 && $modeSelectorStore.selectedMode) {
        processChartData(selectedData);
    }

    $: currentModeHasData = (() => {
        if (!selectedData || !Array.isArray(selectedData) || selectedData.length === 0) return false;
        
        if ($modeSelectorStore.selectedMode === 'trust') {
            const selectedOrgNames = new Set($analyseOptions.selectedOrganisations || []);
            
            // If no trusts are selected, check if there's any data available for percentiles
            if (selectedOrgNames.size === 0) {
                return selectedData.some(item => 
                    Array.isArray(item.data) && 
                    item.data.length > 0 &&
                    item.data.some(([_, value]) => value && !isNaN(parseFloat(value)))
                );
            }
            
            // If trusts are selected, check if selected trusts have data
            return selectedData.filter(item => selectedOrgNames.has(item.organisation__ods_name))
                .some(item => 
                    Array.isArray(item.data) && 
                    item.data.length > 0 &&
                    item.data.some(([_, value]) => value && !isNaN(parseFloat(value)))
                );
        }

        const tableData = processTableDataByMode(
            selectedData, 
            $modeSelectorStore.selectedMode, 
            'all',
            $resultsStore.aggregatedData,
            null,
            $analyseOptions.selectedOrganisations || [],
            $organisationSearchStore.items || [],
            $organisationSearchStore.predecessorMap || new Map(),
            new Set()
        );
        
        return tableData && tableData.length > 0 && tableData.some(entry => entry.total > 0);
    })();

    $: isInTrustModeWithNoData = $modeSelectorStore.selectedMode === 'trust' && 
           $analyseOptions.selectedOrganisations?.length > 0 &&
           !currentModeHasData &&
           !$resultsStore.showPercentiles;

    $: canShowPercentilesWithoutTrustData = $modeSelectorStore.selectedMode === 'trust' &&
           $analyseOptions.selectedOrganisations?.length > 0 &&
           !currentModeHasData &&
           $resultsStore.showPercentiles;

    let colorMappings = new Map();


    function showShareFeedback(message, variant = 'success') {
        shareToastMessage = message;
        shareToastVariant = variant;
        showShareToast = true;

        if (shareToastTimeout) {
            clearTimeout(shareToastTimeout);
        }

        shareToastTimeout = setTimeout(() => {
            showShareToast = false;
            shareToastTimeout = null;
        }, 2500);
    }

    async function handleCopyAnalysisLink() {
        if (isCopyingShareLink) return;

        isCopyingShareLink = true;

        try {
            const currentUrl = getCurrentUrl();
            await copyToClipboard(currentUrl);
            showShareFeedback('Analysis link copied to clipboard!', 'success');
        } catch (error) {
            console.error('Failed to copy analysis link:', error);
            showShareFeedback('Could not copy link. Please try again.', 'error');
        } finally {
            isCopyingShareLink = false;
        }
    }

    onDestroy(() => {
        if (shareToastTimeout) {
            clearTimeout(shareToastTimeout);
        }
    });


    function processChartData(data) {
        const processor = new ChartDataProcessor(
            data,
            $resultsStore.aggregatedData,
            {
                selectedOrganisations: $analyseOptions.selectedOrganisations,
                predecessorMap: $organisationSearchStore.predecessorMap,
                showPercentiles: $resultsStore.showPercentiles !== false
            }
        );

        const { datasets: chartDatasets, maxValue, needsPercentiles, percentilesData } = processor.processMode($modeSelectorStore.selectedMode);
        const combinedUnits = processor.getCombinedUnits();

        let finalDatasets = chartDatasets;
        
        if ($modeSelectorStore.selectedMode === 'trust' && needsPercentiles && $resultsStore.showPercentiles) {
            const percentilesResult = calculatePercentiles(
                percentilesData, 
                $organisationSearchStore.predecessorMap,
                $organisationSearchStore.items
            );
            
            const trustCount = getTrustCount(percentilesResult);

            resultsStore.update(store => ({
                ...store,
                percentiles: percentilesResult.percentiles,
                trustCount,
                excludedTrusts: percentilesResult.excludedTrusts
            }));

            if (percentilesResult.percentiles.length > 0) {
                const percentileDatasets = processor.createPercentileDatasets(percentilesResult.percentiles);
                finalDatasets = [...percentileDatasets, ...chartDatasets];
            }
        }

        let yAxisLabel;
        if (processor.uniqueUnits.length === 1) {
            const unit = processor.uniqueUnits[0];

            if (unit && unit.startsWith('DDD (')) {
                yAxisLabel = 'DDDs';
            } else if (unit === 'DDD') {
                yAxisLabel = 'DDDs';
            } else {
                yAxisLabel = pluralize(unit);
            }
        } else if (processor.uniqueUnits.length > 1) {

            const allDDD = processor.uniqueUnits.every(unit => unit && unit.startsWith('DDD ('));
            if (allDDD) {
                yAxisLabel = 'DDDs';
            } else {
                yAxisLabel = combinedUnits;
            }
        } else {
            yAxisLabel = 'units';
        }

        const chartConfig = {
            mode: $modeSelectorStore.selectedMode,
            yAxisLabel: yAxisLabel,
            yAxisRange: maxValue > 0 ? [0, maxValue] : [0, 100],
            visibleItems: new Set(finalDatasets.map(d => d.label)),
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
                const pluralizedUnit = formatUnitForTooltip(baseUnit, value);
                return formatNumber(value, { showUnit: true, unit: pluralizedUnit });
            }
        };

        const chartData = {
            labels: processor.allDates,
            datasets: finalDatasets
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
                    ingredients: Array.isArray(vmp.ingredients) ? vmp.ingredients : (vmp.ingredients || []),
                    vtm: vmp.vtm || '',
                }));

            const availableCodes = new Set(vmps.map(vmp => String(vmp.code ?? '')));
            const currentExcluded = Array.isArray($resultsStore.excludedVmps) ? $resultsStore.excludedVmps : [];
            const filteredExcluded = currentExcluded.filter(code => availableCodes.has(String(code)));

            resultsStore.update(store => ({
                ...store,
                analysisData: selectedData,
                showResults: true,
                searchType: data.searchType || $analyseOptions.searchType,
                quantityType: data.quantityType || $analyseOptions.quantityType,
                excludedVmps: filteredExcluded
            }));

            const vmpsWithValidData = vmps.filter(vmp => vmp.unit !== 'nan');

            viewModeCalculator = new ViewModeCalculator(
                $resultsStore,
                $analyseOptions,
                $organisationSearchStore,
                vmpsWithValidData
            );

            viewModes = viewModeCalculator.calculateAvailableModes();

            if (viewModes.length > 0) {
                const hasSelectedOrganisations = $analyseOptions.selectedOrganisations && 
                                               $analyseOptions.selectedOrganisations.length > 0;
                const availableModeValues = viewModes.map(mode => mode.value);
                let nextMode = null;

                if (!isModeFromUrlApplied) {
                    if (modeFromUrl && availableModeValues.includes(modeFromUrl)) {
                        nextMode = modeFromUrl;
                    }

                    isModeFromUrlApplied = true;
                    modeFromUrl = null;
                }

                if (!nextMode) {
                    const currentMode = $modeSelectorStore.selectedMode;
                    if (currentMode && availableModeValues.includes(currentMode)) {
                        nextMode = currentMode;
                    } else {
                        nextMode = selectDefaultMode(viewModes, hasSelectedOrganisations);
                    }
                }
                
                modeSelectorStore.setSelectedMode(nextMode);
            } else {
                modeSelectorStore.setSelectedMode(null);
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

        // Recalculate available view modes based on selected VMPs
        const filteredVMPs = vmps.filter(vmp => 
            selectedVMPs.some(selectedVmp => selectedVmp.vmp === vmp.vmp)
        );

        const vmpsWithValidData = filteredVMPs.filter(vmp => vmp.unit !== 'nan');

        if (vmpsWithValidData.length > 0) {
            viewModeCalculator = new ViewModeCalculator(
                $resultsStore,
                $analyseOptions,
                $organisationSearchStore,
                vmpsWithValidData
            );

            const newViewModes = viewModeCalculator.calculateAvailableModes();
            viewModes = newViewModes;

            // Check if current mode is still available, if not select new default
            const currentModeStillAvailable = newViewModes.some(mode => 
                mode.value === $modeSelectorStore.selectedMode
            );

            if (!currentModeStillAvailable && newViewModes.length > 0) {
                const hasSelectedOrganisations = $analyseOptions.selectedOrganisations && 
                                               $analyseOptions.selectedOrganisations.length > 0;
                const newDefaultMode = selectDefaultMode(newViewModes, hasSelectedOrganisations);
                modeSelectorStore.setSelectedMode(newDefaultMode);
            }
        } else {
            viewModes = [];
            modeSelectorStore.setSelectedMode(null);
        }

        const chartData = processChartData(filteredData);

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
        
        const dataToProcess = $resultsStore.filteredData && $resultsStore.filteredData.length > 0 ? 
            $resultsStore.filteredData : selectedData;
        
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

    function formatUnitForTooltip(baseUnit, value) {
        if (!baseUnit) return 'units';
        
        // Handle DDD units with dose information like "DDD (60.0mg)"
        if (baseUnit.startsWith('DDD (') && baseUnit.includes(')')) {
            const doseMatch = baseUnit.match(/^DDD (\(.+\))$/);
            if (doseMatch) {
                const doseInfo = doseMatch[1];
                return value === 1 ? `DDD ${doseInfo}` : `DDDs ${doseInfo}`;
            }
        }
        
        // Handle plain "DDD" units
        if (baseUnit === 'DDD') {
            return value === 1 ? 'DDD' : 'DDDs';
        }
        
        return pluralize(baseUnit, value);
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
            
            unit = formatUnitForTooltip(baseUnit, value);
        } else {
            const baseUnit = chartConfig?.yAxisLabel || 'units';
            unit = formatUnitForTooltip(baseUnit, value);
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


    function handlePercentileToggle() {
        if ($modeSelectorStore.selectedMode !== 'trust' || !($analyseOptions.selectedOrganisations?.length > 0)) {
            return;
        }
        resultsStore.update(store => ({
            ...store,
            showPercentiles: !store.showPercentiles
        }));
    }

    $: chartExplainerText = getChartExplainerText($modeSelectorStore.selectedMode, {
        hasSelectedOrganisations: $analyseOptions.selectedOrganisations?.length > 0,
        currentModeHasData,
        vmpsCount: vmps.length
    });
</script>

{#if showResults || (urlValidationErrors && urlValidationErrors.length > 0)}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col {className}">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if urlValidationErrors && urlValidationErrors.length > 0}
                <div class="p-4 border-b border-gray-200 bg-yellow-50">
                    <div class="flex items-start">
                        <div class="flex-shrink-0">
                            <svg class="h-5 w-5 text-yellow-400" viewBox="0 0 20 20" fill="currentColor">
                                <path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
                            </svg>
                        </div>
                        <div class="ml-3">
                            <h3 class="text-sm font-medium text-yellow-800">URL Parameter Issues</h3>
                            <div class="mt-2 text-sm text-yellow-700">
                                <ul class="list-disc pl-5 space-y-1">
                                    {#each urlValidationErrors as error}
                                        <li>{error}</li>
                                    {/each}
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
            {/if}
            {#if isAnalysisRunning}
                <div class="flex items-center justify-center h-[500px] p-16">
                    <div class="flex items-center">
                        <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-oxford-600"></div>
                        <span class="ml-3 text-sm text-oxford-600">Running analysis...</span>
                    </div>
                </div>
            {:else if selectedData.length > 0}
                <div class="space-y-6 p-6">
                    <section class="bg-white rounded-lg p-4 border-2 border-oxford-300 shadow-sm">
                        <ProductsTable {vmps} {excludedVmps} on:dataFiltered={handleFilteredData} />
                    </section>
                    
                    {#if viewModes.length > 0}
                    <section class="p-4">
                        <div class="mb-4">
                            <div class="flex flex-col sm:items-end mb-2">
                                <button
                                    type="button"
                                    on:click={handleCopyAnalysisLink}
                                    class="flex items-center gap-2 px-3 py-2 text-sm font-medium text-oxford-600 bg-white border border-oxford-200 rounded-md hover:bg-oxford-50 hover:border-oxford-300 transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-400 disabled:opacity-70 disabled:cursor-not-allowed"
                                    disabled={isCopyingShareLink}
                                    title="Copy link to share this analysis with current selections"
                                >
                                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-4 h-4">
                                        <path stroke-linecap="round" stroke-linejoin="round" d="M7.217 10.907a2.25 2.25 0 100 2.186m0-2.186c.18.324.283.696.283 1.093s-.103.77-.283 1.093m0-2.186l9.566-5.314m-9.566 7.5l9.566 5.314m0 0a2.25 2.25 0 103.935 2.186 2.25 2.25 0 00-3.935-2.186zm0-12.814a2.25 2.25 0 103.935-2.186 2.25 2.25 0 00-3.935 2.186z" />
                                    </svg>
                                    {isCopyingShareLink ? 'Copying...' : 'Share analysis'}
                                </button>
                            </div>
                            <div class="flex flex-col sm:flex-row sm:items-center gap-4">
                                <ModeSelector 
                                    options={viewModes}
                                    initialMode={selectDefaultMode(viewModes, $analyseOptions.selectedOrganisations?.length > 0)}
                                    label="View Mode"
                                    variant="pill"
                                />

                                {#if $modeSelectorStore.selectedMode === 'trust'}
                                <div class="flex flex-col items-center gap-2">
                                    <span class="text-sm text-gray-600 leading-tight text-center">
                                        Show percentiles
                                    </span>
                                    <div class="flex items-center gap-2">
                                        <label class="inline-flex items-center {$analyseOptions.selectedOrganisations?.length > 0 ? 'cursor-pointer' : 'cursor-not-allowed opacity-50'}">
                                            <input
                                                type="checkbox"
                                                class="sr-only peer"
                                                checked={$resultsStore.showPercentiles}
                                                disabled={!$analyseOptions.selectedOrganisations?.length}
                                                on:change={handlePercentileToggle}
                                            />
                                            <div class="relative w-9 h-5 bg-gray-200 peer-focus:outline-none {$analyseOptions.selectedOrganisations?.length > 0 ? 'peer-focus:ring-2 peer-focus:ring-blue-500' : ''} rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600 {$analyseOptions.selectedOrganisations?.length > 0 ? '' : 'peer-disabled:opacity-50'}"></div>
                                        </label>
                                        <div class="relative inline-block group">
                                            <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                                                <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                                                </svg>
                                            </button>
                                            <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                                                        group-hover:scale-100 w-[280px] -translate-x-1/2 left-1/2 sm:-translate-x-full sm:left-5 top-5 rounded-md shadow-lg bg-white 
                                                        ring-1 ring-black ring-opacity-5 p-4">
                                                <p class="text-sm text-gray-500">
                                                    {#if $analyseOptions.selectedOrganisations?.length > 0}
                                                        Percentiles show variation in product quantity across NHS Trusts and allow easy comparison of Trust activity relative to the median Trust level. See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret them.
                                                    {:else}
                                                        Percentiles are always shown when no trusts are selected. Select trusts to enable this toggle. See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                                                    {/if}
                                                </p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                {/if}
                            </div>
                        </div>

                        {#if currentModeHasData || canShowPercentilesWithoutTrustData}
                        <div class="mb-4">
                            <p class="text-sm text-gray-700">
                                {#if $modeSelectorStore.selectedMode === 'trust' && $resultsStore.showPercentiles}
                                    {#if $analyseOptions.selectedOrganisations?.length > 0}
                                        {#if currentModeHasData}
                                            This chart shows individual NHS Trust quantities overlaid on percentile ranges. Selected trusts appear as colored lines, while percentile bands show the distribution across all trusts with data.
                                        {:else}
                                            This chart shows percentile ranges across all NHS Trusts with data. The selected trusts have no data for these products, but percentile bands show the distribution across all trusts with data.
                                        {/if}
                                    {:else}
                                        This chart shows percentile ranges across all NHS Trusts with data for the selected products. The bands represent the variation in quantities across trusts.
                                    {/if}
                                    {#if $resultsStore.trustCount > 0}
                                        Trusts are only included if they have issued any of the selected products during the time period. For the selected products above, this is <strong>{$resultsStore.trustCount}/{$organisationSearchStore.items.length} trusts</strong>
                                        {#if $resultsStore.excludedTrusts && $resultsStore.excludedTrusts.length > 0}
                                            <button
                                                type="button"
                                                class="text-blue-600 hover:text-blue-800 underline text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-1 rounded"
                                                on:click={() => showTrustCountDetails = !showTrustCountDetails}
                                            >
                                                ({showTrustCountDetails ? 'hide' : 'show'} excluded trusts)
                                            </button>
                                        {/if}
                                        .
                                    {/if}
                                    Variation can reflect differences in hospital size rather than genuine variation. 
                                    See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret this chart.
                                {:else}
                                    {chartExplainerText}
                                    See <a href="/faq/" class="underline font-semibold" target="_blank">the FAQs</a> for more details about interpreting charts.
                                {/if}
                            </p>
                            
                            {#if $modeSelectorStore.selectedMode === 'trust' && $resultsStore.showPercentiles && showTrustCountDetails && $resultsStore.excludedTrusts && $resultsStore.excludedTrusts.length > 0}
                            <div class="mt-3 p-3 bg-gray-50 border border-gray-200 rounded">
                                <div class="text-sm text-gray-700 font-medium mb-2">
                                    Excluded trusts ({$resultsStore.excludedTrusts.length}):
                                </div>
                                <div class="max-h-32 overflow-y-auto bg-white border border-gray-200 rounded p-3">
                                    <div class="space-y-1">
                                        {#each $resultsStore.excludedTrusts as trust}
                                        <div class="text-xs text-gray-600">
                                            {trust.name}
                                        </div>
                                        {/each}
                                    </div>
                                </div>
                            </div>
                            {/if}

                        </div>
                        {/if}

                        {#if showShareToast}
                            <div class="fixed bottom-4 right-4 z-50" aria-live="polite">
                                <div class={`px-4 py-2 rounded-lg shadow-lg border transform translate-y-0 opacity-100 transition-all duration-300 ${shareToastVariant === 'success' ? 'bg-oxford-50 text-oxford-800 border-oxford-100' : 'bg-red-50 text-red-800 border-red-200'}`}>
                                    <div class="flex items-center gap-2">
                                        {#if shareToastVariant === 'success'}
                                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                                                <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                                            </svg>
                                        {:else}
                                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                                                <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m0 3.75h.007v.008H12v-.008z" />
                                                <path stroke-linecap="round" stroke-linejoin="round" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                                            </svg>
                                        {/if}
                                        <span class="text-sm font-medium">{shareToastMessage}</span>
                                    </div>
                                </div>
                            </div>
                        {/if}
                    </section>
                    <section>
                    {#if isInTrustModeWithNoData}
                            <div class="flex items-center justify-center h-[550px] border-2 border-dashed border-gray-300 rounded-lg p-8 bg-gray-50">
                                <div class="">
                                    <div class="text-center space-y-6">
                                        <div>
                                            <p class="text-oxford-600 text-xl font-medium mb-3">No data for selected trusts</p>
                                            <p class="text-gray-600 text-base max-w-md mb-4">
                                                The selected NHS Trusts have no data for the chosen products.
                                            </p>
                                            <p class="text-gray-600 text-base max-w-md">
                                                Try <strong>turning on percentiles</strong>, to see variation across all trusts that do have data, or <strong>select more trusts</strong> in the analysis builder. 
                                                <a href="/faq/#why-is-there-no-quantity-for-some-products" class="link-oxford" target="_blank">
                                                    Learn more about why quantities might be missing
                                                </a>.
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        {:else if currentModeHasData || canShowPercentilesWithoutTrustData}
                            <div class="grid grid-cols-1 gap-4">
                                <div class="relative h-[550px] mb-8">
                                    <Chart 
                                        store={resultsChartStore} 
                                        data={filteredData.length > 0 ? filteredData : selectedData}
                                        formatTooltipContent={customTooltipFormatter}
                                        exportData={{
                                            data: $resultsStore.filteredData && $resultsStore.filteredData.length > 0
                                                ? $resultsStore.filteredData
                                                : analysisData,
                                            excludedVmps: $resultsStore.excludedVmps || [],
                                            selectedTrusts: $analyseOptions.selectedOrganisations || null,
                                            percentilesData: $resultsStore.percentiles || [],
                                            predecessorMap: $organisationSearchStore.predecessorMap || new Map(),
                                        }}
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
                                            <a href="/faq/#why-is-there-no-quantity-for-some-products" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                                                Learn more about why quantities might be missing
                                            </a>.
                                        </p>
                                    </div>
                                </div>
                            </div>
                        {/if}
                    </section>

                    <section class="bg-amber-50 border-l-4 border-amber-400 p-4 mx-4 mb-12 mt-16 relative z-10">
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
                                        <a href="/faq/#why-is-there-no-quantity-for-some-products" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
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
