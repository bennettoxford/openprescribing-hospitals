<svelte:options customElement={{
    tag: 'measure-component',
    props: {
        orgdata: { type: 'String', reflect: true },
        regiondata: { type: 'String', reflect: true },
        icbdata: { type: 'String', reflect: true },
        nationaldata: { type: 'String', reflect: true },
        percentiledata: { type: 'String', reflect: true },
        quantitytype: { type: 'String', reflect: true },
        hasdenominators: { type: 'String', reflect: true },
        denominatorvmps: { type: 'String', reflect: true },
        numeratorvmps: { type: 'String', reflect: true },
        annotations: { type: 'String', reflect: true },
        defaultviewmode: { type: 'String', reflect: true }
    },
    shadow: 'none'
}} />

<script>
    import { onMount } from 'svelte';
    import { createChartStore } from '../../stores/chartStore.js';
    import { 
        selectedMode, 
        orgdata as orgdataStore, 
        regiondata as regionStore, 
        icbdata as icbStore, 
        nationaldata as nationalStore,
        percentiledata as percentileStore,
        visibleRegions,
        visibleTrusts,
        visibleICBs,
        showPercentiles,
        updatePercentilesVisibility,
        getDatasetVisibility
    } from '../../stores/measureChartStore.js';
    import Chart from '../common/Chart.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from '../common/ModeSelector.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';
    import { modeSelectorStore } from '../../stores/modeSelectorStore.js';
    import { filteredData } from '../../stores/measureChartStore.js';
    import { formatNumber } from '../../utils/utils.js';
    import pluralize from 'pluralize';

    export let orgdata = '[]';
    export let regiondata = '[]';
    export let icbdata = '[]';
    export let nationaldata = '[]';
    export let percentiledata = '[]';
    export let quantitytype = 'dose';
    export let hasdenominators = 'true';
    export let denominatorvmps = '[]';
    export let numeratorvmps = '[]';
    export let annotations = '[]';
    export let defaultviewmode = 'percentiles';
   
    let trusts = [];
    let icbs = [];
    let regions = [];
    let uniqueUnits = [];

    function extractUniqueUnits() {
        try {
            const parsedDenominatorVmps = JSON.parse(denominatorvmps || '[]');
            const parsedNumeratorVmps = JSON.parse(numeratorvmps || '[]');
            
            const allProducts = [...parsedDenominatorVmps, ...parsedNumeratorVmps];
            const units = allProducts
                .map(product => product.unit ? pluralize(product.unit) : null)
                .filter(unit => unit && unit !== 'null' && unit !== 'undefined');
            
            return [...new Set(units)].sort();
        } catch (error) {
            console.error('Failed to parse products data:', error);
            return [];
        }
    }

    $: yAxisLabel = getYAxisLabel(quantitytype, hasdenominators, uniqueUnits);
    $: yAxisTickFormatter = getYAxisTickFormatter(quantitytype, hasdenominators);
    $: yAxisLimits = getYAxisLimits(hasdenominators, $filteredData);
    
    function getYAxisLabel(quantityType, hasDenominators, units) {
        const hasDenom = hasDenominators === 'true';
        
        if (hasDenom) {
            return '%';
        } else {
            if (quantityType === 'indicative_cost') {
                return 'Indicative Cost (£)';
            } else {

                if (units && units.length > 0) {
                    const unitsString = units.join(' / ');
                    return `Quantity (${unitsString})`;
                } else {
                    return 'Quantity';
                }
            }
        }
    }

    function getYAxisTickFormatter(quantityType, hasDenominators) {
        const hasDenom = hasDenominators === 'true';
        
        return function(value, range) {
            if (hasDenom) {
                // For measures with denominators, show percentages
                let decimals = 1;
                if (range <= 0.1) decimals = 3;
                else if (range <= 1) decimals = 2;
                return `${value.toFixed(decimals)}%`;
            } else if (quantityType === 'indicative_cost') {
                // For indicative cost, show currency formatting
                if (value === 0) return '£0';
                if (value >= 1000) {
                    return `£${(value / 1000)}K`;
                } else {
                    return `£${value}`;
                }
            } else {
                // For other quantity types, show plain numbers
                if (value === 0) return '0';
                if (value >= 1000) {
                    return `${(value / 1000)}K`;
                } else {
                    return value.toString();
                }
            }
        };
    }

    function getYAxisLimits(hasDenominators, chartData) {
        const hasDenom = hasDenominators === 'true';
        
        if (hasDenom) {
            return [0, 100];
        } else if (chartData && chartData.datasets && chartData.datasets.length > 0) {
            let maxValue = 0;
            
            chartData.datasets.forEach(dataset => {
                if (!dataset.hidden && dataset.data && Array.isArray(dataset.data)) {
                    dataset.data.forEach(value => {
                        if (typeof value === 'object' && value.upper !== undefined) {
                            maxValue = Math.max(maxValue, value.upper);
                        } else if (typeof value === 'number') {
                            maxValue = Math.max(maxValue, value);
                        }
                    });
                }
            });
            return [0, maxValue * 1.1];
        } else {
            return [0, 100];
        }
    }


    $: showFilter = ['percentiles', 'icb', 'region', 'national'].includes($selectedMode);

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.setItems(icbs);
            organisationSearchStore.setFilterType('icb');
        } else if ($selectedMode === 'percentiles') {
            organisationSearchStore.setItems(trusts);
            organisationSearchStore.setFilterType('trust');
        } else if ($selectedMode === 'region') {
            organisationSearchStore.setItems(regions);
            organisationSearchStore.setFilterType('region');
        }
    }

    const measureChartStore = createChartStore({
        mode: 'percentiles',
        yAxisLabel: yAxisLabel,
        yAxisTickFormat: yAxisTickFormatter,
        yAxisRange: yAxisLimits,
        yAxisBehavior: {
            forceZero: true,
            resetToInitial: true,
            fixedRange: hasdenominators === 'true'
        },
        percentileConfig: {
            medianColor: '#DC3220',
            rangeColor: 'rgb(0, 90, 181)'
        }
    });

    $: chartOptions = {
        chart: {
            type: 'line',
            height: 350
        },
        title: {
            text: undefined
        },
        yAxis: hasdenominators === 'true' ? {
        min: 0,
        max: 100,
        allowDecimals: false,
        tickInterval: 20
        } : {
            allowDecimals: true,
            tickAmount: 6,
            endOnTick: true,
            startOnTick: true,
        }
    };

    onMount(() => {
        const parsedOrgData = JSON.parse(orgdata);

        orgdataStore.set(parsedOrgData.data || {});
        

        uniqueUnits = extractUniqueUnits();
        
        measureChartStore.setDimensions({
            height: 500,
            margin: { top: 10, right: 20, bottom: 30, left: 80 }
        });

        trusts = Object.keys(parsedOrgData.data || {});

        const availableTrusts = trusts.filter(trust => parsedOrgData.data[trust].available);
        
        const shouldDisablePercentiles = availableTrusts.length < 30;
        
        organisationSearchStore.setItems(trusts);
        organisationSearchStore.setAvailableItems(availableTrusts);
        organisationSearchStore.setFilterType('trust');
        
        try {
            const predecessorMapObj = new Map(Object.entries(parsedOrgData.predecessor_map || {}));
            organisationSearchStore.setPredecessorMap(predecessorMapObj);
        } catch (error) {
            organisationSearchStore.setPredecessorMap(new Map());
        }
        
        const parsedIcbData = JSON.parse(icbdata);
        icbStore.set(parsedIcbData);
        icbs = parsedIcbData.map(icb => icb.name);
        
        const parsedRegionData = JSON.parse(regiondata);
        regionStore.set(parsedRegionData);
        regions = parsedRegionData.map(region => region.name);
        
        percentileStore.set(JSON.parse(percentiledata));
        selectedMode.set(defaultviewmode);
        modeSelectorStore.setSelectedMode(defaultviewmode);

        visibleICBs.set(new Set(icbs));
        visibleRegions.set(new Set(regions));

        if (shouldDisablePercentiles) {
            visibleTrusts.set(new Set(availableTrusts));
            organisationSearchStore.updateSelection(availableTrusts);
            showPercentiles.set(false);
        } else {
            visibleTrusts.set(new Set());
            organisationSearchStore.updateSelection([]);
        }
        
        if ($selectedMode === 'icb') {
            organisationSearchStore.updateSelection(Array.from($visibleICBs));
        } else if ($selectedMode === 'region') {
            organisationSearchStore.updateSelection(Array.from($visibleRegions));
        } else if ($selectedMode === 'percentiles') {
            organisationSearchStore.updateSelection(Array.from($visibleTrusts));
        }

        const parsedNationalData = JSON.parse(nationaldata);
        nationalStore.set(parsedNationalData);
    });

    function handleSelectionChange(event) {
        const selectedItems = event.detail?.selectedItems || [];
        const source = event.detail?.source;
 
        if ($selectedMode === 'icb') {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : Array.from(selectedItems);
            visibleICBs.set(new Set(itemsArray));
            organisationSearchStore.updateSelection(itemsArray);
            
            const updatedData = {
                ...$filteredData,
                datasets: ($filteredData?.datasets || []).map(dataset => ({
                    ...dataset,
                    hidden: !itemsArray.includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        } else if ($selectedMode === 'region') {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : Array.from(selectedItems);
            visibleRegions.set(new Set(itemsArray));
            organisationSearchStore.updateSelection(itemsArray);

            const updatedData = {
                ...$filteredData,
                datasets: ($filteredData?.datasets || []).map(dataset => ({
                    ...dataset,
                    hidden: !itemsArray.includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        } else if ($selectedMode === 'percentiles') {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : Array.from(selectedItems);
            visibleTrusts.set(new Set(itemsArray));
            organisationSearchStore.updateSelection(itemsArray);
            if ($selectedMode === 'percentiles') {
                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: (!$showPercentiles && (
                            dataset.label === 'Median (50th percentile)' ||
                            dataset.label.includes('th percentile')
                        )) || (
                            !Array.from($visibleTrusts).includes(dataset.label) && 
                            !dataset.alwaysVisible
                        )
                    }))
                };
                measureChartStore.setData(updatedData);
            }
            measureChartStore.updateVisibleItems(new Set(itemsArray));
        }
    }

    const modeOptions = [
        { value: 'percentiles', label: 'NHS Trust' },
        { value: 'icb', label: 'ICB' },
        { value: 'region', label: 'Region' },
        { value: 'national', label: 'National' },
    ];

    $: currentMode = $modeSelectorStore.selectedMode;
    $: {
        if (currentMode) {
            selectedMode.set(currentMode);
            

            if (currentMode === 'icb') {
                organisationSearchStore.setItems(icbs);
                organisationSearchStore.setFilterType('icb');
                organisationSearchStore.setAvailableItems(icbs);
                organisationSearchStore.updateSelection(Array.from($visibleICBs));

                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: !Array.from($visibleICBs).includes(dataset.label)
                    }))
                };
                measureChartStore.setData(updatedData);
            } else if (currentMode === 'region') {
                organisationSearchStore.setItems(regions);
                organisationSearchStore.setFilterType('region');
                organisationSearchStore.setAvailableItems(regions);
                organisationSearchStore.updateSelection(Array.from($visibleRegions));

                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: !Array.from($visibleRegions).includes(dataset.label)
                    }))
                };
                measureChartStore.setData(updatedData);
            } else if (currentMode === 'percentiles') {
                organisationSearchStore.setItems(trusts);
                organisationSearchStore.setFilterType('trust');
                const availableTrusts = trusts.filter(trust => $orgdataStore[trust]?.available);
                organisationSearchStore.setAvailableItems(availableTrusts);

                organisationSearchStore.updateSelection(Array.from($visibleTrusts));
                measureChartStore.updateVisibleItems(new Set($visibleTrusts));
                if (currentMode === 'percentiles') {
                    const updatedData = {
                        ...$filteredData,
                        datasets: $filteredData.datasets.map(dataset => ({
                            ...dataset,
                            hidden: (!$showPercentiles && (
                                dataset.label === 'Median (50th percentile)' ||
                                dataset.label.includes('th percentile')
                            )) || (
                                !Array.from($visibleTrusts).includes(dataset.label) && 
                                !dataset.alwaysVisible
                            )
                        }))
                    };
                    measureChartStore.setData(updatedData);
                }
            } else if (currentMode === 'national') {

                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: false
                    }))
                };
                measureChartStore.setData(updatedData);
            }
        }
    }

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.updateSelection(Array.from($visibleICBs));
        } else if ($selectedMode === 'region') {
            organisationSearchStore.updateSelection(Array.from($visibleRegions));
        } else if ($selectedMode === 'percentiles') {
            organisationSearchStore.updateSelection(Array.from($visibleTrusts));
        }
    }

    function handleModeChange(newMode) {
        selectedMode.set(newMode);

        if ($filteredData) {
            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => {
                    if (newMode === 'percentiles') {
                        return {
                            ...dataset,
                            hidden: !getDatasetVisibility(dataset, newMode, $visibleTrusts, $showPercentiles)
                        };
                    } else {
                        return {
                            ...dataset,
                            hidden: newMode === 'national' ? false :
                                newMode === 'region' ? !$visibleRegions.has(dataset.label) :
                                newMode === 'icb' ? !$visibleICBs.has(dataset.label) :
                                !$visibleTrusts.has(dataset.label)
                        };
                    }
                })
            };
            measureChartStore.setData(updatedData);
        }
    }

    $: {
        if ($filteredData && $selectedMode === 'percentiles') {
            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => ({
                    ...dataset,
                    hidden: (!$showPercentiles && (
                        dataset.label === 'Median (50th percentile)' ||
                        dataset.label.includes('th percentile')
                    )) || (
                        !Array.from($visibleTrusts).includes(dataset.label) && 
                        !dataset.alwaysVisible
                    )
                }))
            };
            measureChartStore.setData(updatedData);
        }
    }

    $: if ($selectedMode) {
        const currentLimits = getYAxisLimits(hasdenominators, $filteredData);
        measureChartStore.setConfig({
            ...$measureChartStore.config,
            mode: $selectedMode,
            yAxisLabel: yAxisLabel,
            yAxisTickFormat: yAxisTickFormatter,
            yAxisRange: currentLimits,
            yAxisBehavior: {
                forceZero: true,
                resetToInitial: true,
                fixedRange: hasdenominators === 'true'
            },
            percentileConfig: {
                medianColor: '#DC3220',
                rangeColor: 'rgb(0, 90, 181)'
            }
        });
    }

    function handleClearAll() {
        if ($selectedMode === 'icb') {
            visibleICBs.set(new Set());
        } else if ($selectedMode === 'region') {
            visibleRegions.set(new Set());
        } else if ($selectedMode === 'percentiles') {
            visibleTrusts.set(new Set());
        }
        
        measureChartStore.updateVisibleItems(new Set());
        organisationSearchStore.updateSelection([]);

        const updatedData = {
            ...$filteredData,
            datasets: $filteredData.datasets.map(dataset => ({
                ...dataset,
                hidden: true
            }))
        };

        measureChartStore.setData(updatedData);
    }

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.setItems(icbs);
            organisationSearchStore.setFilterType('icb');
            organisationSearchStore.setAvailableItems(icbs);
        } else if ($selectedMode === 'region') {
            organisationSearchStore.setItems(regions);
            organisationSearchStore.setFilterType('region');
            organisationSearchStore.setAvailableItems(regions);
        } else if ($selectedMode === 'percentiles') {
            organisationSearchStore.setItems(trusts);
            organisationSearchStore.setFilterType('trust');
            const availableTrusts = trusts.filter(trust => $orgdataStore[trust]?.available);
            organisationSearchStore.setAvailableItems(availableTrusts);
        }
    }

    function customTooltipFormatter(d) {
        const label = d.dataset.name || d.dataset.label || 'No label';
        const date = new Date(d.date);
        const formattedDate = date.toLocaleString('en-GB', { month: 'short', year: 'numeric' });
        
        let value;
        if (hasdenominators === 'true') {
            value = (d.value).toFixed(1) + '%';
        } else if (quantitytype === 'indicative_cost') {
            value = '£' + formatNumber(d.value, { addCommas: true, decimalPlaces: 2 });
        } else {
            value = formatNumber(d.value, { addCommas: true });
        }
        
        const index = d.index;

        const tooltipContent = [
            { text: label, class: 'font-medium' }
        ];

        if ($selectedMode === 'region' || $selectedMode === 'icb' || $selectedMode === 'national') {
            const tooltipEntries = [
                { label: 'Date', value: formattedDate }
            ];
            
            if (hasdenominators === 'true') {
                tooltipEntries.push(
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator?.[index] || 0, { addCommas: true }) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator?.[index] || 0, { addCommas: true }) }
                );
            }
            
            tooltipEntries.push({ label: 'Value', value });
            tooltipContent.push(...tooltipEntries);
        } else if ($selectedMode === 'percentiles') {
            if (d.dataset.label === 'Median (50th percentile)' || d.dataset.name === 'Median (50th percentile)') {
                tooltipContent.push(
                    { label: 'Date', value: formattedDate },
                    { label: 'Value', value }
                );
            } else if (d.dataset.isTrust || d.dataset.isOrganisation) {
                const tooltipEntries = [
                    { label: 'Date', value: formattedDate }
                ];
                
                if (hasdenominators === 'true') {
                    tooltipEntries.push(
                        { label: 'Numerator', value: formatNumber(d.dataset.numerator?.[index] || 0, { addCommas: true }) },
                        { label: 'Denominator', value: formatNumber(d.dataset.denominator?.[index] || 0, { addCommas: true }) }
                    );
                }
                
                tooltipEntries.push({ label: 'Value', value });
                tooltipContent.push(...tooltipEntries);
            }
        }

        return tooltipContent;
    }

    $: percentilesDisabled = trusts.filter(trust => $orgdataStore[trust]?.available).length < 20;

    function handlePercentileToggle() {
        if (!percentilesDisabled) {
            showPercentiles.update(v => !v);
            const updatedData = updatePercentilesVisibility(!$showPercentiles);
            measureChartStore.setData(updatedData);
        }
    }
</script>

<div>
    <div class="flex flex-col md:flex-row justify-between gap-4 px-4 sm:px-8">
        {#if showFilter}
            <div class="w-full md:w-7/12 relative z-10">
                <OrganisationSearch 
                    source={organisationSearchStore}
                    overlayMode={true}
                    on:selectionChange={handleSelectionChange}
                    on:clearAll={handleClearAll}
                    disabled={$selectedMode === 'national'}
                />
            </div>
        {:else}
            <div class="w-full md:w-7/12"></div>
        {/if}

        <div class="w-full md:w-auto flex justify-between items-end gap-4">
            {#if $selectedMode === 'percentiles'}
            <div class="flex flex-col items-center gap-2">
                <span class="text-sm text-gray-600 leading-tight text-center">
                    Show<br>percentiles
                </span>
                <div class="flex items-center gap-2">
                    <label class="inline-flex items-center cursor-pointer {percentilesDisabled ? 'opacity-50 cursor-not-allowed' : ''}">
                        <input
                            type="checkbox"
                            class="sr-only peer"
                            checked={$showPercentiles}
                            on:change={handlePercentileToggle}
                            disabled={percentilesDisabled}
                        />
                        <div class="relative w-9 h-5 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600 {percentilesDisabled ? 'opacity-50' : ''}"></div>
                    </label>
                    <div class="relative inline-block group">
                        <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                            </svg>
                        </button>
                        <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                                    group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                                    ring-1 ring-black ring-opacity-5 p-4">
                            <p class="text-sm text-gray-500">
                                {percentilesDisabled 
                                    ? 'Percentiles are disabled when there are fewer than 30 trusts in a measure.'
                                    : 'Percentiles show variation in this measure across Trusts and allow easy comparison of Trust activity relative to the median Trust level. See <a href="/faq/#percentiles" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret them.'
                                }
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        {/if}
            <div>
                <ModeSelector 
                    options={modeOptions}
                    initialMode={defaultviewmode}
                    label="Select Mode"
                    onChange={handleModeChange}
                    variant="dropdown"
                />
            </div>

        </div>
    </div>

    <div class="lg:col-span-4 relative h-[550px]">
        <div class="chart-container absolute inset-0">
            {#if $orgdataStore.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <Chart 
                    data={$filteredData}
                    mode={$selectedMode}
                    yAxisLabel={yAxisLabel}
                    formatTooltipContent={customTooltipFormatter}
                    store={measureChartStore}
                    {chartOptions}
                    {annotations}
                />
            {/if}
        </div>
    </div>
</div>
