<svelte:options customElement={{
    tag: 'measure-component',
    props: {
        orgdata: { type: 'String', reflect: true },
        regiondata: { type: 'String', reflect: true },
        icbdata: { type: 'String', reflect: true },
        percentiledata: { type: 'String', reflect: true }
    },
    shadow: 'none'
}} />

<script>
    import { onMount } from 'svelte';
    import * as d3 from 'd3';
    import { createChartStore } from '../../stores/chartStore.js';
    import { 
        selectedMode, 
        orgdata as orgdataStore, 
        regiondata as regionStore, 
        icbdata as icbStore, 
        percentiledata as percentileStore, 
        visibleRegions,
        visibleTrusts,
        visibleICBs,
        getOrAssignColor,
        showPercentiles
    } from '../../stores/measureChartStore.js';
    import Chart from '../common/Chart.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from '../common/ModeSelector.svelte';
    import ChartLegend from '../common/ChartLegend.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';
    import { modeSelectorStore } from '../../stores/modeSelectorStore.js';
    import { regionColors } from '../../utils/chartConfig.js';
    import { filteredData } from '../../stores/measureChartStore.js';
    import { legendStore } from '../../stores/legendStore.js';

    export let orgdata = '[]';
    export let regiondata = '[]';
    export let icbdata = '[]';
    export let percentiledata = '[]';
   
    let trusts = [];
    let icbs = [];
    let regions = [];

    $: showFilter = ['percentiles', 'icb', 'region', 'national'].includes($selectedMode);

    $: showLegend = $selectedMode === 'percentiles' || 
                    $selectedMode === 'region' || 
                    $selectedMode === 'icb' ||
                    $selectedMode === 'national';

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
        yAxisLabel: '%',
        yAxisBehavior: {
            forceZero: true,
            resetToInitial: true,
            fixedRange: true
        },
        yAxisRange: [0, 100],
        percentileConfig: {
            medianColor: '#DC3220',
            rangeColor: 'rgb(0, 90, 181)'
        }
    });

    onMount(() => {
        const parsedOrgData = JSON.parse(orgdata);
        orgdataStore.set(parsedOrgData);
        
        measureChartStore.setDimensions({
            height: 350,
            margin: { top: 10, right: 20, bottom: 30, left: 80 }
        });
        
        // Get all trusts and available trusts
        trusts = Object.keys(parsedOrgData);
        const availableTrusts = trusts.filter(trust => parsedOrgData[trust].available);
        
        organisationSearchStore.setItems(trusts);
        organisationSearchStore.setAvailableItems(availableTrusts);
        organisationSearchStore.setFilterType('trust');
        
        const parsedIcbData = JSON.parse(icbdata);
        icbStore.set(parsedIcbData);
        icbs = parsedIcbData.map(icb => icb.name);
        
        const parsedRegionData = JSON.parse(regiondata);
        regionStore.set(parsedRegionData);
        regions = parsedRegionData.map(region => region.name);
        
        percentileStore.set(JSON.parse(percentiledata));
        selectedMode.set('percentiles');

        visibleICBs.set(new Set(icbs));
        visibleRegions.set(new Set(regions));
        visibleTrusts.set(new Set());
        
        if ($selectedMode === 'icb') {
            organisationSearchStore.updateSelection(Array.from($visibleICBs));
        } else if ($selectedMode === 'region') {
            organisationSearchStore.updateSelection(Array.from($visibleRegions));
        } else if ($selectedMode === 'percentiles') {
            organisationSearchStore.updateSelection(Array.from($visibleTrusts));
        }
    });

    function handleSelectionChange(event) {
        const { selectedItems, source } = event.detail;
 
        if ($selectedMode === 'icb') {
            visibleICBs.set(new Set(selectedItems));
            organisationSearchStore.updateSelection(selectedItems);
            legendStore.setVisibleItems(selectedItems);
            
            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => ({
                    ...dataset,
                    hidden: !selectedItems.includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        } else if ($selectedMode === 'region') {
            visibleRegions.set(new Set(selectedItems));
            organisationSearchStore.updateSelection(selectedItems);
            legendStore.setVisibleItems(selectedItems);

            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => ({
                    ...dataset,
                    hidden: !selectedItems.includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        } else if ($selectedMode === 'percentiles') {
            visibleTrusts.set(new Set(selectedItems));
            organisationSearchStore.updateSelection(selectedItems);
            if ($selectedMode === 'percentiles') {
                legendStore.setVisibleItems([
                    'Median (50th Percentile)',
                    'Percentile Range',
                    ...selectedItems
                ]);
            } else {
                legendStore.setVisibleItems(selectedItems);
            }
            measureChartStore.updateVisibleItems(new Set(selectedItems));
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
                legendStore.setVisibleItems(Array.from($visibleICBs));
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
                legendStore.setVisibleItems(Array.from($visibleRegions));
            } else if (currentMode === 'percentiles') {
                organisationSearchStore.setItems(trusts);
                organisationSearchStore.setFilterType('trust');
                const availableTrusts = trusts.filter(trust => $orgdataStore[trust]?.available);
                organisationSearchStore.setAvailableItems(availableTrusts);

                organisationSearchStore.updateSelection(Array.from($visibleTrusts));
                measureChartStore.updateVisibleItems(new Set($visibleTrusts));
                if (currentMode === 'percentiles') {
                    legendStore.setVisibleItems([
                        'Median (50th Percentile)',
                        'Percentile Range',
                        ...Array.from($visibleTrusts)
                    ]);
                } else {
                    legendStore.setVisibleItems(Array.from($visibleTrusts));
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
                legendStore.setVisibleItems(['National']);
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
                datasets: $filteredData.datasets.map(dataset => ({
                    ...dataset,
                    hidden: newMode === 'national' ? false :
                        newMode === 'region' ? !Array.from($visibleRegions).includes(dataset.label) :
                        newMode === 'icb' ? !Array.from($visibleICBs).includes(dataset.label) :
                        !Array.from($visibleTrusts).includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        }
    }

    $: legendItems = $selectedMode === 'region' ? 
        Array.from($visibleRegions).map(region => ({
            label: region,
            color: regionColors[region],
            visible: true,
            selectable: true
        })) :
        $selectedMode === 'percentiles' ?
            [
                ...$showPercentiles ? [
                    { label: 'Median (50th percentile)', color: '#DC3220', visible: true, selectable: false },
                    { label: '5th-95th percentile', color: 'rgb(0, 90, 181)', visible: true, selectable: false, opacity: 0.1 },
                    { label: '15th-85th percentile', color: 'rgb(0, 90, 181)', visible: true, selectable: false, opacity: 0.2 },
                    { label: '25th-75th percentile', color: 'rgb(0, 90, 181)', visible: true, selectable: false, opacity: 0.4 },
                    { label: '35th-65th percentile', color: 'rgb(0, 90, 181)', visible: true, selectable: false, opacity: 0.6 },
                    { label: '45th-55th percentile', color: 'rgb(0, 90, 181)', visible: true, selectable: false, opacity: 0.8 }
                ] : [],
                ...Array.from($visibleTrusts || []).map(trust => ({
                    label: trust,
                    color: getOrAssignColor(trust),
                    visible: true,
                    selectable: true
                }))
            ] :
        $selectedMode === 'icb' ?
            Array.from($visibleICBs).map(icb => ({
                label: icb,
                color: getOrAssignColor(icb),
                visible: true,
                selectable: true
            })) :
        $selectedMode === 'national' ?
            [{ 
                label: 'National', 
                color: 'rgb(0, 90, 181)',
                visible: true, 
                selectable: false 
            }] :
            [];


    $: legendKey = $selectedMode + Array.from($visibleTrusts).join(',');

    function handleLegendChange(items) {
        const newVisible = new Set(items);

        const updatedData = {
            ...$filteredData,
            datasets: $filteredData.datasets.map(dataset => ({
                ...dataset,
                hidden: $selectedMode === 'percentiles' ?
                    (!$showPercentiles && (
                        dataset.label === 'Median (50th Percentile)' ||
                        dataset.label.includes('th Percentile')
                    )) || (
                        !newVisible.has(dataset.label) && 
                        !dataset.alwaysVisible
                    ) :
                    !newVisible.has(dataset.label)
            }))
        };
        
        measureChartStore.setData(updatedData);
        legendStore.setVisibleItems(items);

    }

    $: {
        if ($filteredData && $selectedMode === 'percentiles') {
            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => ({
                    ...dataset,
                    hidden: (!$showPercentiles && (
                        dataset.label === 'Median (50th Percentile)' ||
                        dataset.label.includes('th Percentile')
                    )) || (
                        !$legendStore.visibleItems.has(dataset.label) && 
                        !dataset.alwaysVisible
                    )
                }))
            };
            measureChartStore.setData(updatedData);
        }
    }

    $: if ($selectedMode) {
        measureChartStore.setConfig({
            ...$measureChartStore.config,
            mode: $selectedMode,
            yAxisBehavior: {
                forceZero: true,
                resetToInitial: true,
                fixedRange: true
            },
            yAxisRange: [0, 100],
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
        
        legendStore.clearVisibleItems();
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

    $: if ($selectedMode === 'percentiles' && $visibleTrusts) {
        legendStore.setVisibleItems([
            'Median (50th Percentile)',
            'Percentile Range',
            ...Array.from($visibleTrusts)
        ]);
    }

    function customTooltipFormatter(d) {
        const label = d.dataset.label || 'No label';
        const date = d3.timeFormat('%b %Y')(d.date);
        const value = (d.value).toFixed(1) + '%';
        const index = d.index;

        const tooltipContent = [
            { text: label, class: 'font-medium' }
        ];

        if ($selectedMode === 'region' || $selectedMode === 'icb' || $selectedMode === 'national') {
            tooltipContent.push(
                { label: 'Date', value: date },
                { label: 'Numerator', value: formatNumber(d.dataset.numerator?.[index]) },
                { label: 'Denominator', value: formatNumber(d.dataset.denominator?.[index]) },
                { label: 'Value', value }
            );
        } else if ($selectedMode === 'percentiles') {
            if (d.dataset.label === 'Median (50th Percentile)') {
                tooltipContent.push(
                    { label: 'Date', value: date },
                    { label: 'Value', value }
                );
            } else if (d.dataset.isTrust || d.dataset.isOrganisation) {
                tooltipContent.push(
                    { label: 'Date', value: date },
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator?.[index]) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator?.[index]) },
                    { label: 'Value', value }
                );
            }
        }

        return tooltipContent;
    }

    function formatNumber(value) {
        if (value == null || isNaN(value)) return value;
        return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }
</script>

<div class="grid grid-cols-1 lg:grid-cols-4 gap-x-4 gap-y-2">

    <div class="col-span-full">
        <div class="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-2">

            {#if showFilter}
                <div class="lg:col-span-3 relative z-10">
                    <div class="w-full h-full flex items-end gap-2">
                        <OrganisationSearch 
                            source={organisationSearchStore}
                            overlayMode={true}
                            on:selectionChange={handleSelectionChange}
                            on:clearAll={handleClearAll}
                            disabled={$selectedMode === 'national'}
                        />
                        {#if $selectedMode === 'percentiles'}
                            <div class="flex items-center gap-2">
                                <span class="text-sm text-gray-600 leading-tight text-center">
                                    Show<br>percentiles
                                </span>
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
                                            Percentiles show variation in this measure across Trusts and allow easy comparison of Trust activity relative to the median Trust level. See <a href="/faq/#percentiles" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret them.
                                        </p>
                                    </div>
                                </div>
                                <label class="inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        class="sr-only peer"
                                        checked={$showPercentiles}
                                        on:change={() => showPercentiles.update(v => !v)}
                                    />
                                    <div class="relative w-9 h-5 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600"></div>
                                </label>
                            </div>
                        {/if}
                    </div>
                </div>
            {:else}
                <div class="lg:col-span-3"></div>
            {/if}

            <div class="lg:col-span-1 flex items-end">
                <div class="w-full">
                    <ModeSelector 
                        options={modeOptions}
                        initialMode="percentiles"
                        label="Select Mode"
                        onChange={handleModeChange}
                        variant="dropdown"
                    />
                </div>
            </div>
        </div>
    </div>

    <div class="lg:col-span-3 relative h-[350px]">
        <div class="chart-container absolute inset-0">
            {#if $orgdataStore.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <Chart 
                    data={$filteredData}
                    mode={$selectedMode}
                    yAxisLabel="%"
                    formatTooltipContent={customTooltipFormatter}
                    store={measureChartStore}
                />
            {/if}
        </div>
    </div>

    {#if showLegend}
        <div class="legend-container lg:h-[350px] overflow-y-auto bg-white">
            <ChartLegend 
                items={legendItems}
                isPercentileMode={$selectedMode === 'percentiles'}
                onChange={handleLegendChange}
            />
        </div>
    {/if}
</div>

<style>
    .legend-container {
        padding: 0.5rem;
    }

    @media (max-width: 1024px) {
        .legend-container {
            max-height: 200px;
        }
    }
</style>
