<svelte:options customElement={{
    tag: 'measure-component',
    shadow: 'none'
}} />

<script>
    import { onMount,onDestroy } from 'svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from './ModeSelector.svelte';
    import ChartLegend from './ChartLegend.svelte';
    import MeasureChart from './MeasureChart.svelte';

    export let measureData = '[]';
    export let deciles = '{}';

    let parsedData = [];
    let chartContainer;
    let resizeObserver;
    let organisations = [];
    let selectedItems = [];
    let usedOrganisationSelection = false;
    let selectedMode = 'organisation';
    let regions = [];
    let icbs = [];
    let legendItems = [];
    let filteredData = parsedData;
    let chartWidth = 0;
    let chartHeight = 0;

    const regionColors = {
        'East Of England': '#1f77b4',
        'South East': '#ff7f0e',
        'Midlands': '#2ca02c',
        'North East And Yorkshire': '#d62728',
        'London': '#9467bd',
        'South West': '#8c564b',
        'North West': '#e377c2'
    };

    $: {
        try {
            if (typeof measureData === 'string') {
                let unescapedData = measureData.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                parsedData = JSON.parse(unescapedData);
            } else {
                parsedData = measureData;
            }
            organisations = [...new Set(parsedData.map(item => item.organisation))];
        } catch (e) {
            console.error('Error parsing measureData:', e);
            parsedData = [];
        }

        try {
            if (typeof deciles === 'string') {
                let unescapedDeciles = deciles.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                deciles = JSON.parse(unescapedDeciles);
            } 
        } catch (e) {
            console.error('Error parsing deciles:', e);
            deciles = {};
        }

        regions = [...new Set(parsedData.map(item => item.region))];
        icbs = [...new Set(parsedData.map(item => item.icb))];

        filteredData = filterData(parsedData, selectedMode, selectedItems, usedOrganisationSelection);

        legendItems = createLegendItems(selectedMode, regionColors);
    }

    function filterData(data, mode, items, used) {
        if (mode === 'organisation' || mode === 'icb' || mode === 'region') {
            if (used && items.length > 0) {
                return data.filter(item => 
                    mode === 'organisation' ? items.includes(item.organisation) :
                    mode === 'icb' ? items.includes(item.icb) :
                    items.includes(item.region)
                );
            }
        } else if (mode === 'deciles' && used && items.length > 0) {
            return data.filter(item => items.includes(item.organisation));
        }
        return data;
    }

    function createLegendItems(mode, colors) {
        if (mode === 'deciles') {
            return [
                { label: '1st-9th Percentile', style: 'border-blue-500 border-dotted' },
                { label: '10th-90th Percentile', style: 'border-blue-500 border-dashed' },
                { label: '50th Percentile', style: 'border-red-500 border-dashed' }
            ];
        } else if (mode === 'region') {
            return Object.entries(colors).map(([region, color]) => ({
                label: region,
                style: `background-color: ${color}`
            }));
        }
        return [];
    }

    function getColor(key, index, total) {
        if (selectedMode === 'region') {
            return regionColors[key] || `hsl(${index * 360 / total}, 70%, 50%)`;
        } else {
            return `hsl(${index * 360 / total}, 70%, 50%)`;
        }
    }

    function prepareChartData(data) {
        if (data.length === 0) {
            return { labels: [], datasets: [] };
        }

        let filteredData;
        let breakdownKeys;

        if (selectedMode === 'organisation' || selectedMode === 'icb' || selectedMode === 'region') {
            if (usedOrganisationSelection) {
                if (selectedItems.length > 0) {
                    filteredData = data.filter(item => 
                        selectedMode === 'organisation' ? selectedItems.includes(item.organisation) :
                        selectedMode === 'icb' ? selectedItems.includes(item.icb) :
                        selectedItems.includes(item.region)
                    );
                    breakdownKeys = selectedItems;
                } else {
                    return { labels: [], datasets: [] };
                }
            } else {
                filteredData = data;
                breakdownKeys = selectedMode === 'organisation' ? organisations :
                                selectedMode === 'icb' ? icbs :
                                regions;
            }

            const groupedData = filteredData.reduce((acc, item) => {
                const key = item.month;
                if (!acc[key]) {
                    acc[key] = {};
                }
                const breakdownKey = selectedMode === 'organisation' ? item.organisation :
                                     selectedMode === 'icb' ? item.icb :
                                     item.region;
                if (!acc[key][breakdownKey]) {
                    acc[key][breakdownKey] = { sum: 0, count: 0 };
                }
                acc[key][breakdownKey].sum += parseFloat(item.quantity);
                acc[key][breakdownKey].count += 1;
                return acc;
            }, {});

            const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));

            return {
                labels: sortedDates,
                datasets: breakdownKeys.map((key, index) => {
                    const dataPoints = sortedDates.map(date => {
                        if (!groupedData[date] || !groupedData[date][key]) {
                            return 0;
                        }
                        const { sum, count } = groupedData[date][key];
                        return count > 0 ? sum / count : 0;
                    });
                    return {
                        label: key,
                        data: dataPoints,
                        color: getColor(key, index, breakdownKeys.length),
                        strokeWidth: 2
                    };
                })
            };
        } else if (selectedMode === 'deciles') {
            const sortedDates = Object.keys(deciles).sort((a, b) => new Date(a) - new Date(b));
    
            const decileDatasets = Array.from({ length: 27 }, (_, i) => {
                let label;
                let color;
                let strokeWidth;
                let strokeDasharray;
                
                if (i < 9) {
                    label = `${i + 1}th Percentile`;
                    color = 'blue';
                    strokeWidth = 1;
                    strokeDasharray = '2,2'; // Dotted blue
                } else if (i < 18 && i !== 13) {
                    label = `${(i - 9 + 1) * 10}th Percentile`;
                    color = 'blue';
                    strokeWidth = 2;
                    strokeDasharray = '4,2'; // Dashed blue
                } else if (i === 13) { // 50th percentile
                    label = '50th Percentile';
                    color = 'red';
                    strokeWidth = 4;
                    strokeDasharray = '4,2'; // Dashed red
                } else {
                    label = `${i - 17 + 90}th Percentile`;
                    color = 'blue';
                    strokeWidth = 1;
                    strokeDasharray = '2,2'; // Dotted blue
                }

                return {
                    label: label,
                    data: sortedDates.map(date => deciles[date][i] || 0),
                    color: color,
                    strokeWidth: strokeWidth,
                    strokeDasharray: strokeDasharray
                };
            });

            if (usedOrganisationSelection && selectedItems.length > 0) {
                const filteredData = data.filter(item => selectedItems.includes(item.organisation));
                const groupedData = filteredData.reduce((acc, item) => {
                    const key = item.month;
                    if (!acc[key]) {
                        acc[key] = {};
                    }
                    const breakdownKey = item.organisation;
                    if (!acc[key][breakdownKey]) {
                        acc[key][breakdownKey] = 0;
                    }
                    acc[key][breakdownKey] += parseFloat(item.quantity);
                    return acc;
                }, {});

                const organisationDatasets = selectedItems.map((key, index) => ({
                    label: key,
                    data: sortedDates.map(date => groupedData[date][key] || 0),
                    color: `hsl(${index * 360 / selectedItems.length}, 70%, 50%)`,
                    strokeWidth: 3 // Thicker lines for selected organisations
                }));

                return {
                    labels: sortedDates,
                    datasets: [...decileDatasets, ...organisationDatasets]
                };
            }

            return {
                labels: sortedDates,
                datasets: decileDatasets
            };
        } else if (selectedMode === 'region' || selectedMode === 'icb') {
            filteredData = data;
            breakdownKeys = selectedMode === 'region' ? Object.keys(regionColors) : icbs;

            const groupedData = filteredData.reduce((acc, item) => {
                const key = item.month;
                if (!acc[key]) {
                    acc[key] = {};
                }
                const breakdownKey = selectedMode === 'region' ? item.region : item.icb;
                if (!acc[key][breakdownKey]) {
                    acc[key][breakdownKey] = { sum: 0, count: 0 };
                }
                acc[key][breakdownKey].sum += parseFloat(item.quantity);
                acc[key][breakdownKey].count += 1;
                return acc;
            }, {});

            const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));

            return {
                labels: sortedDates,
                datasets: breakdownKeys.map((key, index) => {
                    const dataPoints = sortedDates.map(date => {
                        if (!groupedData[date] || !groupedData[date][key]) {
                            return 0;
                        }
                        const { sum, count } = groupedData[date][key];
                        return count > 0 ? sum / count : 0;
                    });
                    return {
                        label: key,
                        data: dataPoints,
                        color: getColor(key, index, breakdownKeys.length),
                        strokeWidth: 2
                    };
                })
            };
        }
    }

    function handleItemSelection(event) {
        selectedItems = event.detail.selectedItems;
        usedOrganisationSelection = event.detail.usedOrganisationSelection;
    }

    function handleModeChange(event) {
        selectedMode = event.target.value;
        // Reset selection when mode changes, except for 'deciles'
        if (selectedMode !== 'deciles') {
            selectedItems = [];
            usedOrganisationSelection = false;
        }
    }

    function handleResize() {
        if (chartContainer) {
            chartWidth = chartContainer.clientWidth;
            chartHeight = chartContainer.clientHeight;
        }
    }

    onMount(() => {
        resizeObserver = new ResizeObserver(handleResize);
        if (chartContainer) {
            resizeObserver.observe(chartContainer);
        }
        handleResize();
    });

    onDestroy(() => {
        if (resizeObserver) {
            resizeObserver.disconnect();
        }
    });
</script>

<div class="flex flex-col">
    <div class="flex flex-wrap justify-between items-end mb-4">
        <div class="flex-grow mr-4 relative">
            <OrganisationSearch 
                items={selectedMode === 'deciles' ? organisations :
                       selectedMode === 'organisation' ? organisations : 
                       selectedMode === 'icb' ? icbs : 
                       regions}
                on:selectionChange={handleItemSelection}
                overlayMode={true}
                filterType={selectedMode === 'deciles' ? 'organisation' : selectedMode}
            />
        </div>
        <ModeSelector
            selectedMode={selectedMode}
            handleModeChange={handleModeChange}
        />
    </div>

    <div class="flex-grow relative" style="min-height: 400px;">
        <div bind:this={chartContainer} class="chart-container absolute inset-0">
            {#if parsedData.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <MeasureChart 
                    chartData={prepareChartData(filteredData)}
                    {selectedMode}
                    width={chartWidth}
                    height={chartHeight}
                />
            {/if}
        </div>
    </div>

    <ChartLegend
        legendItems={legendItems}
        selectedMode={selectedMode}
    />
</div>