<svelte:options customElement={{
    tag: 'measure-component',
    props: {
        measuredata: { type: 'String', reflect: true },
        decilesdata: { type: 'String', reflect: true }
    },
    shadow: 'none'
}} />

<script>
    import { onMount, onDestroy } from 'svelte';
    import { measureData, deciles, selectedMode, selectedItems, usedOrganisationSelection, filteredData, legendItems, organisations, regions, icbs } from '../../stores/measureChartStore.js';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from './ModeSelector.svelte';
    import ChartLegend from './ChartLegend.svelte';
    import MeasureChart from './MeasureChart.svelte';
    import { chartOptions } from '../../utils/chartConfig.js';

    export let measuredata = '[]';
    export let decilesdata = '{}';

    let chartContainer;
    let resizeObserver;
    let chartWidth = 0;
    let chartHeight = 0;

    $: {
        console.log('Received measuredata:', measuredata);
        console.log('Received decilesdata:', decilesdata);
        
        try {
            if (typeof measuredata === 'string') {
                let unescapedData = measuredata.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                let parsedData = JSON.parse(unescapedData);
                console.log('Parsed measuredata:', parsedData);
                measureData.set(parsedData);
            } else if (Array.isArray(measuredata)) {
                console.log('Setting measuredata directly:', measuredata);
                measureData.set(measuredata);
            } else {
                console.error('Invalid measuredata format');
                measureData.set([]);
            }
        } catch (e) {
            console.error('Error parsing measureData:', e);
            measureData.set([]);
        }

        try {
            if (typeof decilesdata === 'string') {
                let unescapedDeciles = decilesdata.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                let parsedDeciles = JSON.parse(unescapedDeciles);
                console.log('Parsed decilesdata:', parsedDeciles);
                deciles.set(parsedDeciles);
            } else if (typeof decilesdata === 'object' && decilesdata !== null) {
                console.log('Setting decilesdata directly:', decilesdata);
                deciles.set(decilesdata);
            } else {
                console.error('Invalid decilesdata format');
                deciles.set({});
            }
        } catch (e) {
            console.error('Error parsing deciles:', e);
            deciles.set({});
        }
    }

    function handleItemSelection(event) {
        selectedItems.set(event.detail.selectedItems);
        usedOrganisationSelection.set(event.detail.usedOrganisationSelection);
    }

    function handleModeChange(event) {
        selectedMode.set(event.target.value);
        // Reset selection when mode changes, except for 'deciles'
        if ($selectedMode !== 'deciles') {
            selectedItems.set([]);
            usedOrganisationSelection.set(false);
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
                items={$selectedMode === 'deciles' ? $organisations :
                       $selectedMode === 'organisation' ? $organisations : 
                       $selectedMode === 'icb' ? $icbs : 
                       $regions}
                on:selectionChange={handleItemSelection}
                overlayMode={true}
                filterType={$selectedMode === 'deciles' ? 'organisation' : $selectedMode}
            />
        </div>
        <ModeSelector {handleModeChange} />
    </div>

    <div class="flex-grow relative" style="min-height: {chartOptions.minHeight}px;">
        <div bind:this={chartContainer} class="chart-container absolute inset-0">
            {#if $measureData.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <MeasureChart 
                    width={chartWidth}
                    height={chartHeight}
                />
            {/if}
        </div>
    </div>

    <ChartLegend />
</div>