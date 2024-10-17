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
    import { selectedMode, orgdata as orgStore, regiondata as regionStore, icbdata as icbStore, percentiledata as percentileStore, selectedItems, resetSelectedItems } from '../../stores/measureChartStore.js';
    import MeasureChart from './MeasureChart.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from './ModeSelector.svelte';
    import ChartLegend from './ChartLegend.svelte';

    export let orgdata = '[]';
    export let regiondata = '[]';
    export let icbdata = '[]';
    export let percentiledata = '[]';
   
    let trusts = [];
    let icbs = [];

    $: showFilter = $selectedMode === 'trust' || $selectedMode === 'percentiles' || $selectedMode === 'icb';
    $: filterItems = $selectedMode === 'icb' ? icbs : trusts;
    $: filterType = $selectedMode === 'icb' ? 'icb' : 'trust';
    $: showLegend = $selectedMode === 'percentiles' || $selectedMode === 'region';

    onMount(() => {
        const parsedOrgData = JSON.parse(orgdata);
        orgStore.set(parsedOrgData);
        trusts = Object.keys(parsedOrgData);
        
        const parsedIcbData = JSON.parse(icbdata);
        icbStore.set(parsedIcbData);
        icbs = parsedIcbData.map(icb => icb.name);
        
        regionStore.set(JSON.parse(regiondata));
        percentileStore.set(JSON.parse(percentiledata));
        selectedMode.set('national'); // Set default mode to 'national'
    });

    function handleSelectionChange(event) {
        selectedItems.set(event.detail.selectedItems);
    }

    function handleModeChange(event) {
        const newMode = event.target.value;
        selectedMode.set(newMode);
    }
</script>

<div class="flex flex-col">
    <div class="mb-4 flex justify-between items-end">
        {#if showFilter}
            <div class="relative z-10 flex-grow mr-4">
                <OrganisationSearch 
                    items={filterItems}
                    on:selectionChange={handleSelectionChange}
                    {filterType}
                    overlayMode={true}
                />
            </div>
        {:else}
            <div class="flex-grow mr-4"></div>
        {/if}
        <div class="flex-shrink-0">
            <ModeSelector {handleModeChange} />
        </div>
    </div>
    <div class="flex-grow relative" style="min-height: 400px;">
        <div class="chart-container absolute inset-0">
            {#if orgdata.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <MeasureChart />
            {/if}
        </div>
    </div>
    {#if showLegend}
        <ChartLegend />
    {/if}
</div>
