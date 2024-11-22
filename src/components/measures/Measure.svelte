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
    import { 
        selectedMode, 
        orgdata as orgStore, 
        regiondata as regionStore, 
        icbdata as icbStore, 
        percentiledata as percentileStore, 
        selectedTrusts,
        selectedICBs,
        visibleRegions,
        visibleTrusts,
        visibleICBs
    } from '../../stores/measureChartStore.js';
    import MeasureChart from './MeasureChart.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from './ModeSelector.svelte';
    import ChartLegend from './ChartLegend.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';

    export let orgdata = '[]';
    export let regiondata = '[]';
    export let icbdata = '[]';
    export let percentiledata = '[]';
   
    let trusts = [];
    let icbs = [];
    let regions = [];

    $: showFilter = $selectedMode === 'trust' || 
                    $selectedMode === 'percentiles' || 
                    $selectedMode === 'icb' ||
                    $selectedMode === 'region';
    $: filterItems = $selectedMode === 'icb' ? icbs : 
                    $selectedMode === 'region' ? regions :
                    trusts;
    $: filterType = $selectedMode === 'icb' ? 'icb' : 
                    $selectedMode === 'region' ? 'region' :
                    'trust';
    $: showLegend = $selectedMode === 'percentiles' || 
                    $selectedMode === 'region' || 
                    $selectedMode === 'trust' || 
                    $selectedMode === 'icb';

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.setItems(icbs);
            organisationSearchStore.setFilterType('icb');
        } else if ($selectedMode === 'trust' || $selectedMode === 'percentiles') {
            organisationSearchStore.setItems(trusts);
            organisationSearchStore.setFilterType('trust');
        } else if ($selectedMode === 'region') {
            console.log('setting regions');
            console.log(regions);
            organisationSearchStore.setItems(regions);
            organisationSearchStore.setFilterType('region');
        }
    }

    onMount(() => {
        const parsedOrgData = JSON.parse(orgdata);
        orgStore.set(parsedOrgData);
        trusts = Object.keys(parsedOrgData);
        
        const parsedIcbData = JSON.parse(icbdata);
        icbStore.set(parsedIcbData);
        icbs = parsedIcbData.map(icb => icb.name);
        
        organisationSearchStore.setItems(trusts);
        organisationSearchStore.setFilterType('trust');
        
        const parsedRegionData = JSON.parse(regiondata);
        regionStore.set(parsedRegionData);
        regions = parsedRegionData.map(region => region.name);
        
        percentileStore.set(JSON.parse(percentiledata));
        selectedMode.set('percentiles');
        
        if ($selectedMode === 'icb') {
            organisationSearchStore.updateSelection($selectedICBs);
        } else if ($selectedMode === 'trust' || $selectedMode === 'percentiles') {
            organisationSearchStore.updateSelection($selectedTrusts);
        } else if ($selectedMode === 'region') {
            organisationSearchStore.updateSelection($visibleRegions);
        }
    });

    function handleSelectionChange(event) {
        const { selectedItems } = event.detail;
        
        if ($selectedMode === 'icb') {
            visibleICBs.set(new Set(selectedItems));
        } else if ($selectedMode === 'region') {
            visibleRegions.set(new Set(selectedItems));
        } else if ($selectedMode === 'trust' || $selectedMode === 'percentiles') {
            visibleTrusts.set(new Set(selectedItems));
        }
    }

    function handleModeChange(event) {
        const newMode = event.target.value;
        selectedMode.set(newMode);
        
        visibleRegions.set(new Set());
        visibleTrusts.set(new Set());
        visibleICBs.set(new Set());
        
        if (newMode === 'icb') {
            organisationSearchStore.setItems(icbs);
            organisationSearchStore.setFilterType('icb');
        } else if (newMode === 'region') {
            organisationSearchStore.setItems(regions);
            organisationSearchStore.setFilterType('region');
        } else if (newMode === 'trust' || newMode === 'percentiles') {
            organisationSearchStore.setItems(trusts);
            organisationSearchStore.setFilterType('trust');
        }
        organisationSearchStore.updateSelection([]);
    }

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.updateSelection(Array.from($visibleICBs));
        } else if ($selectedMode === 'region') {
            organisationSearchStore.updateSelection(Array.from($visibleRegions));
        } else if ($selectedMode === 'trust' || $selectedMode === 'percentiles') {
            organisationSearchStore.updateSelection(Array.from($visibleTrusts));
        }
    }
</script>

<div class="flex flex-col">
    <div class="mb-4 flex flex-col sm:flex-row sm:justify-between sm:items-end gap-4">
        {#if showFilter}
            <div class="relative z-10 flex-grow mx-2 sm:mr-4 sm:ml-4">
                <OrganisationSearch 
                    source={organisationSearchStore}
                    overlayMode={true}
                    on:selectionChange={handleSelectionChange}
                />
            </div>
        {:else}
            <div class="flex-grow mx-2 sm:mx-4"></div>
        {/if}
        <div class="flex-shrink-0 mx-2 sm:mx-0">
            <ModeSelector {handleModeChange} />
        </div>
    </div>
    <div class="flex flex-col lg:flex-row gap-4">
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
            <div class="h-[200px] lg:h-[400px] lg:w-48 flex-shrink-0">
                <ChartLegend />
            </div>
        {/if}
    </div>
</div>
