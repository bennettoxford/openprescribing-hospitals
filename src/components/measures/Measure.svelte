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
        orgdata as orgdataStore, 
        regiondata as regionStore, 
        icbdata as icbStore, 
        percentiledata as percentileStore, 
        selectedTrusts,
        selectedICBs,
        visibleRegions,
        visibleTrusts,
        visibleICBs,
        getOrganisationColor,
        getOrganisationIndex
    } from '../../stores/measureChartStore.js';
    import MeasureChart from './MeasureChart.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from '../common/ModeSelector.svelte';
    import ChartLegend from '../common/ChartLegend.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';
    import { modeSelectorStore } from '../../stores/modeSelectorStore.js';
    import { regionColors } from '../../utils/chartConfig.js';

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
        orgdataStore.set(parsedOrgData);
        
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

    const modeOptions = [
        { value: 'percentiles', label: 'Trust (Percentiles)' },
        { value: 'trust', label: 'Trust' },
        { value: 'icb', label: 'ICB' },
        { value: 'region', label: 'Region' },
        { value: 'national', label: 'National' },
    ];

    $: currentMode = $modeSelectorStore.selectedMode;
    $: {
        if (currentMode) {
            selectedMode.set(currentMode);
            
            // Clear all selections
            visibleRegions.set(new Set());
            visibleTrusts.set(new Set());
            visibleICBs.set(new Set());
            
            // Update organisation search based on mode
            if (currentMode === 'icb') {
                organisationSearchStore.setItems(icbs);
                organisationSearchStore.setFilterType('icb');
            } else if (currentMode === 'region') {
                organisationSearchStore.setItems(regions);
                organisationSearchStore.setFilterType('region');
            } else if (currentMode === 'trust' || currentMode === 'percentiles') {
                organisationSearchStore.setItems(trusts);
                organisationSearchStore.setFilterType('trust');
            }
            organisationSearchStore.updateSelection([]);
        }
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

    function handleModeChange(newMode) {
        selectedMode.set(newMode);
    }

    $: legendItems = $selectedMode === 'region' ? 
        Object.entries(regionColors).map(([region, color]) => ({
            label: region,
            color,
        })) :
        $selectedMode === 'trust' && $orgdataStore ?
            Object.keys($orgdataStore).map((trust) => ({
                label: trust,
                color: getOrganisationColor(getOrganisationIndex(trust, $orgdataStore)),
            })) :
        $selectedMode === 'icb' && $icbStore ?
            $icbStore.map((icb, index) => ({
                label: icb.name,
                color: getOrganisationColor(index),
            })) :
            [];

    function handleLegendChange(items) {
        const newVisible = new Set(items);
        if ($selectedMode === 'region') {
            visibleRegions.set(newVisible);
        } else if ($selectedMode === 'trust' || $selectedMode === 'percentiles') {
            visibleTrusts.set(newVisible);
        } else if ($selectedMode === 'icb') {
            visibleICBs.set(newVisible);
        }
    }
</script>

<div class="grid grid-cols-1 lg:grid-cols-4 gap-x-4 gap-y-2">
    <!-- Top row - different layout for mobile vs desktop -->
    <div class="col-span-full">
        <div class="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-2">
            <!-- Organisation Search - full width on mobile, spans 3 columns on lg+ -->
            {#if showFilter}
                <div class="lg:col-span-3 relative z-10 flex items-end w-full">
                    <div class="w-full">
                        <OrganisationSearch 
                            source={organisationSearchStore}
                            overlayMode={true}
                            on:selectionChange={handleSelectionChange}
                        />
                    </div>
                </div>
            {:else}
                <div class="lg:col-span-3"></div>
            {/if}
            <!-- Mode Selector - full width on mobile, spans 1 column on lg+ -->
            <div class="lg:col-span-1">
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

    <!-- Bottom row -->
    <!-- Chart - spans 3 columns on lg+ -->
    <div class="lg:col-span-3 relative" style="min-height: 350px;">
        <div class="chart-container absolute inset-0">
            {#if orgdataStore.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <MeasureChart />
            {/if}
        </div>
    </div>

    <!-- Legend - spans 1 column on lg+ -->
    {#if showLegend}
        <div class="legend-container">
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
    max-height: 320px;
    overflow-y: auto;
  }

  @media (max-width: 1024px) {
    .legend-container {
      max-height: 200px;
    }
  }
</style>
