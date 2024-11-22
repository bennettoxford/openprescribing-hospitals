<script>
  import { 
    selectedMode, 
    visibleRegions, 
    visibleTrusts,
    visibleICBs,
    orgdata,
    icbdata,
    getOrganisationColor,
    getOrganisationIndex 
  } from '../../stores/measureChartStore.js';
  import { regionColors, percentilesLegend } from '../../utils/chartConfig.js';
  import { get } from 'svelte/store';

  let legendItems = [];
  let isPercentileLegend = false;

  $: {
    isPercentileLegend = $selectedMode === 'percentiles';
    if ($selectedMode === 'region') {
      legendItems = Object.entries(regionColors).map(([region, color]) => ({
        label: region,
        color,
        visible: $visibleRegions.size === 0 || $visibleRegions.has(region)
      }));
    } else if ($selectedMode === 'percentiles') {
      legendItems = [];
    } else if ($selectedMode === 'trust' && $orgdata) {
      legendItems = Object.keys($orgdata).map((trust) => ({
        label: trust,
        color: getOrganisationColor(getOrganisationIndex(trust, $orgdata)),
        visible: $visibleTrusts.size === 0 || $visibleTrusts.has(trust)
      }));
    } else if ($selectedMode === 'icb' && $icbdata) {
      legendItems = $icbdata.map((icb, index) => ({
        label: icb.name,
        color: getOrganisationColor(index),
        visible: $visibleICBs.size === 0 || $visibleICBs.has(icb.name)
      }));
    }
  }

  function toggleItem(item) {
    let store;
    let currentSet;
    
    switch ($selectedMode) {
      case 'region':
        store = visibleRegions;
        currentSet = get(visibleRegions);
        break;
      case 'trust':
        store = visibleTrusts;
        currentSet = get(visibleTrusts);
        break;
      case 'icb':
        store = visibleICBs;
        currentSet = get(visibleICBs);
        break;
      default:
        return;
    }

    const newVisible = new Set(currentSet || []);
    if (newVisible.has(item.label)) {
      newVisible.delete(item.label);
    } else {
      newVisible.add(item.label);
    }
    store.set(newVisible);
  }

  $: title = {
    'region': 'Regions',
    'trust': 'Trusts',
    'icb': 'ICBs',
    'percentiles': 'Percentiles'
  }[$selectedMode] || '';

  $: isInteractive = ['region', 'trust', 'icb'].includes($selectedMode);

  const percentileRanges = [
    { range: [5, 95], opacity: 0.1 },
    { range: [15, 85], opacity: 0.2 },
    { range: [25, 75], opacity: 0.4 },
    { range: [35, 65], opacity: 0.6 },
    { range: [45, 55], opacity: 0.8 }
  ];
</script>

<div class="h-full flex flex-col overflow-hidden">
  {#if title}
    <h3 class="font-semibold mb-3 text-gray-700">{title}</h3>
  {/if}

  {#if isPercentileLegend}
    <div class="space-y-6 flex-shrink-0">
      <!-- Median line -->
      <div class="flex items-center gap-2">
        <div class="w-8 h-0.5 bg-[#DC3220]"></div>
        <span class="text-sm">Median (50th Percentile)</span>
      </div>
      
      <!-- Percentile ranges -->
      <div>
        <div class="text-sm font-medium mb-2">Percentiles</div>
        <div class="space-y-2 lg:w-full w-64 mx-auto">
          {#each percentileRanges as { range, opacity }}
            <div class="flex items-center gap-2">
              <div class="w-12 text-xs text-right">{range[0]}-{range[1]}</div>
              <div 
                class="h-4 flex-1" 
                style="background-color: rgb(0, 90, 181, {opacity});"
              ></div>
            </div>
          {/each}
        </div>
      </div>
    </div>
  {:else}
    <ul class="space-y-3 overflow-y-auto flex-grow">
      {#each legendItems as item}
        <li 
          class="flex items-start gap-2 p-1 rounded transition-colors min-w-0"
          class:opacity-50={!item.visible}
          class:cursor-pointer={!isPercentileLegend}
          class:hover:bg-gray-50={!isPercentileLegend}
          on:click={() => !isPercentileLegend && toggleItem(item)}
        >
          <div 
            class="w-4 h-4 rounded-sm flex-shrink-0 mt-1" 
            style="background-color: {item.color};"
          ></div>
          <span class="text-sm break-words leading-tight min-w-0 flex-shrink">{item.label}</span>
        </li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  li {
    user-select: none;
  }
</style>



