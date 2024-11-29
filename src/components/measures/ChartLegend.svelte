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
 
  <ul class="space-y-1 overflow-y-auto flex-grow">
    {#if isPercentileLegend}
      <!-- Median line -->
      <div class="flex flex-col">
        <li class="flex items-start gap-2 rounded">
          <div class="w-4 h-4 rounded-sm flex-shrink-0 mt-1 flex items-center justify-center">
            <div class="w-full h-0.5 bg-[#DC3220]"></div>
          </div>
          <span class="text-sm break-words leading-tight">Median (50th Percentile)</span>
        </li>
        {#each percentileRanges as { range, opacity }}
        <li class="flex items-start gap-2 rounded">
          <div 
            class="w-4 h-4 rounded-sm flex-shrink-0" 
            style="background-color: rgb(0, 90, 181, {opacity});"
          ></div>
          <span class="text-sm break-words leading-tight">{range[0]}th-{range[1]}th percentile</span>
        </li>
      {/each}
      </div>
      <!-- Percentile ranges -->
      

      <!-- Overlaid organisations -->
      {#if $selectedMode === 'percentiles' && $visibleTrusts.size > 0}
     
        {#each Array.from($visibleTrusts) as trust, index}
          <li 
            class="flex items-start gap-2 rounded transition-colors cursor-pointer hover:bg-gray-50"
            on:click={() => toggleItem({ label: trust })}
          >
            <div 
              class="w-4 h-4 rounded-sm flex-shrink-0 mt-1" 
              style="background-color: {getOrganisationColor(index)};"
            ></div>
            <span class="text-sm break-words leading-tight">{trust}</span>
          </li>
        {/each}
      {/if}
    {:else}
      <!-- Existing non-percentile legend items -->
      {#each legendItems as item}
        <li 
          class="flex items-start gap-2 rounded transition-colors min-w-0"
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
    {/if}
  </ul>
</div>

<style>
  li {
    user-select: none;
  }
</style>



