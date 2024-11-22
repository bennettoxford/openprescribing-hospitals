<script>
  import { 
    selectedMode, 
    visibleRegions, 
    visibleTrusts,
    visibleICBs,
    orgdata,
    icbdata,
    getOrganisationColor 
  } from '../../stores/measureChartStore.js';
  import { regionColors, percentilesLegend } from '../../utils/chartConfig.js';
  import { get } from 'svelte/store';

  let legendItems = [];

  $: {
    if ($selectedMode === 'region') {
      legendItems = Object.entries(regionColors).map(([region, color]) => ({
        label: region,
        color,
        visible: $visibleRegions.size === 0 || $visibleRegions.has(region)
      }));
    } else if ($selectedMode === 'percentiles') {
      legendItems = percentilesLegend;
    } else if ($selectedMode === 'trust' && $orgdata) {
      legendItems = Object.keys($orgdata).map((trust, index) => ({
        label: trust,
        color: getOrganisationColor(index),
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
</script>

<div class="bg-white rounded-lg shadow-md p-4 h-full flex flex-col">
  {#if title}
    <h3 class="font-semibold mb-3 text-gray-700">{title}</h3>
  {/if}
  <ul class="space-y-3 overflow-y-auto flex-grow">
    {#each legendItems as item}
      <li 
        class="flex items-start gap-2 p-1 rounded transition-colors min-w-0"
        class:opacity-50={!item.visible}
        class:cursor-pointer={isInteractive}
        class:hover:bg-gray-50={isInteractive}
        on:click={() => isInteractive && toggleItem(item)}
      >
        <div 
          class="w-4 h-4 rounded-sm flex-shrink-0 mt-1" 
          style="background-color: {item.color};"
        ></div>
        <span class="text-sm break-words leading-tight min-w-0 flex-shrink">{item.label}</span>
      </li>
    {/each}
  </ul>
</div>

<style>
  li {
    user-select: none;
  }
</style>



