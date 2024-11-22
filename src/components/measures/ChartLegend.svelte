<script>
  import { selectedMode, visibleRegions } from '../../stores/measureChartStore.js';
  import { regionColors, percentilesLegend } from '../../utils/chartConfig.js';

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
    }
  }

  function toggleRegion(region) {
    if ($selectedMode === 'region') {
      const newVisibleRegions = new Set($visibleRegions);
      if (newVisibleRegions.has(region)) {
        newVisibleRegions.delete(region);
      } else {
        newVisibleRegions.add(region);
      }
      visibleRegions.set(newVisibleRegions);
    }
  }
</script>

<div class="bg-white rounded-lg shadow-md p-4">
  <ul class="space-y-3">
    {#each legendItems as item}
      <li 
        class="flex items-start gap-2 cursor-pointer hover:bg-gray-50 p-1 rounded transition-colors"
        class:opacity-50={!item.visible}
        on:click={() => toggleRegion(item.label)}
      >
        <div 
          class="w-4 h-4 rounded-sm flex-shrink-0 mt-1" 
          style="background-color: {item.color};"
        ></div>
        <span class="text-sm break-words leading-tight">{item.label}</span>
      </li>
    {/each}
  </ul>
</div>

<style>
  li {
    user-select: none;
  }
</style>



