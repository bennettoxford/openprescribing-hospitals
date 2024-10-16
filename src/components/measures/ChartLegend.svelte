<script>
  import { selectedMode } from '../../stores/measureChartStore.js';
  import { percentilesLegend, regionColors } from '../../utils/chartConfig.js';

  $: legendItems = $selectedMode === 'percentiles' ? percentilesLegend :
                   $selectedMode === 'region' ? Object.entries(regionColors).map(([region, color]) => ({ label: region, color })) :
                   $selectedMode === 'total' ? [{ label: 'Total Average', color: '#000000' }] :
                   [];
</script>

{#if legendItems.length > 0}
  <div class="flex flex-wrap justify-center mt-4">
    {#each legendItems as item}
      <div class="flex items-center mr-4 mb-2">
        {#if $selectedMode === 'percentiles'}
          <div class="w-8 h-0 mr-2 border-t-2" style="border-color: {item.color}; border-style: {item.strokeDasharray === '4,2' ? 'dashed' : 'dotted'};"></div>
        {:else}
          <div class="w-4 h-4 mr-2" style="background-color: {item.color};"></div>
        {/if}
        <span class="text-sm">{item.label}</span>
      </div>
    {/each}
  </div>
{/if}
