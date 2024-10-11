<script>
  export let legendItems;
  export let selectedMode;
</script>

{#if legendItems.length > 0}
  <div class="flex flex-wrap justify-center mt-4">
    {#each legendItems as item}
      <div class="flex items-center mr-4 mb-2">
        {#if selectedMode === 'deciles'}
          <div class="w-8 h-0.5 border-t {item.style} mr-2"></div>
        {:else}
          <div class="w-4 h-4 mr-2" style="{item.style}"></div>
        {/if}
        <span class="text-sm">{item.label}</span>
      </div>
    {/each}
  </div>
{/if}
