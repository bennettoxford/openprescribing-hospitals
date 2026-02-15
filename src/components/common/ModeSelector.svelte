<script>
  import { modeSelectorStore } from '../../stores/modeSelectorStore.js';

  export let options = [];
  export let initialMode = null;
  export let label = 'Select Mode';
  export let onChange = () => {};
  export let variant = 'dropdown';

  const totalTooltipText = 'Monthly quantity across all selected products and NHS Trusts';

  $: {
    modeSelectorStore.setOptions(options);
    if (initialMode && !$modeSelectorStore.selectedMode) {
      modeSelectorStore.setSelectedMode(initialMode);
    }
  }

  $: if ($modeSelectorStore.selectedMode !== undefined) {
    onChange($modeSelectorStore.selectedMode);
  }

  function handleChange(event) {
    modeSelectorStore.setSelectedMode(event.target.value);
  }

  function handlePillClick(value) {
    modeSelectorStore.setSelectedMode(value);
  }
</script>

{#if variant === 'dropdown'}
  <div>
    <label for="mode-select" class="block text-sm font-medium text-gray-700 mb-1">{label}</label>
    <select 
      id="mode-select" 
      class="dropdown-select dropdown-arrow text-sm p-2 border border-gray-300 rounded-md bg-white w-full min-w-0 xl:w-auto xl:min-w-[120px]" 
      on:change={handleChange}
      value={$modeSelectorStore.selectedMode}
    >
      {#each $modeSelectorStore.options as option}
        <option 
          value={option.value}
          title={option.value === 'total' ? totalTooltipText : ''}
        >
          {option.label}
        </option>
      {/each}
    </select>
  </div>
{:else}
  <div class="w-full">
    {#if label}
      <label class="block text-sm font-medium text-gray-700 mb-2">{label}</label>
      <div class="flex flex-wrap gap-2">
        {#each $modeSelectorStore.options as option}
          <div class="relative inline-block group">
            <button
              class="px-3 py-1 rounded-full text-sm font-medium transition-colors
                {$modeSelectorStore.selectedMode === option.value
                  ? 'bg-oxford-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
              on:click={() => handlePillClick(option.value)}
            >
              {option.label}
            </button>
            {#if option.value === 'total'}
              <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                          group-hover:scale-100 w-[200px] left-0 top-full mt-1
                          rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 p-4
                          max-w-[calc(100vw-2rem)]">
                <p class="text-sm text-gray-500">
                  {totalTooltipText}
                </p>
              </div>
            {/if}
          </div>
        {/each}
      </div>
    {/if}
  </div>
{/if}
