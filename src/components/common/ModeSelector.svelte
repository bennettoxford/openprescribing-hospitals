<script>
  import { modeSelectorStore } from '../../stores/modeSelectorStore.js';

  export let options = [];
  export let initialMode = null;
  export let label = 'Select Mode';
  export let onChange = () => {};
  export let variant = 'dropdown';

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
      class="p-2 border border-gray-300 rounded-md bg-white min-w-[180px]" 
      on:change={handleChange}
      value={$modeSelectorStore.selectedMode}
    >
      {#each $modeSelectorStore.options as option}
        <option value={option.value}>{option.label}</option>
      {/each}
    </select>
  </div>
{:else}
  <div class="w-full">
    {#if label}
      <label class="block text-sm font-medium text-gray-700 mb-2">{label}</label>
    {/if}
    <div class="flex flex-wrap gap-2">
      {#each $modeSelectorStore.options as option}
        <button
          class="px-3 py-1 rounded-full text-sm font-medium transition-colors
            {$modeSelectorStore.selectedMode === option.value
              ? 'bg-oxford-600 text-white'
              : 'bg-gray-100 text-gray-700 hover:bg-gray-200'}"
          on:click={() => handlePillClick(option.value)}
        >
          {option.label}
        </button>
      {/each}
    </div>
  </div>
{/if}
