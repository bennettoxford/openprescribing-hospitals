<script>
  import { modeOptions } from '../../utils/chartConfig.js';

  export let selectedMode;
  export let handleModeChange;
  
  let modeSelectWidth = 'auto';

  $: selectedMode, adjustModeSelectWidth();

  function adjustModeSelectWidth() {
    const select = document.getElementById('mode-select');
    if (select) {
      const tempSpan = document.createElement('span');
      tempSpan.style.visibility = 'hidden';
      tempSpan.style.position = 'absolute';
      tempSpan.style.whiteSpace = 'nowrap';
      tempSpan.innerHTML = select.options[select.selectedIndex].text;
      document.body.appendChild(tempSpan);
      const width = tempSpan.offsetWidth;
      document.body.removeChild(tempSpan);
      modeSelectWidth = `${width + 40}px`;
    }
  }
</script>

<div class="flex-shrink-0 mr-8">
  <label for="mode-select" class="block text-sm font-medium text-gray-700 mb-1">Select Mode</label>
  <select 
    id="mode-select" 
    class="p-2 border border-gray-300 rounded-md bg-white" 
    on:change={handleModeChange}
    style="width: {modeSelectWidth};"
    bind:value={selectedMode}
  >
    {#each modeOptions as option}
      <option value={option.value}>{option.label}</option>
    {/each}
  </select>
</div>
