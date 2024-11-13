<script>
  import { selectedMode } from '../../stores/measureChartStore.js';

  export let handleModeChange;
  
  const modeOptions = [
    { value: 'percentiles', label: 'Trust (Percentiles)' },
    { value: 'trust', label: 'Trust' },
    { value: 'icb', label: 'ICB' },
    { value: 'region', label: 'Region' },
    { value: 'national', label: 'National' },
  ];

  let modeSelectWidth = 'auto';

  $: $selectedMode, adjustModeSelectWidth();

  function adjustModeSelectWidth() {
    const select = document.getElementById('mode-select');
    if (select) {
      const tempSpan = document.createElement('span');
      tempSpan.style.visibility = 'hidden';
      tempSpan.style.position = 'absolute';
      tempSpan.style.whiteSpace = 'nowrap';
      tempSpan.style.font = window.getComputedStyle(select).font;
      tempSpan.innerHTML = select.options[select.selectedIndex].text;
      document.body.appendChild(tempSpan);
      const width = tempSpan.offsetWidth;
      document.body.removeChild(tempSpan);
      modeSelectWidth = `${width + 60}px`;
    }
  }
</script>

<div>
  <label for="mode-select" class="block text-sm font-medium text-gray-700 mb-1">Select Mode</label>
  <select 
    id="mode-select" 
    class="p-2 border border-gray-300 rounded-md bg-white min-w-[180px]" 
    on:change={handleModeChange}
    style="width: {modeSelectWidth};"
    bind:value={$selectedMode}
  >
    {#each modeOptions as option}
      <option value={option.value}>{option.label}</option>
    {/each}
  </select>
</div>
