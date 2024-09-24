<svelte:options customElement={{
    tag: 'layout-component',
    shadow: 'none'
  }} />

<script>
  import { onMount } from 'svelte';
  import AnalyseBox from './analyse/AnalyseBox.svelte';
  import ResultsBox from './results/ResultsBox.svelte';

  let isLeftBoxCollapsed = false;
  let leftBoxWidth = 'w-1/4';

  function toggleLeftBox() {
    isLeftBoxCollapsed = !isLeftBoxCollapsed;
    leftBoxWidth = isLeftBoxCollapsed ? 'w-0' : 'w-1/4';
  }

  onMount(() => {
    const analyseBox = document.querySelector('analyse-box');
    const resultsBox = document.querySelector('results-box');

    if (analyseBox && resultsBox) {
      analyseBox.addEventListener('runAnalysis', (event) => {
        resultsBox.dispatchEvent(new CustomEvent('updateData', { detail: event.detail }));
      });
    }
  });
</script>

<div class="bg-gray-100 min-h-screen">
  <div class="max-w-full mx-auto flex flex-row h-screen">
    <div class={`${leftBoxWidth} min-w-[400px] transition-all duration-300 ease-in-out relative ${isLeftBoxCollapsed ? 'ml-[-400px]' : ''}`}>
      <analyse-box class="bg-white rounded-rg shadow-md h-full"></analyse-box>
      <button
        on:click={toggleLeftBox}
        class={`absolute top-0 ${isLeftBoxCollapsed ? '-right-[72px]' : 'right-0'} bg-oxford-500 hover:bg-oxford-600 text-white p-2 rounded-r-md focus:outline-none shadow-md transition-all duration-300 ease-in-out`}
      >
        {isLeftBoxCollapsed ? 'Analyse' : '◀'}
      </button>
    </div>
    <div class={`flex-grow transition-all duration-300 ease-in-out ${isLeftBoxCollapsed ? 'pl-[72px]' : ''}`}>
      <results-box class="bg-white rounded-lg shadow-md h-full"></results-box>
    </div>
  </div>
</div>