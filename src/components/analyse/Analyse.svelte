<svelte:options customElement={{
    tag: 'layout-component',
    shadow: 'none'
  }} />

<script>
  import { onMount } from 'svelte';
  import AnalyseBox from './analyse/AnalyseBox.svelte';
  import ResultsBox from './results/ResultsBox.svelte';
  import { writable } from 'svelte/store';

  let isLeftBoxCollapsed = false;
  let leftBoxWidth = 'w-1/4';
  let isAnalysisRunning = writable(false);
  let isSmallScreen = false;

  function toggleLeftBox() {
    isLeftBoxCollapsed = !isLeftBoxCollapsed;
    if (isSmallScreen) {
      leftBoxWidth = isLeftBoxCollapsed ? 'h-12' : 'h-auto';
    } else {
      leftBoxWidth = isLeftBoxCollapsed ? 'w-0' : 'w-full sm:w-1/4';
    }
  }

  function handleResize() {
    isSmallScreen = window.innerWidth < 640; // Adjust this breakpoint as needed
    if (isSmallScreen) {
      leftBoxWidth = isLeftBoxCollapsed ? 'h-12' : 'h-auto';
    } else {
      leftBoxWidth = isLeftBoxCollapsed ? 'w-0' : 'w-1/4';
    }
  }

  onMount(() => {
    const analyseBox = document.querySelector('analyse-box');
    const resultsBox = document.querySelector('results-box');

    if (analyseBox && resultsBox) {
      analyseBox.addEventListener('runAnalysis', (event) => {
        resultsBox.dispatchEvent(new CustomEvent('updateData', { detail: event.detail }));
      });

      analyseBox.addEventListener('analysisRunningChange', (event) => {
        isAnalysisRunning.set(event.detail);
        resultsBox.dispatchEvent(new CustomEvent('analysisRunningChange', { detail: event.detail }));
      });
    }

    handleResize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
    };
  });
</script>

<svelte:window on:resize={handleResize} />

<div class="bg-gray-100 min-h-screen">
  <div class="max-w-full mx-auto flex flex-col sm:flex-row h-screen p-4 sm:p-6">
    <div class={`${leftBoxWidth} min-w-0 sm:min-w-[300px] transition-all duration-300 ease-in-out relative mb-4 sm:mb-0 ${isSmallScreen ? 'w-full' : (isLeftBoxCollapsed ? 'sm:ml-[-300px]' : '')}`}>
      <div class="bg-white rounded-lg shadow-md h-full overflow-hidden">
        <div class="flex justify-between items-center bg-oxford-500 text-white p-2">
          <h2 class="text-lg font-semibold">Analysis</h2>
          <button
            on:click={toggleLeftBox}
            class="focus:outline-none"
          >
            {isLeftBoxCollapsed ? '▼' : '▲'}
          </button>
        </div>
        <div class={`transition-all duration-300 ease-in-out ${isSmallScreen && isLeftBoxCollapsed ? 'h-0' : 'h-auto'}`}>
          <analyse-box></analyse-box>
        </div>
      </div>
      {#if !isSmallScreen}
        <button
          on:click={toggleLeftBox}
          class={`absolute top-0 ${isLeftBoxCollapsed ? '-right-[72px]' : 'right-0'} bg-oxford-500 hover:bg-oxford-600 text-white p-2 rounded-r-md focus:outline-none shadow-md transition-all duration-300 ease-in-out`}
        >
          {isLeftBoxCollapsed ? 'Analyse' : '◀'}
        </button>
      {/if}
    </div>
    <div class={`flex-grow transition-all duration-300 ease-in-out ${isSmallScreen ? 'w-full' : (isLeftBoxCollapsed ? 'sm:pl-[72px]' : 'sm:ml-4')}`}>
      <results-box class="bg-white rounded-lg shadow-md h-full" {isAnalysisRunning}></results-box>
    </div>
  </div>
</div>