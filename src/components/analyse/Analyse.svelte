<svelte:options runes={false} customElement={{
    tag: 'layout-component',
    shadow: 'none'
  }} />

<script>
  import { onMount } from 'svelte';
  import AnalysisBuilder from './analyse/AnalysisBuilder.svelte';
  import AnalysisResults from './results/AnalysisResults.svelte';
  import { resultsStore, setAnalysisRunning, clearAnalysisResults } from '../../stores/resultsStore';

  let analysisResults;
  let isAnalyseBoxCollapsed = false;
  let isLargeScreen = false;
  let isResultsBoxPopulated = false;
  let urlValidationErrors = [];

  export let minDate;
  export let maxDate;
  export let orgData;
  export let isAuthenticated = false;
  export let maxVmpCount = null;

  $: isAuth = isAuthenticated === true || isAuthenticated === 'true';

  $: isResultsBoxPopulated = $resultsStore.showResults
    || (urlValidationErrors && urlValidationErrors.length > 0);

  function handleAnalysisStart() {
    setAnalysisRunning(true);
    analysisResults?.prepareForNewRun?.();
    isAnalyseBoxCollapsed = true;
  }

  function handleAnalysisComplete(event) {
    analysisResults?.loadAnalysis?.(event.detail);
    isAnalyseBoxCollapsed = true;
  }

  function handleAnalysisError() {
    setAnalysisRunning(false);
  }

  function handleUrlValidationErrors(event) {
    urlValidationErrors = event.detail.errors || [];
    if (urlValidationErrors.length > 0) {
      resultsStore.update(store => ({ ...store, showResults: true }));
    }
  }

  function handleAnalysisClear() {
    clearAnalysisResults();
    analysisResults?.prepareForNewRun?.();
    urlValidationErrors = [];
  }

  function toggleAnalyseBox() {
    isAnalyseBoxCollapsed = !isAnalyseBoxCollapsed;
  }

  function checkScreenSize() {
    isLargeScreen = window.innerWidth >= 1024;
  }

  window.addEventListener('resize', checkScreenSize);
  onMount(() => {
    checkScreenSize();
  });
</script>


  <div class="max-w-screen-2xl mx-auto min-h-screen p-4 sm:p-6">
    <div class="grid grid-cols-1 lg:grid-cols-[350px_1fr] lg:gap-6">
        <div class="mb-4 lg:mb-0 min-w-0 max-w-full transition-all duration-300 ease-in-out">
            <div class="bg-white rounded-lg shadow-md flex flex-col overflow-visible relative">
                <div class="bg-gradient-to-r from-oxford-600/60 via-bn-roman-600/70 to-bn-strawberry-600/60 text-white p-2 rounded-t-lg">
                    <h2 class="text-lg font-semibold">Analysis builder</h2>
                </div>
                <div class="flex-grow overflow-y-auto overflow-x-hidden transition-all duration-300 ease-in-out"
                     class:max-h-0={isAnalyseBoxCollapsed && !isLargeScreen}
                     class:max-h-[1000px]={!isAnalyseBoxCollapsed && !isLargeScreen}>
                    <analysis-builder
                        class="block w-full max-w-full"
                        {minDate}
                        {maxDate}
                        {orgData}
                        isAuthenticated={isAuth}
                        maxVmpCount={maxVmpCount}
                        on:analysisStart={handleAnalysisStart}
                        on:analysisComplete={handleAnalysisComplete}
                        on:analysisError={handleAnalysisError}
                        on:analysisClear={handleAnalysisClear}
                        on:urlValidationErrors={handleUrlValidationErrors}
                        on:overlaySelectionChange={() => analysisResults?.syncOverlayFromBuilder?.()}
                    ></analysis-builder>
                </div>
                {#if !isLargeScreen}
                <button
                    class="w-full p-2 bg-gray-100 border-t border-gray-200 flex items-center justify-center hover:bg-oxford-50 active:bg-oxford-100 cursor-pointer transition-colors duration-150 gap-2 rounded-b-lg"
                    on:click={toggleAnalyseBox}
                >
                    <span class="text-sm font-medium text-gray-700">
                        {isAnalyseBoxCollapsed ? 'Show Analysis Options' : 'Hide Analysis Options'}
                    </span>
                    <svg 
                        class="w-4 h-4 text-gray-700 transform transition-transform duration-200 {isAnalyseBoxCollapsed ? '' : 'rotate-180'}" 
                        fill="none" 
                        stroke="currentColor" 
                        viewBox="0 0 24 24"
                    >
                        <path 
                            stroke-linecap="round" 
                            stroke-linejoin="round" 
                            stroke-width="2" 
                            d="M19 9l-7 7-7-7"
                        />
                    </svg>
                </button>
                {/if}
            </div>
        </div>
        <div class="h-fit">
            <div class={`bg-white rounded-lg shadow-md flex flex-col ${isResultsBoxPopulated ? 'h-auto min-h-[600px]' : 'h-fit'}`}>
                <div class="bg-gradient-to-r from-oxford-600/60 via-bn-roman-600/70 to-bn-strawberry-600/60 text-white p-2 rounded-t-lg">
                    <h2 class="text-lg font-semibold">Results</h2>
                </div>
                <AnalysisResults
                    bind:this={analysisResults}
                    className="flex-grow"
                    isAuthenticated={isAuth}
                    {urlValidationErrors}
                />
            </div>
        </div>
    </div>
  </div>
