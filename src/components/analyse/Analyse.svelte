<svelte:options customElement={{
    tag: 'layout-component',
    shadow: 'none'
  }} />

<script>
  import { onMount } from 'svelte';
  import AnalyseBox from './analyse/AnalyseBox.svelte';
  import ResultsBox from './results/ResultsBox.svelte';
  import { writable } from 'svelte/store';
  import { analyseOptions } from '../../stores/analyseOptionsStore';
  import { get } from 'svelte/store';
  import { organisationSearchStore } from '../../stores/organisationSearchStore';

  let isAnalysisRunning = writable(false);
  let isOrganisationDropdownOpen = false;
  let showResults = false;
  let analysisData = null;
  let isAdvancedMode = false;

  export let minDate;
  export let maxDate;
  export let odsData;

  function handleAnalysisStart() {
    showResults = true;
    isAnalysisRunning.set(true);
    analysisData = null;
  }

  function handleAnalysisComplete(event) {
    isAnalysisRunning.set(false);
    analysisData = event.detail;
  }

  function handleAnalysisError(event) {
    isAnalysisRunning.set(false);
    // Handle error if needed
  }

  function handleOrganisationDropdownToggle(event) {
    isOrganisationDropdownOpen = event.detail.isOpen;
  }

  function handleAnalysisClear() {
    showResults = false;
    analysisData = null;
    isAnalysisRunning.set(false);
  }

  function handleAdvancedModeChange(event) {
    isAdvancedMode = event.detail;
  }

  function handleVMPSelection(event) {
    analyseOptions.update(options => ({
        ...options,
        selectedProducts: event.detail.items,
        searchType: event.detail.type
    }));
    console.log("Selected Items:", $analyseOptions.selectedProducts, "Search Type:", $analyseOptions.searchType);
  }

  onMount(() => {
    const analyseBox = document.querySelector('analyse-box');
    const resultsBox = document.querySelector('results-box');

    if (analyseBox && resultsBox) {
      analyseBox.addEventListener('runAnalysis', (event) => {
        handleAnalysisStart();
        handleAnalysisComplete(event);
      });

      analyseBox.addEventListener('analysisRunningChange', (event) => {
        isAnalysisRunning.set(event.detail);
      });

      analyseBox.addEventListener('organisationDropdownToggle', handleOrganisationDropdownToggle);
      analyseBox.addEventListener('analysisClear', handleAnalysisClear);
    }

    if (odsData) {
      try {
        const parsedData = typeof odsData === 'string' ? JSON.parse(odsData) : odsData;
        organisationSearchStore.setItems(parsedData);
      } catch (error) {
        console.error('Error parsing ODS data:', error);
      }
    }
  });
</script>


  <div class="max-w-screen-xl mx-auto flex flex-col lg:flex-row min-h-screen p-4 sm:p-6">
    <div class="w-full lg:w-1/3 xl:w-1/4 mb-4 lg:mb-0 lg:mr-4 flex flex-col">
      <div class="bg-white rounded-lg shadow-md flex flex-col overflow-visible">
        <div class="bg-gradient-to-r from-oxford-600/60 via-bn-roman-600/70 to-bn-strawberry-600/60 text-white p-2 rounded-t-lg">
          <h2 class="text-lg font-semibold">Analysis builder</h2>
        </div>
        <div class="flex-grow overflow-y-auto overflow-x-visible">
          <analyse-box 
            {isAdvancedMode}
            {minDate}
            {maxDate}
            {odsData}
            on:analysisStart={handleAnalysisStart}
            on:analysisComplete={handleAnalysisComplete}
            on:analysisError={handleAnalysisError}
            on:analysisClear={handleAnalysisClear}
            on:organisationDropdownToggle={handleOrganisationDropdownToggle}
            on:advancedModeChange={handleAdvancedModeChange}
            on:vmpSelection={handleVMPSelection}
          ></analyse-box>
        </div>
      </div>
    </div>
    <div class="flex-grow">
      <results-box 
        class="bg-white rounded-lg shadow-md h-full" 
        isAnalysisRunning={$isAnalysisRunning} 
        analysisData={analysisData} 
        {showResults}
      ></results-box>
    </div>
  </div>
