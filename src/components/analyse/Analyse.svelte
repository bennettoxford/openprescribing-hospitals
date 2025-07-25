<svelte:options customElement={{
    tag: 'layout-component',
    shadow: 'none'
  }} />

<script>
  import { onMount } from 'svelte';
  import AnalysisBuilder from './analyse/AnalysisBuilder.svelte';
  import AnalysisResults from './results/AnalysisResults.svelte';
  import { writable } from 'svelte/store';
  import { analyseOptions } from '../../stores/analyseOptionsStore';
  import { organisationSearchStore } from '../../stores/organisationSearchStore';

  let isAnalysisRunning = writable(false);
  let isOrganisationDropdownOpen = false;
  let showResults = false;
  let analysisData = null;
  let isAdvancedMode = false;
  let isAnalyseBoxCollapsed = false;
  let isLargeScreen = false;
  let isResultsBoxPopulated = false;

  export let minDate;
  export let maxDate;
  export let orgData;
  export let isAuthenticated;

  $: isResultsBoxPopulated = analysisData && analysisData.length > 0;

  function handleAnalysisStart() {
    showResults = true;
    isAnalysisRunning.set(true);
    analysisData = null;
    isAnalyseBoxCollapsed = true;
  }

  function handleAnalysisComplete(event) {
    isAnalysisRunning.set(false);
    analysisData = event.detail;
    isAnalyseBoxCollapsed = true;
  }

  function handleAnalysisError() {
    isAnalysisRunning.set(false);
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

  function toggleAnalyseBox() {
    isAnalyseBoxCollapsed = !isAnalyseBoxCollapsed;
  }

  function checkScreenSize() {
    isLargeScreen = window.innerWidth >= 1024;
  }

  window.addEventListener('resize', checkScreenSize);
  onMount(() => {
    checkScreenSize();
    if (orgData) {
        try {
            const parsedData = typeof orgData === 'string' ? JSON.parse(orgData) : orgData;
            const predecessorMap = new Map(Object.entries(parsedData.predecessorMap));
            
            organisationSearchStore.setItems(parsedData.items);
            organisationSearchStore.setAvailableItems(parsedData.items);
            organisationSearchStore.setPredecessorMap(predecessorMap);
            organisationSearchStore.setFilterType('trust');
        } catch (error) {
            console.error('Error parsing ODS data:', error);
        }
    }
  });
</script>


  <div class="max-w-screen-2xl mx-auto min-h-screen p-4 sm:p-6">
    <div class="grid grid-cols-1 lg:grid-cols-[350px_1fr] lg:gap-6">
        <div class="mb-4 lg:mb-0 transition-all duration-300 ease-in-out">
            <div class="bg-white rounded-lg shadow-md flex flex-col overflow-visible relative">
                <div class="bg-gradient-to-r from-oxford-600/60 via-bn-roman-600/70 to-bn-strawberry-600/60 text-white p-2 rounded-t-lg">
                    <h2 class="text-lg font-semibold">Analysis builder</h2>
                </div>
                <div class="flex-grow overflow-y-auto overflow-x-visible transition-all duration-300 ease-in-out"
                     class:max-h-0={isAnalyseBoxCollapsed && !isLargeScreen}
                     class:max-h-[1000px]={!isAnalyseBoxCollapsed || isLargeScreen}>
                    <analysis-builder 
                        {isAdvancedMode}
                        {minDate}
                        {maxDate}
                        {orgData}
                        {isAuthenticated}
                        on:analysisStart={handleAnalysisStart}
                        on:analysisComplete={handleAnalysisComplete}
                        on:analysisError={handleAnalysisError}
                        on:analysisClear={handleAnalysisClear}
                        on:organisationDropdownToggle={handleOrganisationDropdownToggle}
                        on:advancedModeChange={handleAdvancedModeChange}
                        on:vmpSelection={handleVMPSelection}
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
                <analysis-results 
                    className="flex-grow" 
                    isAnalysisRunning={$isAnalysisRunning} 
                    analysisData={analysisData} 
                    {showResults}
                ></analysis-results>
            </div>
        </div>
    </div>
  </div>
