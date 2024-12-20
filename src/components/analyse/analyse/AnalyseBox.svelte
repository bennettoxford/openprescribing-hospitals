<svelte:options customElement={{
    tag: 'analyse-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import '../../../styles/styles.css';
    import Search from '../../common/Search.svelte';
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import { createEventDispatcher } from 'svelte';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { updateResults } from '../../../stores/resultsStore';
    import { getCookie } from '../../../utils/utils';
    
    const dispatch = createEventDispatcher();

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isOrganisationDropdownOpen = false;

    $: selectedVMPs = $analyseOptions.selectedVMPs;
    $: quantityType = $analyseOptions.quantityType;
    $: searchType = $analyseOptions.searchType;

    export let odsData = null;
    export let mindate = null;
    export let maxdate = null;
    export let isAdvancedMode = false;

    onMount(async () => {
        try {
            if (odsData) {
                try {
                    const parsedData = typeof odsData === 'string' ? JSON.parse(odsData) : odsData;
                    organisationSearchStore.setItems(parsedData);
                    organisationSearchStore.updateSelection(parsedData);
                } catch (error) {
                    console.error('Error parsing ODS data:', error);
                }
            }
        } catch (error) {
            console.error('Error in onMount:', error);
        }
    });

    const csrftoken = getCookie('csrftoken');
    const quantityOptions = ['--', 'VMP Quantity', 'Ingredient Quantity', 'DDD'];

    async function runAnalysis() {
        if (isAnalysisRunning) return;

        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = "Please select at least one product or ingredient.";
            return;
        }

        if (!$organisationSearchStore.selectedItems || $organisationSearchStore.selectedItems.length === 0) {
            errorMessage = "Please select at least one Trust to analyse.";
            return;
        }

        if (!isAdvancedMode) {
            quantityType = "VMP Quantity";
        } else if (quantityType === '--') {
            errorMessage = "Please select a quantity type before running the analysis.";
            return;
        }

        isAnalysisRunning = true;
        dispatch('analysisStart');
        
        let endpoint = '/api/filtered-quantities/';
        
        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrftoken
                },
                body: JSON.stringify({
                    quantity_type: quantityType,
                    names: selectedVMPs,
                    ods_names: $organisationSearchStore.selectedItems,
                    search_type: searchType
                })
            });

            if (!response.ok) {
                const data = await response.json();
                throw new Error(data.error || `HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
  
            analyseOptions.runAnalysis({
                selectedVMPs,
                quantityType,
                searchType,
                organisations: $organisationSearchStore.selectedItems
            });

            updateResults(data, {
                quantityType,
                searchType
            });

            dispatch('analysisComplete', { 
                data: Array.isArray(data) ? data : [data],
                quantityType,
                searchType
            });
        } catch (error) {
            console.error("Error fetching filtered data:", error);
            errorMessage = "An error occurred while fetching data. Please try again.";
            dispatch('analysisError', { error: errorMessage });
        } finally {
            isAnalysisRunning = false;
        }
    }

    function dispatchAnalysisRunningChange(running) {
        const analyseBox = document.querySelector('analyse-box');
        if (analyseBox) {
            analyseBox.dispatchEvent(new CustomEvent('analysisRunningChange', { detail: running }));
        }
    }

    function handleVMPSelection(event) {
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: event.detail.items
        }));
    }

    function handleODSSelection(event) {
        const { selectedItems, usedOrganisationSelection } = event.detail;
        organisationSearchStore.updateSelection(selectedItems, usedOrganisationSelection);
    }

    function handleQuantityTypeChange(event) {
        analyseOptions.update(options => ({
            ...options,
            quantityType: event.target.value
        }));
        console.log('Quantity type changed:', $analyseOptions.quantityType);
    }

    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
    }

    function resetSelections(newQuantityType, clearOrgs = false) {
        const allOrgs = $organisationSearchStore.availableItems;
        
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: [],
            searchType: 'vmp',
            quantityType: newQuantityType
        }));
        
        if (clearOrgs) {
            organisationSearchStore.updateSelection(allOrgs);
        }

        errorMessage = '';

        const searchComponent = document.querySelector('analyse-box search-component');
        if (searchComponent) {
            searchComponent.clearInput();
        }
    }

    function toggleAdvancedMode() {
        isAdvancedMode = !isAdvancedMode;

        const currentOrgSelections = $organisationSearchStore.selectedItems;

        resetSelections(isAdvancedMode ? '--' : 'VMP Quantity');

        if (currentOrgSelections && currentOrgSelections.length > 0) {
            organisationSearchStore.updateSelection(currentOrgSelections);
        }
        dispatch('advancedModeChange', isAdvancedMode);
    }

    function handleClearAnalysis() {
        resetSelections(isAdvancedMode ? '--' : 'VMP Quantity');
        dispatch('analysisClear');
    }
</script>

<div class="p-4 sm:p-6 bg-white rounded-lg w-full">
  <div class="grid gap-8">
    <!-- Header -->
    <div>
      <p class="text-sm text-oxford">
        {#if isAdvancedMode}
          Run a custom analysis of hospitals stock control data using the options below. You can analyse 
          specific medicines or groups of medicines across different NHS Trusts.
        {:else}
          Search for specific medicines to analyse their stock control data across NHS Trusts.
        {/if}
      </p>
    </div>

    <!-- Selection Grid - Now always single column -->
    <div class="grid gap-6">
      <!-- Product Selection -->
      <div class="grid gap-4">
        <div class="flex items-center">
          <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Product selection</h3>
          <div class="relative inline-block group">
            <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
              <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
              </svg>
            </button>
            <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                        group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                        ring-1 ring-black ring-opacity-5 p-4">
              <p class="text-sm text-gray-500">
                {#if isAdvancedMode}
                  Search for and select products to include to analyse. You can select individual products
                  (VMP) or groups of products by ingredient or therapeutic moiety (VTM). See <a href="/faq/#product-selection-types" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                {:else}
                Search for and select individual products or groups of products to analyse. See <a href="/faq/#product-selection-types" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                {/if}
              </p>
            </div>
          </div>
        </div>
        <div class="relative">
          <Search on:selectionChange={handleVMPSelection} {isAdvancedMode} />
        </div>
      </div>

      <!-- Trust Selection -->
      <div class="grid gap-4">
        <div class="flex items-center">
          <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Trusts selection</h3>
          <div class="relative inline-block group">
            <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
              <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
              </svg>
            </button>
            <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                        group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                        ring-1 ring-black ring-opacity-5 p-4">
              <p class="text-sm text-gray-500">
                By default, the analysis will include all NHS Trusts in England. You can restrict the analysis by selecting specific trusts below.
                See <a href="/faq/#trusts-included" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
              </p>
            </div>
          </div>
        </div>
        <div class="relative">
          <OrganisationSearch 
            source={organisationSearchStore}
            overlayMode={false}
            on:selectionChange={handleODSSelection}
            on:dropdownToggle={handleOrganisationDropdownToggle}
          />
        </div>
      </div>
    </div>

    <!-- Second Selection Grid - Now always single column -->
    {#if isAdvancedMode}
    <div class="grid gap-6">
        <!-- Quantity Type Selection -->
        <div class="grid gap-4">
          <div class="flex items-center">
            <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Quantity type selection</h3>
            <div class="relative inline-block group">
              <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                  <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                </svg>
              </button>
              <div class="absolute z-[200] scale-0 transition-all duration-100 origin-top transform 
                          group-hover:scale-100 w-[200px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                          ring-1 ring-black ring-opacity-5 p-4">
                <p class="text-sm text-gray-500">
                  Select the quantity unit most relevant to the selected products to use for the analysis.
                  See <a href="/faq/#quantity-type" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                </p>
              </div>
            </div>
          </div>
          <div class="relative">
            <select 
              id="quantityType"
              bind:value={$analyseOptions.quantityType}
              on:change={handleQuantityTypeChange}
              class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
            >
              {#each quantityOptions as option}
                <option value={option}>{option}</option>
              {/each}
            </select>
          </div>
        </div>
    </div>
    {/if}
    <!-- Analysis Controls -->
    <div class="mt-8 bg-gray-50 rounded-lg p-4 sm:p-6">
      <div class="flex flex-col gap-4">
        <!-- Mode Switch -->
        <button 
          class="text-sm text-oxford-600 hover:text-oxford-700 flex items-center gap-2"
          on:click={toggleAdvancedMode}
        >
          <span class="underline">Switch to {isAdvancedMode ? 'basic' : 'advanced'} mode</span>
        </button>

        <!-- Error Message -->
        {#if errorMessage}
          <div class="p-3 bg-red-100 border border-red-400 text-red-700 rounded">
            {errorMessage}
          </div>
        {/if}

        <!-- Action Buttons -->
        <div class="flex gap-2 justify-end">
          <button
            on:click={handleClearAnalysis}
            class="px-4 sm:px-6 py-2 sm:py-2.5 bg-white text-gray-700 font-medium rounded-md hover:bg-gray-100 transition-colors duration-200 border border-gray-200"
          >
            Clear Analysis
          </button>
          <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="px-4 sm:px-6 py-2 sm:py-2.5 bg-oxford-600 text-white font-medium rounded-md hover:bg-oxford-700 transition-colors duration-200
                 disabled:bg-oxford-300 disabled:cursor-not-allowed"
          >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
          </button>
        </div>
      </div>
    </div>
  </div>
</div>
