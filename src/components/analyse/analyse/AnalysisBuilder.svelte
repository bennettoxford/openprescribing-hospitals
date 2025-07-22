<svelte:options customElement={{
    tag: 'analysis-builder',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import '../../../styles/styles.css';
    import ProductSearch from '../../common/ProductSearch.svelte';
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
    $: isAdvancedMode = $analyseOptions.isAdvancedMode;

    export let orgData = null;
    export let mindate = null;
    export let maxdate = null;
    export let isAuthenticated = 'false';
    
    $: isAuthenticatedBool = isAuthenticated === 'true';
    
    $: if (!isAuthenticatedBool && isAdvancedMode) {
        analyseOptions.setAdvancedMode(false);
    }

    onMount(async () => {
        try {
            if (orgData) {
                try {
                    const parsedData = typeof orgData === 'string' ? JSON.parse(orgData) : orgData;
                    organisationSearchStore.setItems(parsedData.items);
                    organisationSearchStore.setAvailableItems(parsedData.items);
                } catch (error) {
                    console.error('Error parsing ODS data:', error);
                }
            }
        } catch (error) {
            console.error('Error in onMount:', error);
        }
    });

    const csrftoken = getCookie('csrftoken');
    const quantityOptions = ['--', 'VMP Quantity', 'Ingredient Quantity', 'Daily Defined Doses'];

    async function runAnalysis() {
        if (isAnalysisRunning) return;

        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = "Please select at least one product or ingredient.";
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
        
        if (typeof window !== 'undefined' && window.plausible) {
            window.plausible('Analysis Run', {
                props: {
                    all_products: selectedVMPs.join(','),
                    all_organisations: $organisationSearchStore.selectedItems.join(','),
                    product_count: selectedVMPs.length.toString(),
                    organisation_count: $organisationSearchStore.selectedItems.length.toString()
                }
            });
        }
        
        let endpoint = '/api/get-quantity-data/';
        
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

            analyseOptions.setSelectedOrganisations($organisationSearchStore.selectedItems);

            updateResults(data, {
                quantityType,
                searchType,
                selectedOrganisations: $organisationSearchStore.selectedItems
            });

            dispatch('analysisComplete', { 
                data: Array.isArray(data) ? data : [data],
                quantityType,
                searchType,
                selectedOrganisations: $organisationSearchStore.selectedItems
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
        const analyseBox = document.querySelector('analysis-builder');
        if (analyseBox) {
            analyseBox.dispatchEvent(new CustomEvent('analysisRunningChange', { detail: running }));
        }
    }

    function handleVMPSelection(event) {
        if (!isAuthenticatedBool) {
            event.detail.items = event.detail.items.slice(0, 1);
        } else if (!isAdvancedMode && event.detail.items.length > 1) {
            // If not in advanced mode, only allow one product to be selected
            event.detail.items = [event.detail.items[0]];
        }
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
    }

    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
    }

    function resetSelections(newQuantityType) {
        const allOrgs = $organisationSearchStore.availableItems;
        
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: [],
            searchType: 'vmp',
            quantityType: newQuantityType
        }));
        
        organisationSearchStore.updateSelection([]);

        errorMessage = '';

        const searchComponent = document.querySelector('analysis-builder search-component');
        if (searchComponent) {
            searchComponent.clearInput();
        }
    }

    function toggleAdvancedMode() {
        if (!isAuthenticatedBool && !$analyseOptions.isAdvancedMode) {
            return;
        }
        
        analyseOptions.setAdvancedMode(!$analyseOptions.isAdvancedMode);
        const currentOrgSelections = $organisationSearchStore.selectedItems;
        resetSelections($analyseOptions.isAdvancedMode ? '--' : 'VMP Quantity');
        if (currentOrgSelections && currentOrgSelections.length > 0) {
            organisationSearchStore.updateSelection(currentOrgSelections);
        }
        dispatch('advancedModeChange', $analyseOptions.isAdvancedMode);
    }

    function handleClearAnalysis() {
        resetSelections(isAdvancedMode ? '--' : 'VMP Quantity');
        dispatch('analysisClear');
    }
</script>
<div class="mb-6">
  {#if isAuthenticatedBool}
    <div class="border-b border-gray-200">
      <nav class="flex space-x-0 w-full" aria-label="Tabs">
        <div class="flex items-center gap-2 w-full">
          <button
            class={`py-2 border-b-2 font-medium text-sm flex-1 text-center ${
              !isAdvancedMode
                ? 'border-oxford-500 text-oxford-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
            on:click={() => {
              if (isAdvancedMode) toggleAdvancedMode();
            }}
          >
            <span class="text-sm px-2">
              Single product search
            </span>
          </button>
          <button
            class={`py-2 border-b-2 font-medium text-sm flex-1 text-center ${
              isAdvancedMode
                ? 'border-oxford-500 text-oxford-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
            on:click={() => {
              if (!isAdvancedMode) toggleAdvancedMode();
            }}
          >
            <span class="text-sm">
              Multi-product search
            </span>
          </button>
        </div>
      </nav>
    </div>
  {/if}

  <div>
    <!-- Header -->
    <div class="p-4 sm:p-6 bg-white rounded-lg w-full">
      <div class="grid gap-6">
        <!-- Tabs -->
        <div>
          <p class="text-sm text-oxford">
            {#if isAdvancedMode}
              Run a custom analysis of hospitals stock control data using the options below. You can analyse 
              specific medicines or groups of medicines across different NHS Trusts.
            {:else}
              Analyse the stock control data for individual products in individual NHS Trusts.
            {/if}
          </p>
        </div>

        <div class="grid gap-6">
          <!-- Product Selection -->
          <div class="grid gap-4">
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">
                {isAdvancedMode ? 'Select products' : 'Select product'}
              </h3>
              <div class="relative inline-block group">
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                  <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                  </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                            group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-5 rounded-md shadow-lg bg-white 
                            ring-1 ring-black ring-opacity-5 p-4">
                  <p class="text-sm text-gray-500">
                    {#if isAdvancedMode}
                      Search for and select products to include to analyse. You can select individual products
                      or groups of products by ingredient, product group, or ATC code. See <a href="/faq/#which-medicines-and-devices-are-included" class="underline font-semibold" target="_blank">the FAQs</a> for more information of what products are available.
                    {:else}
                      Search for and select individual products to analyse. See <a href="/faq/#which-medicines-and-devices-are-included" class="underline font-semibold" target="_blank">the FAQs</a> for more information of what products are available.
                    {/if}
                  </p>
                </div>
              </div>
            </div>
            <div class="relative">
              <ProductSearch on:selectionChange={handleVMPSelection} {isAdvancedMode} />
            </div>
          </div>

          <!-- Trust Selection -->
          <div class="grid gap-0">
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Select NHS Trust(s) (optional)</h3>
              <div class="relative inline-block group">
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                  <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                  </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                            group-hover:scale-100 w-[250px] -translate-x-[85%] left-1/2 top-5 rounded-md shadow-lg bg-white  
                            ring-1 ring-black ring-opacity-5 p-4">
                  <p class="text-sm text-gray-500">
                    Select up to 10 NHS Trusts to see their individual usage.
                    See <a href="/faq/#which-nhs-trusts-are-included" class="underline font-semibold" target="_blank">the FAQs</a> for more details 
                    on which trusts are included.
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
                maxItems={isAdvancedMode ? null : 10}
                hideSelectAll={!isAdvancedMode}
                showTitle={false}
              />
            </div>
          </div>
        </div>

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
        <div class="mt-2 pt-4 border-t border-gray-200">
          <div class="flex flex-col gap-4">
            <!-- Error Message -->
            {#if errorMessage}
              <div class="p-3 bg-red-100 border border-red-400 text-red-700 rounded">
                {errorMessage}
              </div>
            {/if}

            <!-- Action Buttons -->
            <div class="flex gap-2 justify-between">
              <button
                on:click={runAnalysis}
                disabled={isAnalysisRunning}
                class="w-64 px-6 sm:px-8 py-2 sm:py-2.5 bg-oxford-50 text-oxford-600 font-semibold rounded-md hover:bg-oxford-100 transition-colors duration-200
                     disabled:bg-gray-50 disabled:text-gray-400 disabled:cursor-not-allowed"
              >
                {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
              </button>
              <button
                on:click={handleClearAnalysis}
                title="Clear Analysis"
                class="p-2 sm:p-2.5 bg-white text-gray-700 font-normal rounded-md hover:bg-gray-100 transition-colors duration-200 border border-gray-200"
              >
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
