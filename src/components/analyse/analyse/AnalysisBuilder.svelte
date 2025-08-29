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
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { getCookie } from '../../../utils/utils';
    
    const dispatch = createEventDispatcher();

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isOrganisationDropdownOpen = false;

    $: selectedVMPs = $analyseOptions.selectedVMPs;
    $: searchType = $analyseOptions.searchType;

    export let orgData = null;
    export let mindate = null;
    export let maxdate = null;


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
    const quantityOptions = ['--', 'SCMD Quantity', 'Unit Dose Quantity', 'Ingredient Quantity', 'Defined Daily Dose Quantity'];

    async function runAnalysis() {
        if (isAnalysisRunning) return;

        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = "Please select at least one product or ingredient.";
            return;
        }

        
        modeSelectorStore.reset();

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
                    names: selectedVMPs,
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
                searchType,
                organisations: $organisationSearchStore.selectedItems
            });

            analyseOptions.setSelectedOrganisations($organisationSearchStore.selectedItems);

            updateResults(data, {
                searchType,
                selectedOrganisations: $organisationSearchStore.selectedItems,
                predecessorMap: $organisationSearchStore.predecessorMap
            });

            dispatch('analysisComplete', { 
                data: Array.isArray(data) ? data : [data],
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
        event.detail.items = event.detail.items.slice(0, 1);
        
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: event.detail.items
        }));
    }

    function handleODSSelection(event) {
        const { selectedItems, usedOrganisationSelection } = event.detail;
        organisationSearchStore.updateSelection(selectedItems, usedOrganisationSelection);
    }


    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
    }

    function resetSelections() {
        
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: [],
            searchType: 'vmp',
        }));
        
        organisationSearchStore.updateSelection([]);

        errorMessage = '';

        const searchComponent = document.querySelector('analysis-builder search-component');
        if (searchComponent) {
            searchComponent.clearInput();
        }
    }


    function handleClearAnalysis() {
        resetSelections();
        dispatch('analysisClear');
    }
</script>
<div class="mb-6">
  <div>
    <!-- Header -->
    <div class="p-4 sm:p-6 bg-white rounded-lg w-full">
      <div class="grid gap-6">
        <!-- Tabs -->
        <div>
          <p class="text-sm text-oxford">
           
              Run a custom analysis of hospitals stock control data using the options below. You can analyse 
              specific medicines or groups of medicines across different NHS Trusts.

          </p>
        </div>

        <div class="grid gap-6">
          <!-- Product Selection -->
          <div class="grid gap-4">
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">
                Select product(s)
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
                      Search for and select products to include to analyse. You can select individual products
                      or groups of products by ingredient, product group, or ATC code. See <a href="/faq/#which-medicines-and-devices-are-included" class="underline font-semibold" target="_blank">the FAQs</a> for more information of what products are available.
                  </p>
                </div>
              </div>
            </div>
            <div class="relative">
              <ProductSearch on:selectionChange={handleVMPSelection}/>
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
                  group-hover:scale-100 w-[300px] -translate-x-[85%] left-1/2 top-5 rounded-md shadow-lg bg-white  
                  ring-1 ring-black ring-opacity-5 p-4">
                  <div class="text-sm text-gray-500 space-y-3">
                    <div class="space-y-1 text-xs">
                      <p>Select up to 10 NHS Trusts.</p>
                      <ul>
                        <li><strong>No trusts selected:</strong> Shows national data with regional/ICB breakdowns available</li>
                        <li><strong>Trusts selected:</strong> Filters analysis results to the selected trusts</li>
                      </ul>
                    </div>
                    <div>
                      <p class="text-xs">
                        See <a href="/faq/#how-do-i-see-icb-regional-and-national-breakdowns" class="underline font-semibold" target="_blank">the FAQs</a> for more details
                        of how trust selection affects analysis modes and <a href="/faq/#which-nhs-trusts-are-included" class="underline font-semibold" target="_blank">which trusts are included</a>.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <div class="relative">
              <OrganisationSearch 
                source={organisationSearchStore}
                overlayMode={false}
                on:selectionChange={handleODSSelection}
                on:dropdownToggle={handleOrganisationDropdownToggle}
                maxItems=10
                hideSelectAll={true}
                showTitle={false}
              />
            </div>
          </div>
        </div>

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
