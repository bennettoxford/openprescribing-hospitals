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
    import { getCookie } from '../../../utils/utils';
    import { analyseOptions, clearAnalysisOptions } from '../../../stores/analyseOptionsStore';
    const dispatch = createEventDispatcher();

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isOrganisationDropdownOpen = false;
    let filteredData = [];

    $: selectedVMPs = $analyseOptions.selectedVMPs;
    $: selectedODS = $analyseOptions.selectedODS;
    $: quantityType = $analyseOptions.quantityType;
    $: searchType = $analyseOptions.searchType;
    $: usedOrganisationSelection = $analyseOptions.usedOrganisationSelection;
    $: odsNames = $analyseOptions.odsNames;

    const csrftoken = getCookie('csrftoken');
    // Define quantityOptions
    const quantityOptions = ['--', 'Dose', 'Ingredient Quantity'];

    async function fetchVMPNames() {
        const response = await fetch('/api/unique-vmp-names/');
        vmpNames = await response.json();
    }

    async function fetchODSNames() {
        const response = await fetch('/api/unique-ods-names/');
        odsNames = await response.json();
    }

    async function runAnalysis() {
        if (isAnalysisRunning) return; // Prevent multiple clicks

        console.log("Run Analysis button clicked");
        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = "Please select at least one product (VMP, Ingredient, or VTM).";
            return;
        }

        if (quantityType === '--') {
            errorMessage = "Please select a quantity type before running the analysis.";
            return;
        }

        // Updated check for ODS selection
        if (selectedODS && selectedODS.length === 0 && usedOrganisationSelection) {
            errorMessage = "You've selected to filter by organisations, but haven't chosen any. Please select at least one organisation or clear the organisation filter.";
            return;
        }

        isAnalysisRunning = true;
        dispatch('analysisStart'); // Dispatch event to parent when analysis starts
        
        let endpoint = quantityType === 'Dose' ? '/api/filtered-doses/' : '/api/filtered-ingredient-quantities/';
        
        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrftoken
                },
                body: JSON.stringify({
                    names: selectedVMPs,
                    ods_names: selectedODS,
                    search_type: searchType
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            filteredData = await response.json();
            console.log("Filtered data:", filteredData);

            if (!Array.isArray(filteredData)) {
                filteredData = [filteredData];
            }

            dispatch('analysisComplete', { 
                data: filteredData, 
                quantityType: quantityType, 
                searchType: searchType 
            });
        } catch (error) {
            console.error("Error fetching filtered data:", error);
            filteredData = [];
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
            selectedVMPs: event.detail.items,
            searchType: event.detail.type
        }));
        console.log("Selected Items:", $analyseOptions.selectedVMPs, "Search Type:", $analyseOptions.searchType);
    }

    function handleODSSelection(event) {
        analyseOptions.update(options => ({
            ...options,
            selectedODS: event.detail.selectedItems,
            usedOrganisationSelection: event.detail.usedOrganisationSelection
        }));
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

    function handleClearAnalysis() {
        clearAnalysisOptions();
        errorMessage = '';
        dispatch('analysisClear');
    }

    onMount(async () => {
        try {
            const response = await fetch('/api/get-search-items/');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();

            analyseOptions.update(store => ({
                ...store,
                ...data
            }));
        } catch (error) {
            console.error("Error fetching analysis options:", error);
            errorMessage = "An error occurred while fetching analysis options. Please try again.";
        }
    });
</script>

<div class="p-4 bg-white rounded-lg w-full h-full flex flex-col">
    <div class="flex flex-col mb-2">
        <p class="text-sm text-oxford">
            Run a custom analysis of hospitals stock control data using the options below. You can analyse 
            specific medicines or groups of medicines across different NHS Trusts.
        </p>
    </div>

    <div class="mb-2 flex-shrink-0">
        <div class="flex items-center mb-2">
            <h3 class="text-lg font-semibold text-oxford mr-2">Product selection</h3>
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
                        Search for and select products to include in the analysis. You can select individual products
                        (VMP) or groups of products by ingredient, therapeutic moiety (VTM), or 
                        therapeutic target (ATC). See <a href="/faq/#product-selection-types" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                    </p>
                </div>
            </div>
        </div>
        <Search on:selectionChange={handleVMPSelection} />
    </div>
    
    <div class="mb-4 flex-shrink-0">
        <div class="flex items-center mb-2">
            <h3 class="text-lg font-semibold text-oxford mr-2">Select quantity type</h3>
            <div class="relative inline-block group">
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                    <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                        <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                    </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                            group-hover:scale-100 w-[200px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                            ring-1 ring-black ring-opacity-5 p-4">
                    <p class="text-sm text-gray-500">
                        Select the quantity unit most relevant to the selected products to use for the analysis.
                        See <a href="/faq/#quantity-type" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                    </p>
                </div>
            </div>
        </div>
        <select 
            bind:value={$analyseOptions.quantityType}
            on:change={handleQuantityTypeChange}
            class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
        >
            {#each quantityOptions as option}
                <option value={option}>{option}</option>
            {/each}
        </select>
    </div>

    <div class="mb-4 flex-grow overflow-visible relative">
        <div class="flex flex-col mb-2">
            <div class="flex items-center mb-2">
                <h3 class="text-lg font-semibold text-oxford mr-2">Select Trusts</h3>
                <div class="relative inline-block group">
                    <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                        <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                        </svg>
                    </button>
                    <div class="absolute z-[100] scale-0 transition-all duration-100 origin-top transform 
                                group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                                ring-1 ring-black ring-opacity-5 p-4">
                        <p class="text-sm text-gray-500">
                            By default, the analysis will include all NHS Trusts in England. You can restrict the analysis by selecting specific trusts below.
                            See <a href="/faq/#trusts-included" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                        </p>
                    </div>
                </div>
            </div>
        </div>
        <div class="relative h-full" style="z-index: 1;">
            <OrganisationSearch 
                items={$analyseOptions.odsNames} 
                on:selectionChange={handleODSSelection} 
                on:dropdownToggle={handleOrganisationDropdownToggle}
            />
        </div>
    </div>
    
    {#if errorMessage}
        <div class="mb-4 p-2 bg-red-100 border border-red-400 text-red-700 rounded flex-shrink-0">
            {errorMessage}
        </div>
    {/if}
    
    <div class="mt-auto flex-shrink-0 space-y-2 relative" style="z-index: 2;">
        <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="w-full bg-oxford-500 text-white font-bold py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-oxford-500 transition-colors duration-200
               hover:bg-oxford-600
               disabled:bg-oxford-300 disabled:cursor-not-allowed"
        >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
        </button>
        <button
            on:click={handleClearAnalysis}
            class="w-full bg-gray-100 text-gray-700 font-bold py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-gray-400 transition-colors duration-200
               hover:bg-gray-200"
        >
            Clear Analysis
        </button>
    </div>
</div>
