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
    const dispatch = createEventDispatcher();

    let isAnalysisRunning = false;
    let vmpNames = [];
    let odsNames = [];
    let selectedVMPs = [];
    let selectedODS = [];
    let filteredData = [];
    let quantityType = '--';
    let searchType = 'vmp';

    let errorMessage = '';
    let usedOrganisationSelection = false;
    let isOrganisationDropdownOpen = false;

    
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
        selectedVMPs = event.detail.items;
        searchType = event.detail.type;
        console.log("Selected VMPs:", selectedVMPs, "Search Type:", searchType);
    }

    function handleODSSelection(event) {
        selectedODS = event.detail.selectedItems;
        usedOrganisationSelection = event.detail.usedOrganisationSelection;
    }

    function handleQuantityTypeChange(event) {
        quantityType = event.target.value;
        console.log('Quantity type changed:', quantityType);
    }

    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
    }

    onMount(async () => {
        await fetchVMPNames();
        await fetchODSNames();
    });
</script>

<div class="p-4 bg-white rounded-lg w-full h-full flex flex-col">
    <h2 class="text-2xl font-bold mb-4 text-oxford">Analysis Builder</h2>
    
    <div class="mb-4 flex-shrink-0">
        <h3 class="text-lg font-semibold mb-2 text-oxford">Product selection</h3>
        <Search items={vmpNames} on:selectionChange={handleVMPSelection} />
    </div>
    
    <div class="mb-4 flex-shrink-0">
        <h3 class="text-lg font-semibold mb-2 text-oxford">Select quantity type</h3>
        <select 
            bind:value={quantityType}
            on:change={handleQuantityTypeChange}
            class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
        >
            {#each quantityOptions as option}
                <option value={option}>{option}</option>
            {/each}
        </select>
    </div>

    <div class="mb-4 flex-grow overflow-hidden">
        <h3 class="text-lg font-semibold mb-2 text-oxford">Select organisations</h3>
        <div class="relative h-full">
            <OrganisationSearch 
                items={odsNames} 
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
    
    <div class="mt-auto flex-shrink-0">
        <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="w-full bg-oxford-500 text-white font-bold py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-oxford-500 transition-colors duration-200
               hover:bg-oxford-600
               disabled:bg-oxford-300 disabled:cursor-not-allowed"
        >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
        </button>
    </div>
</div>

