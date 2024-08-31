<svelte:options customElement={{
    tag: 'analyse-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import './styles/styles.css';
    import Search from './Search.svelte';
    import SearchableDropdown from './SearchableDropdown.svelte';

    let isAnalysisRunning = false;
    let vmpNames = [];
    let odsNames = [];
    let selectedVMPs = [];
    let selectedODS = [];
    let filteredData = [];
    let quantityType = '--'; // Set default to '--'

    let resultsBox;

    const quantityOptions = ['--', 'Dose', 'Ingredient Quantity'];

    let errorMessage = '';

    async function fetchVMPNames() {
        const response = await fetch('/api/unique-vmp-names/');
        vmpNames = await response.json();
    }

    async function fetchODSNames() {
        const response = await fetch('/api/unique-ods-names/');
        odsNames = await response.json();
    }

    async function runAnalysis() {
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
        if (selectedODS && selectedODS.length === 0) {
            errorMessage = "You've selected to filter by organizations, but haven't chosen any. Please select at least one organization or clear the organization filter.";
            return;
        }

        isAnalysisRunning = true;
        
        // Dispatch event to clear results
        const analyseBox = document.querySelector('analyse-box');
        if (analyseBox) {
            analyseBox.dispatchEvent(new CustomEvent('clearResults'));
        }
        
        let endpoint = quantityType === 'Dose' ? '/api/filtered-doses/' : '/api/filtered-ingredient-quantities/';
        
        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    vmp_names: selectedVMPs,
                    ods_names: selectedODS
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            filteredData = await response.json();
            console.log("Filtered data:", filteredData);
            
            // Ensure filteredData is an array
            if (!Array.isArray(filteredData)) {
                console.warn("Filtered data is not an array, converting to array");
                filteredData = [filteredData];
            }
            
            // Dispatch a custom event with the filtered data
            const analyseBox = document.querySelector('analyse-box');
            if (analyseBox) {
                analyseBox.dispatchEvent(new CustomEvent('runAnalysis', { 
                    detail: { data: filteredData, quantityType: quantityType }
                }));
            }
        } catch (error) {
            console.error("Error fetching filtered data:", error);
            filteredData = []; // Set to empty array in case of error
            errorMessage = "An error occurred while fetching data. Please try again.";
        } finally {
            isAnalysisRunning = false;
            console.log("Analysis completed");
        }
    }

    function handleVMPSelection(event) {
        selectedVMPs = event.detail;
        console.log("Selected VMPs:", selectedVMPs);
    }

    function handleODSSelection(event) {
        selectedODS = event.detail;
        console.log("Selected ODS:", selectedODS);
    }

    function handleQuantityTypeChange(event) {
        quantityType = event.target.value;
        console.log('Quantity type changed:', quantityType);
    }

    onMount(async () => {
        await fetchVMPNames();
        await fetchODSNames();

        resultsBox = document.querySelector('results-box');
        if (resultsBox) {
            console.log("ResultsBox found");
        } else {
            console.error("ResultsBox not found in onMount");
        }
    });
</script>

<div class="p-4 bg-white rounded-lg shadow-md w-full h-full flex flex-col">
    <h2 class="text-3xl font-bold mb-4">Analysis Builder</h2>
    
    <div class="mb-4">
        <h3 class="text-xl font-semibold mb-2">Product selection</h3>
        <Search items={vmpNames} on:selectionChange={handleVMPSelection} />
    </div>
    
    <div class="mb-4">
        <h3 class="text-xl font-semibold mb-2">Select Quantity Type</h3>
        <select 
            bind:value={quantityType}
            on:change={handleQuantityTypeChange}
            class="w-full p-2 border border-gray-300 rounded-md"
        >
            {#each quantityOptions as option}
                <option value={option}>{option}</option>
            {/each}
        </select>
    </div>

    <div class="mb-4">
        <h3 class="text-xl font-semibold mb-2">Filter ODS Names</h3>
        <SearchableDropdown items={odsNames} on:selectionChange={handleODSSelection} />
    </div>
    
    {#if errorMessage}
        <div class="mb-4 p-2 bg-red-100 border border-red-400 text-red-700 rounded">
            {errorMessage}
        </div>
    {/if}
    
    <div class="mt-6">
        <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="w-full bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded focus:outline-none focus:shadow-outline {isAnalysisRunning ? 'opacity-50 cursor-not-allowed' : ''}"
        >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
        </button>
    </div>
</div>

