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

    let resultsBox;

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
        isAnalysisRunning = true;
        
        try {
            const response = await fetch('/api/filtered-doses/', {
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
            
            // Update the results box with the filtered data
            if (resultsBox) {
                console.log("Updating ResultsBox with filtered data");
                resultsBox.dispatchEvent(new CustomEvent('updateData', { detail: filteredData }));
            } else {
                console.error("ResultsBox not found");
            }
        } catch (error) {
            console.error("Error fetching filtered data:", error);
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

<div class="analyse-box p-4 bg-white rounded-lg shadow-md">
    <h2 class="text-xl font-bold mb-4">Analysis Tools</h2>
    <div class="mb-4">
        <h3 class="text-lg font-semibold mb-2">Search VMP Names</h3>
        <Search items={vmpNames} on:selectionChange={handleVMPSelection} />
    </div>
    <div class="mb-4">
        <h3 class="text-lg font-semibold mb-2">Filter ODS Names</h3>
        <SearchableDropdown items={odsNames} on:selectionChange={handleODSSelection} />
    </div>
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

<style>
    .analyse-box {
        width: 100%;
        max-width: 300px;
    }
</style>