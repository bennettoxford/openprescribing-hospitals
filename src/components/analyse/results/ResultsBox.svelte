<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import TimeSeriesChart from './TimeSeriesChart.svelte';
    import DataTable from './DataTable.svelte';
    import ProductList from './ProductList.svelte';

    let selectedData = [];
    let quantityType = 'Dose';
    let vmps = [];
    let filteredData = [];
    let isLoading = false;
    let missingVMPs = [];
    let currentSearchType = 'vmp';
    let isAnalysisRunning = false;

    function handleUpdateData(event) {
        const { data, quantityType: newQuantityType, searchType } = event.detail;
        selectedData = Array.isArray(data) ? data : [data];
        quantityType = newQuantityType;
        currentSearchType = searchType;
        
        vmps = Array.from(new Set(selectedData.map(item => {
            return JSON.stringify({
                vmp: item.vmp_name,
                unit: item.unit,
                ingredient: item.ingredient_names ? item.ingredient_names[0] : null,
                vtm: item.vtm_name || null,
                searchType: currentSearchType
            });
        }))).map(JSON.parse);

        missingVMPs = vmps.filter(vmp => vmp.unit === 'nan').map(vmp => vmp.vmp);
        filteredData = selectedData.filter(item => item.unit !== 'nan');
        isLoading = false;
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp_name) && item.unit !== 'nan'
        );
        console.log('Filtered data in ResultsBox:', filteredData);
    }

    function handleClearResults() {
        selectedData = [];
        filteredData = [];
        vmps = [];
        missingVMPs = [];
        isLoading = true;
    }

    function handleAnalysisRunningChange(event) {
        isAnalysisRunning = event.detail;
    }

    onMount(() => {
        console.log("ResultsBox onMount called");
        const resultsBox = document.querySelector('results-box');
        resultsBox.addEventListener('updateData', handleUpdateData);
        resultsBox.addEventListener('clearResults', handleClearResults);
        resultsBox.addEventListener('analysisRunningChange', handleAnalysisRunningChange);

        return () => {
            resultsBox.removeEventListener('updateData', handleUpdateData);
            resultsBox.removeEventListener('clearResults', handleClearResults);
            resultsBox.removeEventListener('analysisRunningChange', handleAnalysisRunningChange);
        };
    });
</script>

<div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col">
    <h2 class="text-xl font-bold p-4">Results</h2>
    {#if isAnalysisRunning}
        <div class="flex-grow flex items-center justify-center">
            <div class="animate-spin rounded-full h-32 w-32 border-t-2 border-b-2 border-oxford-500"></div>
        </div>
    {:else if selectedData.length > 0}
        <div class="flex-grow overflow-y-auto">
            <div class="p-4">
                <ProductList {vmps} {currentSearchType} on:dataFiltered={handleFilteredData} />
            </div>
            {#if missingVMPs.length > 0}
                <div class="mx-4 p-4 bg-red-100 border-l-4 border-red-500 text-red-700">
                    <p class="font-bold">Warning: Missing quantities</p>
                    <p>The chosen quantity for the following VMPs can't be calculated and are excluded from the analysis:</p>
                    <ul class="list-disc list-inside mt-2">
                        {#each missingVMPs as vmp}
                            <li>{vmp}</li>
                        {/each}
                    </ul>
                </div>
            {/if}
            <div class="p-4">
                <TimeSeriesChart data={filteredData} {quantityType} searchType={currentSearchType} />
            </div>
            <div class="p-4 pt-0">
                <DataTable data={filteredData} {quantityType} searchType={currentSearchType} />
            </div>
        </div>
    {:else}
        <p class="p-4">Build a query to see results here.</p>
    {/if}
</div>
