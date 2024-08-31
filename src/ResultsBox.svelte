<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import TimeSeriesChart from './TimeSeriesChart.svelte';
    import DataTable from './DataTable.svelte';
    import VMPList from './VMPList.svelte';

    let selectedData = [];
    let quantityType = 'Dose';
    let vmps = [];
    let filteredData = [];
    let isLoading = false;

    function handleUpdateData(event) {
        const { data, quantityType: newQuantityType } = event.detail;
        selectedData = Array.isArray(data) ? data : [data];
        quantityType = newQuantityType;
        console.log('Updated data in ResultsBox:', selectedData);
        
        vmps = Array.from(new Set(selectedData.map(item => {
            return JSON.stringify({
                vmp: item.vmp_name,
                unit: item.unit,
                ingredient: item.ingredient_name || null
            });
        }))).map(JSON.parse);

        filteredData = selectedData;
        isLoading = false;
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp_name)
        );
        console.log('Filtered data in ResultsBox:', filteredData);
    }

    function handleClearResults() {
        selectedData = [];
        filteredData = [];
        vmps = [];
        isLoading = true;
    }

    onMount(() => {
        console.log("ResultsBox onMount called");
        const resultsBox = document.querySelector('results-box');
        resultsBox.addEventListener('updateData', handleUpdateData);
        resultsBox.addEventListener('clearResults', handleClearResults);

        return () => {
            resultsBox.removeEventListener('updateData', handleUpdateData);
            resultsBox.removeEventListener('clearResults', handleClearResults);
        };
    });
</script>

<div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col">
    <h2 class="text-xl font-bold p-4">Results</h2>
    {#if isLoading}
        <p class="p-4">Loading results...</p>
    {:else if selectedData.length > 0}
        <div class="flex-grow overflow-y-auto">
            <div class="p-4">
                <VMPList {vmps} on:dataFiltered={handleFilteredData} />
            </div>
            <div class="p-4">
                <TimeSeriesChart data={filteredData} {quantityType} />
            </div>
            <div class="p-4 pt-0">
                <DataTable data={filteredData} {quantityType} />
            </div>
        </div>
    {:else}
        <p class="p-4">No data available. Please run an analysis.</p>
    {/if}
</div>
