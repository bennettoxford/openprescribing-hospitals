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
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp_name)
        );
        console.log('Filtered data in ResultsBox:', filteredData);
    }

    onMount(() => {
        console.log("ResultsBox onMount called");
        const resultsBox = document.querySelector('results-box');
        resultsBox.addEventListener('updateData', handleUpdateData);

        return () => {
            resultsBox.removeEventListener('updateData', handleUpdateData);
        };
    });
</script>

<div class="results-box p-4 bg-white rounded-lg shadow-md h-full flex flex-col">
    <h2 class="text-xl font-bold mb-4">Results</h2>
    {#if selectedData.length > 0}
        <VMPList {vmps} on:dataFiltered={handleFilteredData} />
        <div class="mb-8">
            <TimeSeriesChart data={filteredData} {quantityType} />
        </div>
        <div>
            <DataTable data={filteredData} {quantityType} />
        </div>
    {:else}
        <p>No data available. Please run an analysis.</p>
    {/if}
</div>
