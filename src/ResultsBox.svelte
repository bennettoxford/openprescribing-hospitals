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

    let timeSeriesChart;
    let dataTable;
    let vmps = [];

    $: console.log('ResultsBox received data:', selectedData);

    function handleUpdateData(event) {
        const { data, quantityType: newQuantityType } = event.detail;
        selectedData = Array.isArray(data) ? data : [data];
        quantityType = newQuantityType;
        console.log('Updated data in ResultsBox:', selectedData);
        

        // update the vmps based on the selcted data. 
        // want to populate with the unique vmp,unit,ingredient combinations
        vmps = Array.from(new Set(selectedData.map(item => {
            const vmpObject = {
                vmp: item.vmp_name,
                unit: item.unit
            };
            if (item.ingredient_name) {
                vmpObject.ingredient = item.ingredient_name;
            }
            return JSON.stringify(vmpObject);
        }))).map(JSON.parse);
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
        <VMPList {vmps} />
        <div class="mb-8">
            <TimeSeriesChart data={selectedData} {quantityType} />
        </div>
        <div>
            <DataTable data={selectedData} {quantityType} />
        </div>
    {:else}
        <p>No data available. Please run an analysis.</p>
    {/if}
</div>
