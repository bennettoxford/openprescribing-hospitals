<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import './styles/styles.css';
    import TimeSeriesChart from './TimeSeriesChart.svelte';
    import DataTable from './DataTable.svelte';
    import VMPList from './VMPList.svelte';

    let selectedData = [];
    let timeSeriesChart;
    let dataTable;
    let vmps = [];

    function handleUpdateData(event) {
        console.log("ResultsBox updateData called with:", event.detail);
        selectedData = event.detail;
        // Extract unique VMP names from the selectedData
        vmps = [...new Set(selectedData.map(item => item.vmp_name))];
        
        if (timeSeriesChart) {
            console.log("Updating TimeSeriesChart");
            timeSeriesChart.updateData(selectedData);
        } else {
            console.error("TimeSeriesChart not found");
        }
        if (dataTable) {
            console.log("Updating DataTable");
            dataTable.updateData(selectedData);
        } else {
            console.error("DataTable not found");
        }
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
    <VMPList {vmps} />
    <div class="mb-8">
        <TimeSeriesChart bind:this={timeSeriesChart} />
    </div>
    <div>
        <DataTable bind:this={dataTable} />
    </div>
</div>
