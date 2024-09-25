<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import TimeSeriesChart from './TimeSeriesChart.svelte';
    import DataTable from './DataTable.svelte';
    import ProductList from './ProductList.svelte';

    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let quantityType = 'Dose';
    let vmps = [];
    let filteredData = [];
    let missingVMPs = [];
    let currentSearchType = 'vmp';

    $: if (analysisData) {
        handleUpdateData(analysisData);
    }

    function handleUpdateData(data) {
        const { data: newData, quantityType: newQuantityType, searchType } = data;
        selectedData = Array.isArray(newData) ? newData : [newData];
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
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp_name) && item.unit !== 'nan'
        );
        console.log('Filtered data in ResultsBox:', filteredData);
    }
</script>

{#if showResults}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if $isAnalysisRunning}
                <div class="flex items-center justify-center h-full p-16">
                    <div class="animate-spin rounded-full h-32 w-32 border-t-4 border-b-4 border-oxford-500"></div>
                </div>
            {:else if selectedData.length > 0}
                <div class="space-y-6 p-6">
                    <section class="bg-white rounded-lg p-4 border-2 border-oxford-300 shadow-sm">
                        <ProductList {vmps} {currentSearchType} on:dataFiltered={handleFilteredData} />
                    </section>

                    {#if missingVMPs.length > 0}
                        <section class="bg-red-50 border-l-4 border-red-500 text-red-700 p-4 rounded-r-lg">
                            <h3 class="font-bold text-lg mb-2">Warning: Missing quantities</h3>
                            <p class="mb-2">The chosen quantity for the following VMPs can't be calculated and are excluded from the analysis:</p>
                            <ul class="list-disc list-inside">
                                {#each missingVMPs as vmp}
                                    <li>{vmp}</li>
                                {/each}
                            </ul>
                        </section>
                    {/if}

                    <section class="bg-gray-50 rounded-lg p-4">
                        <TimeSeriesChart data={filteredData} {quantityType} searchType={currentSearchType} />
                    </section>

                    <section class="bg-gray-50 rounded-lg p-4">
                        <DataTable data={filteredData} {quantityType} searchType={currentSearchType} />
                    </section>
                </div>
            {:else}
                <div class="flex items-center justify-center h-full">
                    <p class="text-oxford text-lg">Analysis complete. No results to display.</p>
                </div>
            {/if}
        </div>
    </div>
{/if}
