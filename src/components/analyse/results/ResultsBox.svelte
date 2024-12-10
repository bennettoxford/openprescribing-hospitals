<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import TimeSeriesChart from './TimeSeriesChart.svelte';
    import DataTable from './DataTable.svelte';
    import ProductList from './ProductList.svelte';
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';

    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let vmps = [];
    let filteredData = [];
    let missingVMPs = [];
    let currentSearchType = 'vmp';

    $: if (analysisData) {
        console.log("New analysis data received:", analysisData);
        handleUpdateData(analysisData);
    }

    $: {
        console.log("Updating resultsStore:", { isAnalysisRunning, showResults, analysisData });
        resultsStore.update(store => ({
            ...store,
            isAnalysisRunning,
            showResults,
            analysisData
        }));
    }

    function handleUpdateData(data) {
        console.log("Handling update data:", data);
        const { data: newData, quantityType: newQuantityType, searchType } = data;
        selectedData = Array.isArray(newData) ? newData : [newData];
        currentSearchType = searchType;
        
        resultsStore.update(store => ({
            ...store,
            quantityType: newQuantityType,
            searchType,
            isAnalysisRunning: false,
            dateRange: $analyseOptions.dateRange
        }));
        
        vmps = Array.from(new Set(selectedData.map(item => {
            return JSON.stringify({
                vmp: item.vmp_name,
                unit: item.unit,
                ingredient_name: item.ingredient_name || null,
                ingredient_code: item.ingredient_code || null,
                vtm: item.vtm_name || null,
                route_names: item.route_names || [],
                searchType: currentSearchType
            });
        }))).map(JSON.parse);

        missingVMPs = vmps.filter(vmp => vmp.unit === 'nan').map(vmp => vmp.vmp);
        filteredData = selectedData.filter(item => item.unit !== 'nan');

        resultsStore.update(store => ({
            ...store,
            filteredData
        }));

        console.log("Updated resultsStore:", $resultsStore);
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp_name) && item.unit !== 'nan'
        );
        console.log('Filtered data in ResultsBox:', filteredData);

        resultsStore.update(store => ({
            ...store,
            filteredData
        }));
    }
</script>

{#if $resultsStore.showResults}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if $resultsStore.isAnalysisRunning}
                <div class="flex items-center justify-center h-[500px] p-16">
                    <div class="animate-spin rounded-full h-32 w-32 border-t-4 border-b-4 border-oxford-500"></div>
                </div>
            {:else if $resultsStore.analysisData?.data?.length > 0}
                <div class="space-y-6 p-6">
                    <section class="bg-white rounded-lg p-4 border-2 border-oxford-300 shadow-sm">
                        <ProductList {vmps} currentSearchType={$resultsStore.searchType} on:dataFiltered={handleFilteredData} />
                    </section>

                    <section class="bg-gray-50 rounded-lg p-4">
                        <TimeSeriesChart data={$resultsStore.filteredData} quantityType={$resultsStore.quantityType} searchType={$resultsStore.searchType} />
                    </section>

                    <section class="bg-gray-50 rounded-lg p-4">
                        <DataTable data={$resultsStore.filteredData} quantityType={$resultsStore.quantityType} searchType={$resultsStore.searchType} />
                    </section>
                </div>
            {:else}
                <div class="flex items-center justify-center h-[500px] p-6">
                    <div class="text-center space-y-6">
                        <div>
                            <p class="text-oxford-600 text-xl font-medium mb-3">No analysis results</p>
                        </div>
                    </div>
                </div>
            {/if}
        </div>
    </div>
{:else}
    <div class="flex items-center justify-center h-[500px] p-6">
        <div class="text-center space-y-6">
            <div>
                <p class="text-oxford-600 text-xl font-medium mb-3">No analysis results to show</p>
                <p class="text-oxford-400 text-base max-w-md">Please run an analysis to see results here.</p>
            </div>
        </div>
    </div>
{/if}
