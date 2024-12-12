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

    $: if (analysisData?.data) {
        console.log("New analysis data received:", analysisData);
        handleUpdateData(analysisData.data);
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
        // Ensure data is an array
        selectedData = Array.isArray(data) ? data : [data];
        
        // Process VMPs from the new data structure
        try {
            // Group by VMP and collect all unique units
            const vmpGroups = selectedData.reduce((acc, item) => {
                const key = item.vmp__name;
                if (!acc[key]) {
                    acc[key] = {
                        vmp: item.vmp__name,
                        code: item.vmp__code,
                        vtm: item.vmp__vtm__name,
                        ingredients: item.ingredients ? [item.ingredients] : [],
                        routes: item.routes ? [item.routes] : [],
                        units: new Set(),
                        searchType: $analyseOptions.searchType
                    };
                }
                // Add unit from data if it exists
                if (item.data?.[0]?.[2]) {
                    acc[key].units.add(item.data[0][2]);
                }
                return acc;
            }, {});

            // Convert to array and format units
            vmps = Object.values(vmpGroups).map(vmp => ({
                ...vmp,
                unit: vmp.units.size > 0 ? Array.from(vmp.units).join(', ') : 'nan'
            }));

            console.log("Processed VMPs:", vmps);

            // Update the results store
            resultsStore.update(store => ({
                ...store,
                analysisData: data,
                filteredData: selectedData,
                productData: processProductData(selectedData),
                showResults: true
            }));
        } catch (error) {
            console.error("Error processing data:", error);
            console.log("Data that caused error:", data);
        }
    }

    function processProductData(data) {
        if (!Array.isArray(data)) {
            console.error("Expected array for processProductData, got:", data);
            return {};
        }

        return data.reduce((acc, item) => {
            if (!item || !item.vmp__code) {
                console.warn("Invalid item in data:", item);
                return acc;
            }

            const key = item.vmp__code;
            try {
                acc[key] = {
                    name: item.vmp__name,
                    code: item.vmp__code,
                    vtm: item.vmp__vtm__name,
                    routes: item.routes ? [item.routes] : [],
                    ingredients: item.ingredients ? [item.ingredients] : [],
                    data: Array.isArray(item.data) ? item.data.map(([date, quantity, unit]) => ({
                        date,
                        quantity: parseFloat(quantity) || 0,
                        unit
                    })) : []
                };
            } catch (error) {
                console.error("Error processing item:", item, error);
            }
            return acc;
        }, {});
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item => 
            selectedVMPs.some(vmp => vmp.vmp === item.vmp__name) && 
            item.data?.[0]?.[2] !== 'nan'
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

                    <!-- <section class="bg-gray-50 rounded-lg p-4">
                        <TimeSeriesChart data={$resultsStore.filteredData} quantityType={$resultsStore.quantityType} searchType={$resultsStore.searchType} />
                    </section>

                    <section class="bg-gray-50 rounded-lg p-4">
                        <DataTable data={$resultsStore.filteredData} quantityType={$resultsStore.quantityType} searchType={$resultsStore.searchType} />
                    </section> -->
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
