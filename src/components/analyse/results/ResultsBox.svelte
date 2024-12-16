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
        // Handle the nested data structure
        selectedData = Array.isArray(data.data) ? data.data : [];
        
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
                        routes: item.route_names ? [item.route_names] : [],
                        units: new Set(),
                        searchType: data.searchType || $analyseOptions.searchType
                    };
                }
                // Add all units from the data array
                if (item.data) {
                    item.data.forEach(dataPoint => {
                        if (dataPoint[2]) {  // Check if unit exists
                            acc[key].units.add(dataPoint[2]);
                        }
                    });
                }
                return acc;
            }, {});

            // Convert to array and format units
            vmps = Object.values(vmpGroups)
                .filter(vmp => vmp.vmp) // Filter out undefined VMPs
                .map(vmp => ({
                    ...vmp,
                    unit: vmp.units.size > 0 ? Array.from(vmp.units).join(', ') : 'nan'
                }));

            console.log("Processed VMPs:", vmps);
            filteredData = selectedData; // Initialize with all data

            // Update the results store
            resultsStore.update(store => ({
                ...store,
                analysisData: selectedData,
                filteredData: selectedData,
                productData: processProductData(selectedData),
                showResults: true,
                searchType: data.searchType || $analyseOptions.searchType,
                quantityType: data.quantityType || $analyseOptions.quantityType
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
            selectedVMPs.some(vmp => vmp.vmp === item.vmp__name)
        );
        console.log('Filtered data:', filteredData);

        resultsStore.update(store => ({
            ...store,
            filteredData
        }));
    }
</script>

{#if showResults}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if isAnalysisRunning}
                <div class="flex items-center justify-center h-[500px] p-16">
                    <div class="animate-spin rounded-full h-32 w-32 border-t-4 border-b-4 border-oxford-500"></div>
                </div>
            {:else if selectedData.length > 0}
                <div class="space-y-6 p-6">
                    <section class="bg-white rounded-lg p-4 border-2 border-oxford-300 shadow-sm">
                        <ProductList {vmps} on:dataFiltered={handleFilteredData} />
                    </section>

                    <section class="bg-gray-50 rounded-lg p-4">
                        <TimeSeriesChart 
                            data={filteredData}
                            quantityType={$analyseOptions.quantityType}
                            searchType={$analyseOptions.searchType}
                        />
                    </section>

                    <section class="bg-gray-50 rounded-lg p-4">
                        <DataTable 
                            data={filteredData} 
                            quantityType={$analyseOptions.quantityType} 
                            searchType={$analyseOptions.searchType} 
                        />
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
