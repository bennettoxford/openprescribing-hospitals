<svelte:options customElement={{
    tag: 'results-box',
    shadow: 'none'
  }} />

<script>
    import DataTable from './DataTable.svelte';
    import ProductList from './ProductList.svelte';
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import Chart from '../../common/Chart.svelte';
    import { legendStore } from '../../../stores/legendStore';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { chartConfig } from '../../../utils/chartConfig.js';
    import ModeSelector from '../../common/ModeSelector.svelte';
    import { createChartStore } from '../../../stores/chartStore';
    import ChartLegend from '../../common/ChartLegend.svelte';
    import { timeFormat } from 'd3-time-format';

    export let className = '';
    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let vmps = [];
    let filteredData = [];

    let viewModes = [
        { value: 'total', label: 'Total' },
        { value: 'organisation', label: 'NHS Trust' }
    ];

    const resultsChartStore = createChartStore({
        mode: 'trust',
        yAxisLabel: 'units',
        yAxisRange: [0, 100],
        visibleItems: new Set(),
        yAxisBehavior: {
            forceZero: true,
            padTop: 1.1,
            resetToInitial: true
        }
    });

    $: if (analysisData) {
        handleUpdateData(analysisData);
    }

    $: {
        resultsStore.update(store => ({
            ...store,
            isAnalysisRunning,
            showResults,
            analysisData
        }));
    }

    let colorMappings = new Map();

    function getOrganisationColor(index) {
        const allColors = chartConfig.allColours;
        
        return allColors[index % allColors.length];
    }

    function getConsistentColor(key, index) {
        if (!colorMappings.has(key)) {
            colorMappings.set(key, getOrganisationColor(colorMappings.size));
        }
        return colorMappings.get(key);
    }

    function formatAxisValue(value, maxValue) {
        if (value === 0) return '0';
        
        if (maxValue >= 1000000) {
            return `${(value / 1000000).toFixed(1)}m`;
        } else if (maxValue >= 1000) {
            return `${(value / 1000).toFixed(1)}k`;
        }
        
        return value.toFixed(0);
    }

    let datasets = [];

    function processChartData(data) {
        if (!Array.isArray(data)) {
            console.error("Expected array for processChartData, got:", data);
            return { labels: [], datasets: [] };
        }

        const uniqueUnits = new Set();
        data.forEach(item => {
            item.data.forEach(([_, __, unit]) => {
                if (unit) uniqueUnits.add(unit);
            });
        });
        
        const formattedUnits = Array.from(uniqueUnits).map(unit => {
            const count = data.reduce((acc, item) => {
                return acc + item.data.filter(([_, __, u]) => u === unit).length;
            }, 0);
            
            if (count > 1) {
                if (unit.endsWith('y')) {
                    return unit.slice(0, -1) + 'ies';
                } else if (!unit.endsWith('s')) {
                    return unit + 's';
                }
            }
            return unit;
        });
        
        const combinedUnits = formattedUnits.join('/');
        
        resultsChartStore.setConfig({
            ...($resultsChartStore.config || {}),
            yAxisLabel: combinedUnits || 'units'
        });

        function formatLargeNumber(value) {
            if (value === 0) return `0 ${combinedUnits}`;
            
            const unit = maxValue >= 1000000 ? 'm' : 
                        maxValue >= 1000 ? 'k' : 
                        '';
            
            const scaledValue = unit === 'm' ? value / 1000000 :
                               unit === 'k' ? value / 1000 :
                               value;
            
            return `${scaledValue.toFixed(1).replace(/\.0$/, '')}${unit} ${combinedUnits}`;
        }

        const allDates = [...new Set(data.flatMap(item => 
            item.data.map(([date]) => date)
        ))].sort();

        let maxValue;

        if ($modeSelectorStore.selectedMode === 'organisation') {
            const orgData = {};
            data.forEach(item => {
                const org = item.organisation__ods_name || 'Unknown';
                const orgCode = item.organisation__ods_code;
                
                if (!orgData[orgCode]) {
                    orgData[orgCode] = {
                        name: org,
                        data: new Array(allDates.length).fill(0)
                    };
                }
                
                item.data.forEach(([date, value]) => {
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            orgData[orgCode].data[dateIndex] += numValue;
                        }
                    }
                });
            });

            datasets = Object.entries(orgData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([orgCode, { name, data }], index) => ({
                    label: name,
                    data: data,
                    color: getConsistentColor(orgCode, index),
                    strokeOpacity: 1,
                    isOrganisation: true
                }));

            maxValue = Math.max(...Object.values(orgData)
                .flatMap(org => org.data)
                .filter(v => v !== null && !isNaN(v)));

        } else if ($modeSelectorStore.selectedMode === 'product') {

            const productData = {};
            data.forEach(item => {
                const productKey = item.vmp__code;
                const productName = item.vmp__name;
                
                if (!productData[productKey]) {
                    productData[productKey] = {
                        name: productName,
                        data: new Array(allDates.length).fill(0)
                    };
                }
                
                item.data.forEach(([date, value]) => {
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            productData[productKey].data[dateIndex] += numValue;
                        }
                    }
                });
            });

            datasets = Object.entries(productData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([code, { name, data }], index) => ({
                    label: name,
                    data: data,
                    color: getConsistentColor(code, index),
                    strokeOpacity: 1,
                    isProduct: true
                }));

            maxValue = Math.max(...Object.values(productData)
                .flatMap(product => product.data)
                .filter(v => v !== null && !isNaN(v)));

        } else if ($modeSelectorStore.selectedMode === 'total') {
            datasets = [{
                label: 'Total',
                data: allDates.map(date => {
                    const total = data.reduce((sum, item) => {
                        const dateData = item.data.find(([d]) => d === date);
                        return sum + (dateData ? parseFloat(dateData[1]) || 0 : 0);
                    }, 0);
                    return total;
                }),
                color: '#1e40af',
                strokeOpacity: 1,
                alwaysVisible: true
            }];

            maxValue = Math.max(...datasets[0].data);
        } else if ($modeSelectorStore.selectedMode === 'productGroup') {
            const vtmGroups = {};
            data.forEach(item => {
                const vtm = item.vmp__vtm__name || item.vtm__name || 'Unknown';
 
                if (!vtmGroups[vtm]) {
                    vtmGroups[vtm] = {
                        data: new Array(allDates.length).fill(0),
                        color: getConsistentColor(vtm, Object.keys(vtmGroups).length)
                    };
                }
                
                item.data.forEach(([date, value]) => {
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        vtmGroups[vtm].data[dateIndex] += parseFloat(value) || 0;
                    }
                });
            });

            datasets = Object.entries(vtmGroups)
                .filter(([_, data]) => data.data.some(v => v > 0))
                .map(([vtm, data]) => ({
                    label: vtm,
                    data: data.data,
                    color: data.color,
                    strokeOpacity: 1,
                    isProductGroup: true
                }));

            maxValue = Math.max(...datasets.flatMap(d => d.data));
        } else if ($modeSelectorStore.selectedMode === 'unit') {
            const unitData = {};
            data.forEach(item => {
                item.data.forEach(([date, value, unit]) => {
                    if (!unit) return;
                    
                    if (!unitData[unit]) {
                        unitData[unit] = {
                            data: new Array(allDates.length).fill(0)
                        };
                    }
                    
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            unitData[unit].data[dateIndex] += numValue;
                        }
                    }
                });
            });

            datasets = Object.entries(unitData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([unit, { data }], index) => ({
                    label: unit,
                    data: data,
                    color: getConsistentColor(unit, index),
                    strokeOpacity: 1,
                    isUnit: true
                }));

            maxValue = Math.max(...Object.values(unitData)
                .flatMap(unit => unit.data)
                .filter(v => v !== null && !isNaN(v)));

        } else if ($modeSelectorStore.selectedMode === 'route') {
            const routeData = {};
            data.forEach(item => {
                const routes = item.routes || ['Unknown'];
                routes.forEach(route => {
                    if (!routeData[route]) {
                        routeData[route] = {
                            data: new Array(allDates.length).fill(0)
                        };
                    }
                    
                    item.data.forEach(([date, value]) => {
                        const dateIndex = allDates.indexOf(date);
                        if (dateIndex !== -1) {
                            const numValue = parseFloat(value);
                            if (!isNaN(numValue)) {
                                routeData[route].data[dateIndex] += numValue / routes.length;
                            }
                        }
                    });
                });
            });

            datasets = Object.entries(routeData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([route, { data }], index) => ({
                    label: route,
                    data: data,
                    color: getConsistentColor(route, index),
                    strokeOpacity: 1,
                    isRoute: true
                }));

            maxValue = Math.max(...Object.values(routeData)
                .flatMap(route => route.data)
                .filter(v => v !== null && !isNaN(v)));
        } else if ($modeSelectorStore.selectedMode === 'ingredient') {
            const ingredientData = {};
            data.forEach(item => {
                const ingredients = item.ingredient_names || ['Unknown'];
                ingredients.forEach((ingredient, index) => {
                    if (!ingredientData[ingredient]) {
                        ingredientData[ingredient] = {
                            data: new Array(allDates.length).fill(0)
                        };
                    }
                    
                    item.data.forEach(([date, value]) => {
                        const dateIndex = allDates.indexOf(date);
                        if (dateIndex !== -1) {
                            const numValue = parseFloat(value);
                            if (!isNaN(numValue)) {
                                // If a product has multiple ingredients, split the quantity equally
                                ingredientData[ingredient].data[dateIndex] += numValue / ingredients.length;
                            }
                        }
                    });
                });
            });

            datasets = Object.entries(ingredientData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([ingredient, { data }], index) => ({
                    label: ingredient,
                    data: data,
                    color: getConsistentColor(ingredient, index),
                    strokeOpacity: 1,
                    isIngredient: true
                }));

            maxValue = Math.max(...Object.values(ingredientData)
                .flatMap(ingredient => ingredient.data)
                .filter(v => v !== null && !isNaN(v)));
        }

        if (maxValue === undefined) {
            maxValue = Math.max(...datasets.flatMap(d => d.data).filter(v => v !== null && !isNaN(v)));
        }
        maxValue = maxValue * 1.1;

        const chartConfig = {
            mode: $modeSelectorStore.selectedMode,
            yAxisLabel: combinedUnits || 'units',
            visibleItems: new Set(datasets.map(d => d.label)),
            yAxisRange: [0, maxValue],
            yAxisBehavior: {
                forceZero: true,
                padTop: 1.1,
                resetToInitial: true
            },
            yAxisTickFormat: value => formatAxisValue(value, maxValue),
            tooltipValueFormat: value => formatLargeNumber(value, combinedUnits)
        };

        const chartData = {
            labels: allDates,
            datasets: datasets
        };

        resultsChartStore.setData(chartData);
        resultsChartStore.setDimensions({
            height: 400,
            margin: {
                top: 20,
                right: 20,
                bottom: 40,
                left: 80
            }
        });

        resultsChartStore.setConfig(chartConfig);

        legendStore.setItems(datasets.map(d => ({
            label: d.label,
            color: d.color,
            visible: true,
            selectable: true
        })));
        
        legendStore.setVisibleItems(datasets.map(d => d.label));

        return { labels: allDates, datasets };
    }

    function handleUpdateData(data) {
        selectedData = Array.isArray(data.data) ? data.data : [];
        
        try {
            const vmpGroups = selectedData.reduce((acc, item) => {
                const key = item.vmp__name;
                if (!acc[key]) {
                    acc[key] = {
                        vmp: item.vmp__name,
                        code: item.vmp__code,
                        vtm: item.vmp__vtm__name,
                        ingredients: item.ingredient_names || [],
                        routes: item.routes || [],
                        units: new Set(),
                        searchType: data.searchType || $analyseOptions.searchType
                    };
                }
                if (item.data) {
                    item.data.forEach(dataPoint => {
                        if (dataPoint[2]) {
                            acc[key].units.add(dataPoint[2]);
                        }
                    });
                }
                return acc;
            }, {});

            vmps = Object.values(vmpGroups)
                .filter(vmp => vmp.vmp) // Filter out undefined VMPs
                .map(vmp => ({
                    ...vmp,
                    unit: vmp.units.size > 0 ? Array.from(vmp.units).join(', ') : 'nan',
                    routes: Array.isArray(vmp.routes) ? vmp.routes : []
                }));

            filteredData = selectedData;


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
        }
    }

    function processProductData(data) {
        if (!Array.isArray(data)) {
            return {};
        }

        return data.reduce((acc, item) => {
            if (!item || !item.vmp__code) {
                return acc;
            }

            const key = item.vmp__code;
            try {
                acc[key] = {
                    name: item.vmp__name,
                    code: item.vmp__code,
                    vtm: item.vmp__vtm__name,
                    routes: item.routes || [],
                    ingredients: item.ingredient_names || [],
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

        const chartData = processChartData(
            filteredData
        );

        resultsChartStore.setData({
            ...chartData,
            forceUpdate: Date.now()
        });

        resultsStore.update(store => ({
            ...store,
            filteredData
        }));
    }

    $: if ($modeSelectorStore.selectedMode && selectedData.length > 0) {
        
        const dataToProcess = filteredData.length > 0 ? filteredData : selectedData;
        
        processChartData(
            dataToProcess
        );
    }

    $: if (analysisData) {
        handleUpdateData(analysisData);
        
        const initialData = selectedData.length > 0 ? selectedData : analysisData.data;
        processChartData(
            initialData,
        );
    }

    $: {
        viewModes = [
            { value: 'total', label: 'Total' }
        ];

        // Only add organisation mode if there are multiple organisations
        const uniqueOrgs = new Set(selectedData.map(item => item.organisation__ods_code));
        if (uniqueOrgs.size > 1) {
            viewModes.push({ value: 'organisation', label: 'NHS Trust' });
        }

        if (vmps.length > 1) {
            viewModes.push({ value: 'product', label: 'Product' });
        }

        const uniqueVtms = new Set(vmps.map(vmp => vmp.vtm));
        if (uniqueVtms.size > 1) {
            viewModes.push({ value: 'productGroup', label: 'Product Group' });
        }

        const uniqueIngredients = new Set(vmps.flatMap(vmp => vmp.ingredients || []));
        if (uniqueIngredients.size > 1) {
            viewModes.push({ value: 'ingredient', label: 'Ingredient' });
        }

        const uniqueUnits = new Set(vmps.flatMap(vmp => Array.from(vmp.units)));
        if (uniqueUnits.size > 1) {
            viewModes.push({ value: 'unit', label: 'Unit' });
        }

        const uniqueRoutes = new Set(vmps.flatMap(vmp => vmp.routes));
        if (uniqueRoutes.size > 1) {
            viewModes.push({ value: 'route', label: 'Route' });
        }
    }

    function handleLegendChange(items) {
        const newVisible = new Set(items);
        
        const currentData = $resultsChartStore.data;
        
        const updatedData = {
            ...currentData,
            datasets: currentData.datasets.map(dataset => ({
                ...dataset,
                hidden: !newVisible.has(dataset.label)
            }))
        };
        
        resultsChartStore.setData(updatedData);
        legendStore.setVisibleItems(items);
    }

    $: legendItems = $modeSelectorStore.selectedMode === 'organisation' ? 
        datasets?.map((dataset, index) => ({
            label: dataset.label,
            color: dataset.color
        })) || [] :
        $modeSelectorStore.selectedMode === 'product' ?
        datasets?.map((dataset, index) => ({
            label: dataset.label,
            color: dataset.color
        })) || [] :
        datasets?.map(dataset => ({
            label: dataset.label,
            color: dataset.color
        })) || [];

    function handleModeChange() {
        const dataToProcess = filteredData.length > 0 ? filteredData : selectedData;
        processChartData(
            dataToProcess
        );
    }

    $: if (analysisData) {
        colorMappings.clear();
        handleUpdateData(analysisData);
    }

    function customTooltipFormatter(d) {
        const label = d.dataset.label || 'No label';
        const formatDate = timeFormat('%b %Y');
        const date = formatDate(d.date);
        const value = d.value;
        const chartConfig = $resultsChartStore.config;
        
        // Get the specific unit for products, otherwise use the combined units
        let unit;
        if (d.dataset.isProduct && vmps.length > 0) {
            // Find the matching VMP to get its specific unit
            const matchingVmp = vmps.find(vmp => vmp.vmp === d.dataset.label);
            const baseUnit = matchingVmp?.unit || chartConfig?.yAxisLabel || 'unit';
            
            if (value !== 1) {
                if (baseUnit.endsWith('y')) {
                    unit = baseUnit.slice(0, -1) + 'ies';
                } else if (!baseUnit.endsWith('s')) {
                    unit = baseUnit + 's';
                } else {
                    unit = baseUnit;
                }
            } else {
                unit = baseUnit;
            }
        } else {
            unit = chartConfig?.yAxisLabel || 'units';
        }

        const tooltipContent = [
            { text: label, class: 'font-medium' },
            { label: 'Date', value: date },
            { label: 'Value', value: `${formatLargeNumber(value)} ${unit}` }
        ];

        if (d.dataset.isOrganisation || d.dataset.isProduct || d.dataset.isProductGroup) {
            if (d.dataset.numerator !== undefined && d.dataset.denominator !== undefined) {
                tooltipContent.push(
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator[d.index]) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator[d.index]) }
                );
            }
        }

        return tooltipContent;
    }

    function formatLargeNumber(value) {
        if (value === 0) return '0';
        
        if (value >= 1000000) {
            return `${(value / 1000000).toFixed(1)}m`;
        } else if (value >= 1000) {
            return `${(value / 1000).toFixed(1)}k`;
        }
        
        return value.toFixed(1);
    }

    function formatNumber(value) {
        if (value == null || isNaN(value)) return 'N/A';
        return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    function hasChartableData(data) {
        if (!Array.isArray(data) || data.length === 0) return false;

        return data.some(item => 
            Array.isArray(item.data) && 
            item.data.length > 0 &&
            item.data.some(([_, value]) => value && !isNaN(parseFloat(value)))
        );
    }
</script>

{#if showResults}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col {className}">
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
                    {#if hasChartableData(selectedData)}
                    <section class="p-4">
                        <div class="mb-4">
                            <ModeSelector 
                                options={viewModes}
                                initialMode="total"
                                label="View Mode"
                                variant="pill"
                                onChange={handleModeChange}
                            />
                        </div>
                        <div class="grid grid-cols-1 lg:grid-cols-4 gap-4">
                            <div class={`relative h-[400px] ${$modeSelectorStore.selectedMode === 'total' ? 'lg:col-span-4' : 'lg:col-span-3'}`}>
                                <Chart 
                                    store={resultsChartStore} 
                                    data={filteredData.length > 0 ? filteredData : selectedData}
                                    formatTooltipContent={customTooltipFormatter}
                                />
                            </div>
                            {#if $modeSelectorStore.selectedMode !== 'total'}
                                <div class="legend-container lg:h-[400px] overflow-y-auto bg-white">
                                    <ChartLegend 
                                        items={datasets.map(d => ({
                                            label: d.label,
                                            color: d.color,
                                            visible: true,
                                            selectable: true
                                        }))}
                                        onChange={handleLegendChange}
                                    />
                                </div>
                            {/if}
                            </div>
                        </section>
                        <section class="p-4">
                            <DataTable 
                                data={filteredData} 
                                quantityType={$analyseOptions.quantityType} 
                                searchType={$analyseOptions.searchType} 
                            />
                        </section>
                    {:else}
                        <div class="flex items-center justify-center h-[500px] p-6">
                            <div class="text-center space-y-6">
                                <div>
                                    <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                                    <p class="text-oxford-400 text-base max-w-md">No data was returned for the selected quantity type of the chose products.</p>
                                </div>
                            </div>
                        </div>
                    {/if}

                   
                </div>
            {:else}
                <div class="flex items-center justify-center h-[500px] p-6">
                    <div class="text-center space-y-6">
                        <div>
                            <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                            <p class="text-oxford-400 text-base max-w-md">The analysis returned no chartable data.</p>
                        </div>
                    </div>
                </div>
            {/if}
        </div>
    </div>
{:else}
    <div class="flex items-center justify-center h-[500px] p-6 {className}">
        <div class="text-center space-y-6">
            <div>
                <p class="text-oxford-600 text-xl font-medium mb-3">No analysis results to show</p>
                <p class="text-oxford-400 text-base max-w-md">Please run an analysis to see results here.</p>
            </div>
        </div>
    </div>
{/if}

<style>
    @media (max-width: 1024px) {
        .legend-container {
            max-height: 200px;
        }
    }
</style>
