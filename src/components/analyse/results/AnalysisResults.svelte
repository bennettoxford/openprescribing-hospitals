<svelte:options customElement={{
    tag: 'analysis-results',
    shadow: 'none'
  }} />

<script>
    import TotalsTable from './TotalsTable.svelte';
    import ProductsTable from './ProductsTable.svelte';
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import Chart from '../../common/Chart.svelte';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { chartConfig } from '../../../utils/chartConfig.js';
    import ModeSelector from '../../common/ModeSelector.svelte';
    import { createChartStore } from '../../../stores/chartStore';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { formatNumber } from '../../../utils/utils';
    import pluralize from 'pluralize';

    export let className = '';
    export let isAnalysisRunning;
    export let analysisData;
    export let showResults;

    let selectedData = [];
    let vmps = [];
    let filteredData = [];

    let viewModes = [
        { value: 'total', label: 'Total' }
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
                return pluralize(unit);
            }
            return unit;
        });
        
        const combinedUnits = formattedUnits.join('/');
        
        resultsChartStore.setConfig({
            ...($resultsChartStore.config || {}),
            yAxisLabel: combinedUnits || 'units'
        });

        const allDates = [...new Set(data.flatMap(item => 
            item.data.map(([date]) => date)
        ))].sort();

        let maxValue;

        if ($modeSelectorStore.selectedMode === 'organisation') {
            const orgData = {};
            data.forEach(item => {
                const org = item.organisation__ods_name || 'Unknown';
                if (!orgData[org]) {
                    orgData[org] = {
                        name: org,
                        data: new Array(allDates.length).fill(0),
                        isPredecessor: false
                    };
                }
                
                item.data.forEach(([date, value]) => {
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            orgData[org].data[dateIndex] += numValue;
                        }
                    }
                });
            });

            for (const [org, orgInfo] of Object.entries(orgData)) {
                for (const [successor, predecessors] of $organisationSearchStore.predecessorMap.entries()) {
                    if (predecessors.includes(org)) {
                        orgData[org].isPredecessor = true;
        
                        if (orgData[successor]) {
                            orgData[successor].data = orgData[successor].data.map(
                                (value, index) => value + (orgData[org].data[index] || 0)
                            );
                        }
                    }
                }
            }

            datasets = Object.entries(orgData)
                .filter(([_, { isPredecessor }]) => !isPredecessor)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([org, { name, data }], index) => ({
                    label: name,
                    data: data,
                    color: getConsistentColor(org, index),
                    strokeOpacity: 1,
                    isOrganisation: true
                }));

            maxValue = Math.max(...Object.values(orgData)
                .filter(org => !org.isPredecessor)
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
        } else if ($modeSelectorStore.selectedMode === 'region') {
            const regionData = {};
            
            data.forEach(item => {
                const orgName = item.organisation__ods_name;
                let targetRegion = item.organisation__region;
                
                for (const [successor, predecessors] of $organisationSearchStore.predecessorMap.entries()) {
                    if (predecessors.includes(orgName)) {
                        const successorData = data.find(d => d.organisation__ods_name === successor);
                        if (successorData) {
                            targetRegion = successorData.organisation__region;
                        }
                        break;
                    }
                }
                
                if (!targetRegion) return;
                
                if (!regionData[targetRegion]) {
                    regionData[targetRegion] = {
                        data: new Array(allDates.length).fill(0)
                    };
                }
                
                item.data.forEach(([date, value]) => {
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            regionData[targetRegion].data[dateIndex] += numValue;
                        }
                    }
                });
            });

            datasets = Object.entries(regionData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([region, { data }], index) => ({
                    label: region,
                    data: data,
                    color: getConsistentColor(region, index),
                    strokeOpacity: 1,
                    isRegion: true
                }));

            maxValue = Math.max(...Object.values(regionData)
                .flatMap(region => region.data)
                .filter(v => v !== null && !isNaN(v)));

        } else if ($modeSelectorStore.selectedMode === 'icb') {
            const icbData = {};
            data.forEach(item => {
                const orgName = item.organisation__ods_name;
                let targetICB = item.organisation__icb;

                for (const [successor, predecessors] of $organisationSearchStore.predecessorMap.entries()) {
                    if (predecessors.includes(orgName)) {
                        const successorData = data.find(d => d.organisation__ods_name === successor);
                        if (successorData) {
                            targetICB = successorData.organisation__icb;
                        }
                        break;
                    }
                }
                
                if (!targetICB) return;
                
                if (!icbData[targetICB]) {
                    icbData[targetICB] = {
                        data: new Array(allDates.length).fill(0)
                    };
                }
                
                item.data.forEach(([date, value]) => {
                    const dateIndex = allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            icbData[targetICB].data[dateIndex] += numValue;
                        }
                    }
                });
            });

            datasets = Object.entries(icbData)
                .filter(([_, { data }]) => data.some(v => v > 0))
                .map(([icb, { data }], index) => ({
                    label: icb,
                    data: data,
                    color: getConsistentColor(icb, index),
                    strokeOpacity: 1,
                    isICB: true
                }));

            maxValue = Math.max(...Object.values(icbData)
                .flatMap(icb => icb.data)
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
            yAxisTickFormat: value => {
                const range = maxValue;
                
                let decimals = 1;
                if (typeof value === 'number' && !isNaN(value)) {
                    // Get the magnitude of the difference between consecutive values
                    const step = range / 10;
                    if (step > 0) {
                        // Calculate required decimals based on step size
                        decimals = Math.min(
                            Math.max(
                                1,
                                Math.ceil(Math.abs(Math.log10(step))) + 1
                            ),
                            5
                        );
                    }
                }
                
                return formatNumber(value, { maxDecimals: decimals });
            },
            tooltipValueFormat: value => formatNumber(value, { showUnit: true, unit: combinedUnits })
        };

        const chartData = {
            labels: allDates,
            datasets: datasets
        };

        resultsChartStore.setData(chartData);
        resultsChartStore.setDimensions({
            height: 600,
            margin: {
                top: 20,
                right: 20,
                bottom: 40,
                left: 80
            }
        });

        resultsChartStore.setConfig(chartConfig);

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

        const organisationsWithData = new Set(
            selectedData
                .filter(item => {
                    const org = item.organisation__ods_name;
                    const isPredecessor = Array.from($organisationSearchStore.predecessorMap.values())
                        .some(predecessors => predecessors.includes(org));
                    
                    return !isPredecessor && 
                        item.data && 
                        item.data.some(([_, value]) => value && !isNaN(parseFloat(value)));
                })
                .map(item => item.organisation__ods_code)
        );

        if (organisationsWithData.size > 1) {
            viewModes.push({ value: 'organisation', label: 'NHS Trust' });
        }

        if ($resultsStore.isAdvancedMode) {
            const mappedICBs = new Set(
                selectedData.map(item => {
                    const orgName = item.organisation__ods_name;
                    let targetICB = item.organisation__icb;
                    
                    for (const [successor, predecessors] of $organisationSearchStore.predecessorMap.entries()) {
                        if (predecessors.includes(orgName)) {
                            const successorData = selectedData.find(d => d.organisation__ods_name === successor);
                            if (successorData) {
                                targetICB = successorData.organisation__icb;
                            }
                            break;
                        }
                    }
                    return targetICB;
                }).filter(Boolean)
            );

            if (mappedICBs.size > 1) {
                viewModes.push({ value: 'icb', label: 'ICB' });
            }

            const uniqueRegions = new Set(selectedData.map(item => item.organisation__region).filter(Boolean));
            if (uniqueRegions.size > 1) {
                viewModes.push({ value: 'region', label: 'Region' });
            }
        }

        if (vmps.length > 1) {
            viewModes.push({ value: 'product', label: 'Product' });
        }

        const uniqueVtms = new Set(vmps.map(vmp => vmp.vtm).filter(vtm => vtm && vtm !== '-' && vtm !== 'nan'));
        if (uniqueVtms.size > 1) {
            viewModes.push({ value: 'productGroup', label: 'Product Group' });
        }

        const uniqueIngredients = new Set(
            vmps.flatMap(vmp => (vmp.ingredients || []))
                .filter(ing => ing && ing !== '-' && ing !== 'nan')
        );
        if (uniqueIngredients.size > 1) {
            viewModes.push({ value: 'ingredient', label: 'Ingredient' });
        }

        const uniqueUnits = new Set(
            vmps.flatMap(vmp => Array.from(vmp.units))
                .filter(unit => unit && unit !== '-' && unit !== 'nan')
        );
        if (uniqueUnits.size > 1) {
            viewModes.push({ value: 'unit', label: 'Unit' });
        }


    }

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

    function formatDate(date) {
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const d = new Date(date);
        return `${months[d.getMonth()]} ${d.getFullYear()}`;
    }

    function customTooltipFormatter(d) {
        const label = d.dataset.label || 'No label';
        const date = formatDate(d.date);
        const value = d.value;
        const chartConfig = $resultsChartStore.config;
        
        // Get the specific unit for products, otherwise use the combined units
        let unit;
        if (d.dataset.isProduct && vmps.length > 0) {
            // Find the matching VMP to get its specific unit
            const matchingVmp = vmps.find(vmp => vmp.vmp === d.dataset.label);
            const baseUnit = matchingVmp?.unit || chartConfig?.yAxisLabel || 'unit';
            
            unit = pluralize(baseUnit, value);
        } else {
            unit = chartConfig?.yAxisLabel || 'units';
        }

        const tooltipContent = [
            { text: label, class: 'font-medium' },
            { label: 'Date', value: date },
            { label: 'Value', value: `${formatNumber(value)} ${unit}` }
        ];

        if (d.dataset.isOrganisation || d.dataset.isProduct || d.dataset.isProductGroup) {
            if (d.dataset.numerator !== undefined && d.dataset.denominator !== undefined) {
                tooltipContent.push(
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator[d.index], { addCommas: true }) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator[d.index], { addCommas: true }) }
                );
            }
        }

        return tooltipContent;
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
                        <ProductsTable {vmps} on:dataFiltered={handleFilteredData} />
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
                        <div class="grid grid-cols-1 gap-4">
                            <div class="relative h-[550px] mb-6 sm:mb-0">
                                <Chart 
                                    store={resultsChartStore} 
                                    data={filteredData.length > 0 ? filteredData : selectedData}
                                    formatTooltipContent={customTooltipFormatter}
                                />
                            </div>
                        </div>
                    </section>

                    <section class="bg-amber-50 border-l-4 border-amber-400 p-4 mx-4 mb-4 mt-2 relative z-10">
                      <div class="flex flex-col sm:flex-row">
                        <div class="flex-shrink-0 mb-2 sm:mb-0">
                          <svg class="h-5 w-5 text-amber-400" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v4.5a.75.75 0 01-1.5 0v-4.5A.75.75 0 0110 5zm0 10a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" />
                          </svg>
                        </div>
                        <div class="sm:ml-3">
                          <p class="text-sm text-amber-700">
                            Individual NHS Trust data submissions may be incomplete or inconsistent. 
                            <a href="/submission-history/" class="font-medium underline hover:text-amber-800">
                              View Submission History
                            </a> to understand data quality issues for individual NHS Trusts.
                          </p>
                        </div>
                      </div>
                    </section>

                    <section class="p-4">
                        <TotalsTable 
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
                                    <p class="text-oxford-400 text-base max-w-md">
                                        No data was returned for the selected quantity type of the chosen products. 
                                        <a href="/faq/#missing-quantities" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                                            Learn more about why quantities might be missing
                                        </a>.
                                    </p>
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
