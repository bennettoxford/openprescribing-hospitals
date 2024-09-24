<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount, afterUpdate } from 'svelte';
    import Chart from 'chart.js/auto';

    export let data = [];
    export let quantityType = 'Dose';
    export let searchType = 'vmp';

    let canvas;
    let chart;
    let viewMode = 'Total';
    let organizations = [];
    let units = [];
    let ingredientUnitPairs = [];
    let vtms = [];
    let legendContainer;

    $: {
        console.log('Data received in TimeSeriesChart:', data);
        if (data.length > 0) {
            organizations = [...new Set(data.map(item => item.ods_name))];
            units = [...new Set(data.map(item => item.unit))];
            if (searchType === 'ingredient') {
                ingredientUnitPairs = [...new Set(data.map(item => 
                    `${item.ingredient_name || item.ingredient_names[0] || 'Unknown'}-${item.unit}`
                ))];
            } else {
                ingredientUnitPairs = [...new Set(data.map(item => 
                    `${item.ingredient_names ? item.ingredient_names[0] : 'Unknown'}-${item.unit}`
                ))];
            }
            vtms = [...new Set(data.map(item => item.vtm_name || 'Unknown'))];
            if (chart) {
                updateChart();
            }
        } else {
            console.log('No data available for chart');
            if (chart) {
                chart.data.datasets = [];
                chart.data.labels = [];
                chart.update();
            }
        }
    }

    function prepareChartData(data, viewMode) {
        console.log('Preparing chart data:', data, viewMode);
        if (data.length === 0) {
            return { labels: [], datasets: [] };
        }

        const groupedData = data.reduce((acc, item) => {
            const key = item.year_month;
            if (!acc[key]) {
                acc[key] = {};
            }
            const breakdownKey = getBreakdownKey(item, viewMode);
            if (!acc[key][breakdownKey]) {
                acc[key][breakdownKey] = 0;
            }
            acc[key][breakdownKey] += parseFloat(item.quantity);
            return acc;
        }, {});

        const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));
        const breakdownKeys = getBreakdownKeys(viewMode);

        if (viewMode === 'Total') {
            return {
                labels: sortedDates,
                datasets: [{
                    label: `Total ${quantityType} over time`,
                    data: sortedDates.map(date => 
                        Object.values(groupedData[date]).reduce((sum, val) => sum + val, 0)
                    ),
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                }]
            };
        } else {
            return {
                labels: sortedDates,
                datasets: breakdownKeys.map((key, index) => ({
                    label: key,
                    data: sortedDates.map(date => groupedData[date][key] || 0),
                    borderColor: `hsl(${index * 360 / breakdownKeys.length}, 70%, 50%)`,
                    tension: 0.1
                }))
            };
        }
    }

    function getBreakdownKey(item, viewMode) {
        switch (viewMode) {
            case 'Organization':
                return item.ods_name;
            case 'Unit':
                return item.unit;
            case 'Ingredient-Unit':
                if (searchType === 'ingredient') {
                    return `${item.ingredient_name || item.ingredient_names[0] || 'Unknown'}-${item.unit}`;
                } else {
                    return `${item.ingredient_names ? item.ingredient_names[0] : 'Unknown'}-${item.unit}`;
                }
            case 'VTM':
                return item.vtm_name || 'Unknown';
            default:
                return 'Total';
        }
    }

    function getBreakdownKeys(viewMode) {
        switch (viewMode) {
            case 'Organization':
                return organizations;
            case 'Unit':
                return units;
            case 'Ingredient-Unit':
                return ingredientUnitPairs;
            case 'VTM':
                return vtms;
            default:
                return ['Total'];
        }
    }

    const scrollableLegendPlugin = {
        id: 'scrollableLegend',
        afterRender: (chart, args, options) => {
            if (viewMode === 'Total') {
                if (legendContainer) legendContainer.innerHTML = '';
                return;
            }

            const ul = document.createElement('ul');
            ul.style.overflowY = 'auto';
            ul.style.maxHeight = '350px';
            ul.style.padding = '10px';
            ul.style.margin = '0';
            ul.style.listStyle = 'none';

            chart.data.datasets.forEach((dataset, index) => {
                const li = document.createElement('li');
                li.style.display = 'flex';
                li.style.alignItems = 'center';
                li.style.marginBottom = '5px';
                li.style.cursor = 'pointer';

                const colorBox = document.createElement('span');
                colorBox.style.width = '20px';
                colorBox.style.height = '20px';
                colorBox.style.backgroundColor = dataset.borderColor;
                colorBox.style.display = 'inline-block';
                colorBox.style.marginRight = '5px';

                const text = document.createTextNode(dataset.label);

                li.appendChild(colorBox);
                li.appendChild(text);

                li.onclick = () => {
                    const meta = chart.getDatasetMeta(index);
                    meta.hidden = meta.hidden === null ? !chart.data.datasets[index].hidden : null;
                    chart.update();
                };

                ul.appendChild(li);
            });

            if (legendContainer) {
                legendContainer.innerHTML = '';
                legendContainer.appendChild(ul);
            }
        }
    };

    function updateChart() {
        console.log('Updating chart');
        if (chart) {
            const chartData = prepareChartData(data, viewMode);
            chart.data = chartData;
            chart.update('none');
        }
    }

    onMount(() => {
        console.log('Mounting TimeSeriesChart');
        Chart.register(scrollableLegendPlugin);
        chart = new Chart(canvas, {
            type: 'line',
            data: prepareChartData(data, viewMode),
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                transitions: {
                    active: {
                        animation: {
                            duration: 0
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: quantityType
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Date'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            },
            plugins: [scrollableLegendPlugin]
        });

        return () => {
            if (chart) {
                chart.destroy();
            }
        };
    });

    afterUpdate(() => {
        console.log('After update in TimeSeriesChart');
        if (chart) {
            updateChart();
        }
    });

    function handleViewModeChange() {
        updateChart();
    }
</script>

<div class="flex flex-col">
    <h3 class="text-lg font-semibold mb-2">Time Series Chart for {quantityType}</h3>
    <div class="mb-4 flex items-center">
        <label for="view-mode-select" class="mr-2">View Mode:</label>
        <select id="view-mode-select" bind:value={viewMode} on:change={handleViewModeChange} class="p-2 border rounded mr-4">
            <option value="Total">Total</option>
            <option value="Organization">Organization Breakdown</option>
            <option value="Unit">Unit Breakdown</option>
            {#if searchType === 'vtm'}
                <option value="VTM">VTM Breakdown</option>
            {:else if searchType === 'ingredient'}
                <option value="Ingredient-Unit">Ingredient-Unit Breakdown</option>
            {/if}
        </select>
    </div>
    <div class="flex">
        <div class="chart-container flex-grow" style="height: 400px;">
            {#if data.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available. Please select at least one VMP.</p>
            {:else}
                <canvas bind:this={canvas}></canvas>
            {/if}
        </div>
        <div bind:this={legendContainer} class="legend-container w-48 bg-white shadow-md ml-4"></div>
    </div>
</div>

<style>
    .chart-container {
        position: relative;
    }
    .legend-container {
        max-height: 400px;
        overflow-y: auto;
    }
</style>