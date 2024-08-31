<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount, afterUpdate } from 'svelte';
    import Chart from 'chart.js/auto';

    export let data = [];
    export let quantityType = 'Dose';

    let canvas;
    let chart;
    let viewMode = 'Total';
    let organizations = [];
    let units = [];
    let ingredientUnitPairs = [];

    $: {
        console.log('Data received in TimeSeriesChart:', data);
        if (data.length > 0) {
            organizations = [...new Set(data.map(item => item.ods_name))];
            units = [...new Set(data.map(item => item.unit))];
            ingredientUnitPairs = [...new Set(data.map(item => `${item.ingredient_name || 'Unknown'}-${item.unit}`))];
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
                return `${item.ingredient_name || 'Unknown'}-${item.unit}`;
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
            default:
                return ['Total'];
        }
    }

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
        chart = new Chart(canvas, {
            type: 'line',
            data: prepareChartData(data, viewMode),
            options: {
                responsive: true,
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
                }
            }
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

<div>
    <h3 class="text-lg font-semibold mb-2">Time Series Chart for {quantityType}</h3>
    <div class="mb-4 flex items-center">
        <label for="view-mode-select" class="mr-2">View Mode:</label>
        <select id="view-mode-select" bind:value={viewMode} on:change={handleViewModeChange} class="p-2 border rounded mr-4">
            <option value="Total">Total</option>
            <option value="Organization">Organization Breakdown</option>
            <option value={quantityType === 'Dose' ? 'Unit' : 'Ingredient-Unit'}>
                {quantityType === 'Dose' ? 'Dose Unit Breakdown' : 'Ingredient-Unit Breakdown'}
            </option>
        </select>
    </div>
    <div class="chart-container" style="height: 400px;">
        {#if data.length === 0}
            <p class="text-center text-gray-500 pt-8">No data available. Please select at least one VMP.</p>
        {:else}
            <canvas bind:this={canvas}></canvas>
        {/if}
    </div>
</div>