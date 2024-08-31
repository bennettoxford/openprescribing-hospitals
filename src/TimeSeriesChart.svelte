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

    $: {
        console.log('Data received in TimeSeriesChart:', data);
        if (data.length > 0) {
            organizations = [...new Set(data.map(item => item.ods_name))];
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
            return {
                labels: [],
                datasets: []
            };
        }

        const groupedData = data.reduce((acc, item) => {
            const key = item.year_month;
            if (!acc[key]) {
                acc[key] = viewMode === 'Total' ? 0 : {};
            }
            if (viewMode === 'Total') {
                acc[key] += parseFloat(item.quantity);
            } else {
                acc[key][item.ods_name] = (acc[key][item.ods_name] || 0) + parseFloat(item.quantity);
            }
            return acc;
        }, {});

        const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));
        
        if (viewMode === 'Total') {
            return {
                labels: sortedDates,
                datasets: [{
                    label: `Total ${quantityType} over time`,
                    data: sortedDates.map(date => groupedData[date]),
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                }]
            };
        } else {
            return {
                labels: sortedDates,
                datasets: organizations.map((org, index) => ({
                    label: org,
                    data: sortedDates.map(date => groupedData[date][org] || 0),
                    borderColor: `hsl(${index * 360 / organizations.length}, 70%, 50%)`,
                    tension: 0.1
                }))
            };
        }
    }

    function updateChart() {
        console.log('Updating chart');
        if (chart) {
            const chartData = prepareChartData(data, viewMode);
            chart.data = chartData;
            chart.update();
        }
    }

    onMount(() => {
        console.log('Mounting TimeSeriesChart');
        chart = new Chart(canvas, {
            type: 'line',
            data: prepareChartData(data, viewMode),
            options: {
                responsive: true,
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
</script>

<div>
    <h3 class="text-lg font-semibold mb-2">Time Series Chart for {quantityType}</h3>
    <div class="mb-4">
        <label for="view-mode-select" class="mr-2">View Mode:</label>
        <select id="view-mode-select" bind:value={viewMode} on:change={updateChart} class="p-2 border rounded">
            <option value="Total">Total</option>
            <option value="Organization Breakdown">Organization Breakdown</option>
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