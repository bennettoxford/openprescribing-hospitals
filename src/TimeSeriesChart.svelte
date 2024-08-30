<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import Chart from 'chart.js/auto';

    export let data = [];
    export let quantityType = 'Dose';

    let canvas;
    let chart;
    let viewMode = 'Total';
    let organizations = [];

    $: {
        if (data.length > 0) {
            organizations = [...new Set(data.map(item => item.ods_name))];
            updateChart();
        }
    }

    function prepareChartData(data, viewMode) {
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
        if (chart) {
            chart.data = prepareChartData(data, viewMode);
            chart.update();
        }
    }

    onMount(() => {
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
        <canvas bind:this={canvas}></canvas>
    </div>
</div>