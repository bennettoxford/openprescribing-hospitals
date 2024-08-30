<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import Chart from 'chart.js/auto';
    import './styles/styles.css';

    let chartCanvas;
    let chart;
    let selectedDataset = 'total';

    const data = {
        labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul'],
        datasets: [
            {
                label: 'Sales',
                data: [65, 59, 80, 81, 56, 55, 40],
                backgroundColor: 'rgba(75, 192, 192, 0.6)',
            },
            {
                label: 'Revenue',
                data: [28, 48, 40, 19, 86, 27, 90],
                backgroundColor: 'rgba(153, 102, 255, 0.6)',
            }
        ]
    };

    $: chartData = getChartData(selectedDataset);

    function getChartData(selected) {
        if (selected === 'total') {
            return {
                labels: data.labels,
                datasets: [{
                    label: 'Total',
                    data: data.labels.map((_, i) => 
                        data.datasets.reduce((sum, dataset) => sum + dataset.data[i], 0)
                    ),
                    backgroundColor: 'rgba(255, 159, 64, 0.6)',
                }]
            };
        } else {
            return {
                labels: data.labels,
                datasets: [data.datasets.find(d => d.label === selected)]
            };
        }
    }

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            y: {
                beginAtZero: true
            }
        }
    };

    function updateChart() {
        if (chart) {
            chart.data = chartData;
            chart.update();
        }
    }

    $: if (chart && selectedDataset) {
        updateChart();
    }

    onMount(() => {
        chart = new Chart(chartCanvas, {
            type: 'bar',
            data: chartData,
            options: options
        });

        return () => {
            chart.destroy();
        };
    });
</script>

<div class="p-4 border border-gray-300 rounded-md">
    <h2 class="text-xl font-bold mb-4">Time Series Chart</h2>
    <div class="mb-4">
        <label for="dataset-select" class="mr-2">Select Dataset:</label>
        <select id="dataset-select" bind:value={selectedDataset} class="p-1 border border-gray-300 rounded">
            <option value="total">Total</option>
            {#each data.datasets as dataset}
                <option value={dataset.label}>{dataset.label}</option>
            {/each}
        </select>
    </div>
    <div class="h-64">
        <canvas bind:this={chartCanvas}></canvas>
    </div>
</div>