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

    const data = {
        labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul'],
        datasets: [{
            label: 'Sales',
            data: [65, 59, 80, 81, 56, 55, 40],
            fill: false,
            borderColor: 'rgb(75, 192, 192)',
            tension: 0.1
        }]
    };

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            y: {
                beginAtZero: true
            }
        }
    };

    onMount(() => {
        chart = new Chart(chartCanvas, {
            type: 'line',
            data: data,
            options: options
        });

        return () => {
            chart.destroy();
        };
    });
</script>

<div class="p-4 border border-gray-300 rounded-md">
    <h2 class="text-xl font-bold mb-4">Time Series Chart</h2>
    <div class="h-64">
        <canvas bind:this={chartCanvas}></canvas>
    </div>
</div>