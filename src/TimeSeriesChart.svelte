<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import Chart from 'chart.js/auto';
    import { TimeScale } from 'chart.js';
    import 'chartjs-adapter-date-fns';
    import { enGB } from 'date-fns/locale';
    import './styles/styles.css';

    Chart.register(TimeScale);

    let chartCanvas;
    let chart;

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            x: {
                type: 'time',
                time: {
                    unit: 'month',
                    displayFormats: {
                        month: 'MMM yyyy'
                    }
                },
                adapters: {
                    date: {
                        locale: enGB
                    }
                },
                title: {
                    display: true,
                    text: 'Date'
                }
            },
            y: {
                beginAtZero: true,
                title: {
                    display: true,
                    text: 'SCMD Quantity'
                }
            }
        },
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: true,
                text: 'SCMD Quantity Over Time'
            }
        }
    };

    export function updateData(newData) {
        console.log("TimeSeriesChart updateData called with:", newData);
        
        if (!chart) {
            console.error("Chart not initialized");
            return;
        }

        // Group data by vmp_name and year_month, summing SCMD_quantity
        const groupedData = newData.reduce((acc, item) => {
            const key = `${item.vmp_name}_${item.year_month}`;
            if (!acc[key]) {
                acc[key] = {
                    vmp_name: item.vmp_name,
                    year_month: item.year_month,
                    SCMD_quantity: 0
                };
            }
            acc[key].SCMD_quantity += parseFloat(item.SCMD_quantity);
            return acc;
        }, {});

        // Convert grouped data to array and sort by vmp_name and year_month
        const sortedData = Object.values(groupedData).sort((a, b) => 
            a.vmp_name.localeCompare(b.vmp_name) || a.year_month.localeCompare(b.year_month)
        );

        // Prepare datasets
        const datasets = Object.values(sortedData.reduce((acc, item) => {
            if (!acc[item.vmp_name]) {
                acc[item.vmp_name] = {
                    label: item.vmp_name,
                    data: [],
                    backgroundColor: getRandomColor(),
                    borderColor: getRandomColor(),
                    fill: false
                };
            }
            acc[item.vmp_name].data.push({
                x: new Date(item.year_month),
                y: item.SCMD_quantity
            });
            return acc;
        }, {}));

        // Update chart data
        chart.data.datasets = datasets;
        chart.update();
    }

    function getRandomColor() {
        return `rgba(${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, 0.6)`;
    }

    onMount(() => {
        console.log("TimeSeriesChart onMount called");
        
        chart = new Chart(chartCanvas, {
            type: 'line',
            data: { datasets: [] },
            options: options
        });

        return () => {
            if (chart) {
                chart.destroy();
            }
        };
    });
</script>

<div class="p-4 border border-gray-300 rounded-md">
    <h2 class="text-xl font-bold mb-4">Time Series Chart</h2>
    <div class="h-64">
        <canvas bind:this={chartCanvas}></canvas>
    </div>
</div>