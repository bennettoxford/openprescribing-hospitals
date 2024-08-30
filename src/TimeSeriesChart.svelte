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
    let chartType = 'total';
    let currentData = [];

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
                    text: 'Quantity'
                },
                ticks: {
                    callback: function(value, index, values) {
                        return value.toLocaleString(); // Format large numbers
                    }
                }
            }
        },
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: true,
                text: 'Quantity Over Time'
            },
            tooltip: {
                callbacks: {
                    label: function(context) {
                        let label = context.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (context.parsed.y !== null) {
                            label += context.parsed.y.toLocaleString();
                        }
                        return label;
                    }
                }
            }
        }
    };

    function updateChart(newData) {
        if (!chart) {
            console.error("Chart not initialized");
            return;
        }

        currentData = newData;
        let datasets;
        if (chartType === 'total') {
            datasets = prepareTotalLineChartData(newData);
        } else {
            datasets = prepareOrgLineChartData(newData);
        }

        chart.data.datasets = datasets;
        chart.options.scales.y.title.text = chartType === 'total' ? 'Total Quantity' : 'Quantity';
        chart.update();
    }

    function prepareTotalLineChartData(data) {
        const groupedData = data.reduce((acc, item) => {
            const key = `${item.vmp_name}_${item.year_month}`;
            if (!acc[key]) {
                acc[key] = {
                    vmp_name: item.vmp_name,
                    year_month: item.year_month,
                    quantity: 0
                };
            }
            acc[key].quantity += parseFloat(item.quantity);
            return acc;
        }, {});

        const sortedData = Object.values(groupedData).sort((a, b) => 
            a.vmp_name.localeCompare(b.vmp_name) || a.year_month.localeCompare(b.year_month)
        );

        return Object.values(sortedData.reduce((acc, item) => {
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
                y: item.quantity
            });
            return acc;
        }, {}));
    }

    function prepareOrgLineChartData(data) {
        const groupedData = data.reduce((acc, item) => {
            if (!acc[item.ods_name]) {
                acc[item.ods_name] = {};
            }
            if (!acc[item.ods_name][item.year_month]) {
                acc[item.ods_name][item.year_month] = 0;
            }
            acc[item.ods_name][item.year_month] += parseFloat(item.quantity);
            return acc;
        }, {});

        return Object.entries(groupedData).map(([odsName, values]) => ({
            label: odsName,
            data: Object.entries(values).map(([date, quantity]) => ({
                x: new Date(date),
                y: quantity
            })),
            backgroundColor: getRandomColor(),
            borderColor: getRandomColor(),
            fill: false
        }));
    }

    export function updateData(newData) {
        console.log("TimeSeriesChart updateData called with:", newData);
        updateChart(newData);
    }

    function getRandomColor() {
        return `rgba(${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, 0.6)`;
    }

    function handleChartTypeChange(event) {
        chartType = event.target.value;
        updateChart(currentData);
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
    <div class="mb-4">
        <label for="chart-type" class="mr-2">Chart Type:</label>
        <select id="chart-type" on:change={handleChartTypeChange} class="p-1 border border-gray-300 rounded">
            <option value="total">Total by VMP</option>
            <option value="org">By Organization</option>
        </select>
    </div>
    <div class="h-64">
        <canvas bind:this={chartCanvas}></canvas>
    </div>
</div>