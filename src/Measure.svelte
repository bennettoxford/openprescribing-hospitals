<svelte:options customElement={{
    tag: 'measure-component',
    shadow: 'none'
  }} />

<script>
    import { onMount, afterUpdate } from 'svelte';
    import Chart from 'chart.js/auto';

    export let measureData = '[]';

    let parsedData = [];
    let canvas;
    let chart;
    let viewMode = 'Organization';
    let organizations = [];
    let legendContainer; // Add this line to declare legendContainer

    $: {
        console.log('Received measureData:', measureData);
        try {
            if (typeof measureData === 'string') {
                let unescapedData = measureData.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                parsedData = JSON.parse(unescapedData);
            } else {
                parsedData = measureData;
            }
            console.log('Parsed measureData:', parsedData);
            organizations = [...new Set(parsedData.map(item => item.organization))];
            // Remove regions initialization
            if (chart) {
                updateChart();
            }
        } catch (e) {
            console.error('Error parsing measureData:', e);
            parsedData = [];
        }
    }

    function prepareChartData(data, viewMode) {
        if (data.length === 0) {
            return { labels: [], datasets: [] };
        }

        const groupedData = data.reduce((acc, item) => {
            const key = item.month;
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

        // Remove the 'Total' case
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

    function getBreakdownKey(item, viewMode) {
        return item.organization;
    }

    function getBreakdownKeys(viewMode) {
        return organizations;
    }

    const scrollableLegendPlugin = {
        id: 'scrollableLegend',
        afterRender: (chart, args, options) => {
            // Remove the check for 'Total' viewMode
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
        if (chart) {
            const chartData = prepareChartData(parsedData, viewMode);
            chart.data = chartData;
            chart.update('none');
        }
    }

    onMount(() => {
        Chart.register(scrollableLegendPlugin);
        chart = new Chart(canvas, {
            type: 'line',
            data: prepareChartData(parsedData, viewMode),
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
                            text: 'Quantity'
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
        if (chart) {
            updateChart();
        }
    });

    function handleViewModeChange() {
        updateChart();
    }
</script>

<div class="flex flex-col">
    <h3 class="text-lg font-semibold mb-2">Measure Chart</h3>
    <!-- Remove the view mode select element -->
    <div class="flex">
        <div class="chart-container flex-grow" style="height: 400px;">
            {#if parsedData.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
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