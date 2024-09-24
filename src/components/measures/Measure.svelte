<svelte:options customElement={{
    tag: 'measure-component',
    shadow: 'none'
  }} />

<script>
    import { onMount, afterUpdate } from 'svelte';
    import Chart from 'chart.js/auto';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';

    export let measureData = '[]';

    let parsedData = [];
    let canvas;
    let chart;
    let organizations = [];
    let selectedOrganizations = [];
    let legendContainer;
    let usedOrganizationSelection = false;

    $: {
        
        try {
            if (typeof measureData === 'string') {
                let unescapedData = measureData.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                parsedData = JSON.parse(unescapedData);
            } else {
                parsedData = measureData;
            }
            organizations = [...new Set(parsedData.map(item => item.organization))];
            if (chart) {
                updateChart();
            }
        } catch (e) {
            console.error('Error parsing measureData:', e);
            parsedData = [];
        }
    }

    function prepareChartData(data) {
        if (data.length === 0) {
            return { labels: [], datasets: [] };
        }

        let filteredData;
        let breakdownKeys;

        if (usedOrganizationSelection) {
            if (selectedOrganizations.length > 0) {
                filteredData = data.filter(item => selectedOrganizations.includes(item.organization));
                breakdownKeys = selectedOrganizations;
            } else {
                return { labels: [], datasets: [] };
            }
        } else {
            filteredData = data;
            breakdownKeys = organizations;
        }

        const groupedData = filteredData.reduce((acc, item) => {
            const key = item.month;
            if (!acc[key]) {
                acc[key] = {};
            }
            const breakdownKey = item.organization;
            if (!acc[key][breakdownKey]) {
                acc[key][breakdownKey] = 0;
            }
            acc[key][breakdownKey] += parseFloat(item.quantity);
            return acc;
        }, {});

        const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));

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

    const scrollableLegendPlugin = {
        id: 'scrollableLegend',
        afterRender: (chart, args, options) => {
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
            const chartData = prepareChartData(parsedData);
            chart.data = chartData;
            chart.update();
        }
    }

    function handleOrganizationSelection(event) {
        selectedOrganizations = event.detail.selectedItems;
        usedOrganizationSelection = event.detail.usedOrganizationSelection;
        updateChart();
    }

    onMount(() => {
        Chart.register(scrollableLegendPlugin);
        chart = new Chart(canvas, {
            type: 'line',
            data: prepareChartData(parsedData),
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
</script>

<div class="flex flex-col">

    
    <div class="mb-4">
        <h4 class="text-md font-semibold mb-2">Filter Organizations</h4>
        <OrganisationSearch items={organizations} on:selectionChange={handleOrganizationSelection} />
    </div>
    
    <div class="flex">
        <div class="chart-container flex-grow" style="height: 400px;">
            {#if parsedData.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <canvas bind:this={canvas}></canvas>
            {/if}
        </div>
        <div bind:this={legendContainer} class="legend-container w-48 ml-4"></div>
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