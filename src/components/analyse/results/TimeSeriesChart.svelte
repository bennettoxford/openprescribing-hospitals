<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount, afterUpdate, onDestroy } from 'svelte';
    import * as d3 from 'd3';

    export let data = [];
    export let quantityType = 'Dose';
    export let searchType = 'vmp';

    let chartDiv;
    let viewMode = 'Total';
    let organisations = [];
    let units = [];
    let ingredientUnitPairs = [];
    let vtms = [];
    let tooltip;
    let resizeTimer;

    $: {
        console.log('Data received in TimeSeriesChart:', data);
        if (data.length > 0) {
            organisations = [...new Set(data.map(item => item.ods_name))];
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
            if (chartDiv) {
                updateChart();
            }
        } else {
            console.log('No data available for chart');
            if (chartDiv) {
                d3.select(chartDiv).selectAll('*').remove();
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
                    color: 'rgb(75, 192, 192)'
                }]
            };
        } else {
            return {
                labels: sortedDates,
                datasets: breakdownKeys.map((key, index) => ({
                    label: key,
                    data: sortedDates.map(date => groupedData[date][key] || 0),
                    color: `hsl(${index * 360 / breakdownKeys.length}, 70%, 50%)`
                }))
            };
        }
    }

    function getBreakdownKey(item, viewMode) {
        switch (viewMode) {
            case 'Organisation':
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
            case 'Organisation':
                return organisations;
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

    function updateChart() {
        if (chartDiv) {
            const chartData = prepareChartData(data, viewMode);
            const margin = { top: 20, right: 100, bottom: 50, left: 60 };
            const width = chartDiv.clientWidth - margin.left - margin.right;
            const height = Math.min(400, window.innerHeight * 0.6) - margin.top - margin.bottom;

            d3.select(chartDiv).selectAll('*').remove();

            const svg = d3.select(chartDiv)
                .append('svg')
                .attr('width', width + margin.left + margin.right)
                .attr('height', height + margin.top + margin.bottom)
                .append('g')
                .attr('transform', `translate(${margin.left},${margin.top})`);

            const x = d3.scaleTime()
                .domain(d3.extent(chartData.labels, d => new Date(d)))
                .range([0, width]);

            const y = d3.scaleLinear()
                .domain([0, d3.max(chartData.datasets, d => d3.max(d.data))]).nice()
                .range([height, 0]);

            // Add grid lines
            svg.append('g')
                .attr('class', 'grid')
                .attr('transform', `translate(0,${height})`)
                .call(d3.axisBottom(x)
                    .ticks(d3.timeYear.every(1))
                    .tickSize(-height)
                    .tickFormat(''))
                .selectAll('line')
                .attr('stroke', 'lightgrey')
                .attr('stroke-dasharray', '2,2');

            svg.append('g')
                .attr('class', 'grid')
                .call(d3.axisLeft(y)
                    .tickSize(-width)
                    .tickFormat(''))
                .selectAll('line')
                .attr('stroke', 'lightgrey')
                .attr('stroke-dasharray', '2,2');

            svg.selectAll('.grid path')
                .style('display', 'none');

            // Add axes
            svg.append('g')
                .attr('transform', `translate(0,${height})`)
                .call(d3.axisBottom(x)
                    .ticks(d3.timeYear.every(1))
                    .tickFormat(d3.timeFormat('%Y')))
                .selectAll('text')
                .style('font-size', '12px');

            svg.append('g')
                .call(d3.axisLeft(y)
                    .ticks(5) // Limit the number of ticks
                    .tickFormat(d3.format('.2s'))) // Use SI-prefix notation
                .selectAll('text')
                .style('font-size', '12px');

            // Add axis labels
            svg.append('text')
                .attr('transform', `translate(${width / 2},${height + margin.bottom - 10})`)
                .style('text-anchor', 'middle')
                .text('Date');

            svg.append('text')
                .attr('transform', 'rotate(-90)')
                .attr('y', 0 - margin.left)
                .attr('x', 0 - (height / 2))
                .attr('dy', '1em')
                .style('text-anchor', 'middle')
                .text(quantityType);

            // Add lines
            const line = d3.line()
                .x(d => x(new Date(d.date)))
                .y(d => y(d.value));

            const lines = svg.selectAll('.line')
                .data(chartData.datasets)
                .enter()
                .append('g')
                .attr('class', 'line');

            lines.append('path')
                .attr('fill', 'none')
                .attr('stroke', d => d.color)
                .attr('stroke-width', 2)
                .attr('d', d => line(d.data.map((value, i) => ({ date: chartData.labels[i], value }))))
                .attr('class', (d, i) => `line-${i}`);

            // Add legend (except for organisation breakdown)
            if (viewMode !== 'Organisation') {
                const legend = svg.selectAll('.legend')
                    .data(chartData.datasets)
                    .enter()
                    .append('g')
                    .attr('class', 'legend')
                    .attr('transform', (d, i) => `translate(${width + 10},${i * 20})`);

                legend.append('rect')
                    .attr('x', 0)
                    .attr('width', 18)
                    .attr('height', 18)
                    .style('fill', d => d.color);

                legend.append('text')
                    .attr('x', 24)
                    .attr('y', 9)
                    .attr('dy', '.35em')
                    .style('text-anchor', 'start')
                    .text(d => d.label);
            }

            // Tooltip
            tooltip = d3.select(chartDiv)
                .append('div')
                .attr('class', 'tooltip p-2 bg-gray-800 text-white rounded shadow-lg text-sm')
                .style('position', 'absolute')
                .style('pointer-events', 'none')
                .style('opacity', 0);

            function showTooltip(event, d, index) {
                tooltip.style('opacity', 1);
                svg.selectAll('.line path').style('opacity', 0.2);
                svg.select(`.line-${index}`).raise().style('opacity', 1).style('stroke-width', 3);
            }

            function moveTooltip(event, d) {
                const [xPos, yPos] = d3.pointer(event, chartDiv);
                const date = x.invert(xPos - margin.left);
                const tooltipWidth = tooltip.node().offsetWidth;
                const tooltipHeight = tooltip.node().offsetHeight;

                const bisectDate = d3.bisector(d => new Date(d)).left;
                const index = bisectDate(chartData.labels, date);
                const nearestIndex = index > 0 && (index === chartData.labels.length || (date - new Date(chartData.labels[index - 1])) < (new Date(chartData.labels[index]) - date)) ? index - 1 : index;
                const nearestDate = new Date(chartData.labels[nearestIndex]);
                const nearestValue = d.data[nearestIndex];

                const leftPosition = xPos + 10;
                const topPosition = yPos - tooltipHeight / 2;

                tooltip
                    .html(`<strong>${d.label}</strong><br><strong>Date:</strong> ${d3.timeFormat('%b %Y')(nearestDate)}<br><strong>${quantityType}:</strong> ${nearestValue.toLocaleString('en-GB', { maximumFractionDigits: 2 })}`)
                    .style('left', `${leftPosition}px`)
                    .style('top', `${topPosition}px`);
            }

            function hideTooltip() {
                tooltip.style('opacity', 0);
                svg.selectAll('.line path').style('opacity', 1).style('stroke-width', 2);
            }

            svg.selectAll('.line path')
                .on('mouseover', function(event, d) {
                    const index = chartData.datasets.findIndex(dataset => dataset === d);
                    showTooltip.call(this, event, d, index);
                })
                .on('mousemove', moveTooltip)
                .on('mouseout', hideTooltip)
                .on('click', function(event, d) {
                    const index = chartData.datasets.findIndex(dataset => dataset === d);
                    showTooltip.call(this, event, d, index);
                    moveTooltip.call(this, event, d);
                });

            // Add a transparent overlay for better tooltip interaction
            svg.append('rect')
                .attr('width', width)
                .attr('height', height)
                .style('fill', 'none')
                .style('pointer-events', 'all')
                .on('mousemove', function(event) {
                    const [xPos, yPos] = d3.pointer(event, this);
                    const date = x.invert(xPos);
                    const bisectDate = d3.bisector(d => new Date(d)).left;
                    const index = bisectDate(chartData.labels, date);
                    
                    const nearestDataset = chartData.datasets.reduce((nearest, dataset) => {
                        const yValue = y(dataset.data[index]);
                        const distance = Math.abs(yValue - yPos);
                        return distance < nearest.distance ? { dataset, distance } : nearest;
                    }, { dataset: null, distance: Infinity }).dataset;

                    if (nearestDataset) {
                        const datasetIndex = chartData.datasets.findIndex(dataset => dataset === nearestDataset);
                        showTooltip.call(this, event, nearestDataset, datasetIndex);
                        moveTooltip.call(this, event, nearestDataset);
                    }
                })
                .on('mouseout', hideTooltip);
        }
    }

    function handleViewModeChange() {
        updateChart();
    }

    function handleResize() {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => {
            updateChart();
        }, 250);
    }

    onMount(() => {
        updateChart();
        window.addEventListener('resize', handleResize);
    });

    onDestroy(() => {
        window.removeEventListener('resize', handleResize);
        clearTimeout(resizeTimer);
    });

    afterUpdate(() => {
        if (chartDiv) {
            updateChart();
        }
    });
</script>

<div class="flex flex-col">
    <h3 class="text-lg font-semibold mb-2">Time Series Chart for {quantityType}</h3>
    <div class="mb-4 flex items-center">
        <label for="view-mode-select" class="mr-2">View Mode:</label>
        <select id="view-mode-select" bind:value={viewMode} on:change={handleViewModeChange} class="p-2 border rounded mr-4">
            <option value="Total">Total</option>
            <option value="Organisation">Organisation Breakdown</option>
            <option value="Unit">Unit Breakdown</option>
            {#if searchType === 'vtm'}
                <option value="VTM">VTM Breakdown</option>
            {:else if searchType === 'ingredient'}
                <option value="Ingredient-Unit">Ingredient-Unit Breakdown</option>
            {/if}
        </select>
    </div>
    <div class="flex">
        <div class="chart-container flex-grow" style="height: auto; min-height: 300px;">
            {#if data.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available. Please select at least one VMP.</p>
            {:else}
                <div bind:this={chartDiv}></div>
            {/if}
        </div>
    </div>
</div>

<style>
    .chart-container {
        position: relative;
        width: 100%;
    }
</style>