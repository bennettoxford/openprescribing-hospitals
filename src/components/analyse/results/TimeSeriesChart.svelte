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
    let visibleDatasets = [];

    let chartContainer;
    let isSmallScreen = false;

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
            const datasets = breakdownKeys.map((key, index) => ({
                label: key,
                data: sortedDates.map(date => groupedData[date][key] || 0),
                color: `hsl(${index * 360 / breakdownKeys.length}, 70%, 50%)`
            }));
            console.log("Prepared datasets:", datasets);
            return { labels: sortedDates, datasets };
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
            visibleDatasets = chartData.datasets.map((_, i) => i);
            
            isSmallScreen = window.innerWidth < 768; // Adjust this breakpoint as needed

            const showLegend = viewMode !== 'Total' && viewMode !== 'Organisation' && !isSmallScreen;
            const legendWidth = showLegend ? 150 : 0; // Adjust this value as needed
            const margin = { top: 20, right: 20 + legendWidth, bottom: 50, left: 60 };
            const width = chartContainer.clientWidth - margin.left - margin.right;
            const height = Math.min(400, window.innerHeight * 0.6) - margin.top - margin.bottom;

            d3.select(chartDiv).selectAll('*').remove();

            const svg = d3.select(chartDiv)
                .append('svg')
                .attr('width', chartContainer.clientWidth)
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

            function updateYScale() {
                const visibleData = chartData.datasets
                    .filter((_, i) => visibleDatasets.includes(i))
                    .flatMap(d => d.data);
                
                y.domain([0, d3.max(visibleData)]).nice();

                // Update y-axis
                svg.select('.y-axis')
                    .call(d3.axisLeft(y)
                        .ticks(5)
                        .tickFormat(d3.format('.2s')));

                // Update y-grid
                svg.select('.grid.y')
                    .call(d3.axisLeft(y)
                        .ticks(5)
                        .tickSize(-width)
                        .tickFormat(''))
                    .call(g => g.select('.domain').remove())
                    .call(g => g.selectAll('.tick line')
                        .attr('stroke', 'lightgrey')
                        .attr('stroke-dasharray', '2,2'));

                // Adjust y-axis labels to prevent overlap
                svg.selectAll('.y-axis text')
                    .attr('dy', function(d, i, nodes) {
                        const thisWidth = this.getBBox().width;
                        const prevWidth = i > 0 ? nodes[i-1].getBBox().width : 0;
                        return i > 0 && Math.abs(y(d) - y(+nodes[i-1].textContent)) < Math.max(thisWidth, prevWidth) ? '0.7em' : '0.35em';
                    });
            }

            function updateLines() {
                const lines = svg.selectAll('.line')
                    .data(chartData.datasets.filter((_, i) => visibleDatasets.includes(i)));

                lines.exit().remove();

                lines.enter()
                    .append('path')
                    .attr('class', 'line')
                    .merge(lines)
                    .attr('fill', 'none')
                    .attr('stroke', d => d.color)
                    .attr('stroke-width', 2)
                    .attr('d', d => line(d.data.map((value, i) => ({ date: new Date(chartData.labels[i]), value }))));
            }

            // Initial drawing of lines
            updateLines();

            // Modify the initial y-axis and y-grid creation
            svg.append('g')
                .attr('class', 'y-axis')
                .call(d3.axisLeft(y)
                    .ticks(5)
                    .tickFormat(d3.format('.2s')))
                .selectAll('text')
                .style('font-size', '12px');

            svg.append('g')
                .attr('class', 'grid y')
                .call(d3.axisLeft(y)
                    .ticks(5)
                    .tickSize(-width)
                    .tickFormat(''))
                .call(g => g.select('.domain').remove())
                .call(g => g.selectAll('.tick line')
                    .attr('stroke', 'lightgrey')
                    .attr('stroke-dasharray', '2,2'));

            // Add legend to the right side of the chart
            if (showLegend) {
                console.log("Creating legend for view mode:", viewMode);
                
                const legendItemHeight = 20;
                const legendItemPadding = 5;
                const legendPadding = 10;
                const maxLegendWidth = 200; // Maximum width for the legend
                const maxLegendHeight = height; // Maximum height for the legend

                // Create a temporary SVG to measure text widths
                const tempSvg = d3.select(chartDiv).append('svg').style('visibility', 'hidden');
                const textWidths = chartData.datasets.map(d => {
                    const text = tempSvg.append('text').text(d.label);
                    const width = text.node().getComputedTextLength();
                    text.remove();
                    return width;
                });
                tempSvg.remove();

                const maxTextWidth = Math.min(d3.max(textWidths), maxLegendWidth - 40); // 40 for color indicator and padding
                const legendWidth = maxTextWidth + 40;
                const legendHeight = Math.min(maxLegendHeight, chartData.datasets.length * (legendItemHeight + legendItemPadding) + legendPadding * 2);

                const legend = svg.append('g')
                    .attr('class', 'legend')
                    .attr('transform', `translate(${width + 10}, 0)`);

                // Add a border for the legend
                legend.append('rect')
                    .attr('width', legendWidth)
                    .attr('height', legendHeight)
                    .attr('fill', 'white')
                    .attr('stroke', '#ccc')
                    .attr('stroke-width', 1)
                    .attr('rx', 5)
                    .attr('ry', 5);

                const legendContent = legend.append('g')
                    .attr('class', 'legend-content')
                    .attr('transform', `translate(${legendPadding}, ${legendPadding})`)
                    .attr('clip-path', 'url(#legend-clip)');

                // Add a clip path to prevent content from overflowing
                legend.append('clipPath')
                    .attr('id', 'legend-clip')
                    .append('rect')
                    .attr('width', legendWidth - legendPadding * 2)
                    .attr('height', legendHeight - legendPadding * 2);

                const legendItems = legendContent.selectAll('.legend-item')
                    .data(chartData.datasets)
                    .enter()
                    .append('g')
                    .attr('class', 'legend-item')
                    .attr('transform', (d, i) => `translate(0, ${i * (legendItemHeight + legendItemPadding)})`);

                legendItems.append('line')
                    .attr('x1', 0)
                    .attr('y1', legendItemHeight / 2)
                    .attr('x2', 15)
                    .attr('y2', legendItemHeight / 2)
                    .attr('stroke', d => d.color)
                    .attr('stroke-width', 2);

                legendItems.append('text')
                    .attr('x', 20)
                    .attr('y', legendItemHeight / 2)
                    .attr('dy', '.35em')
                    .style('font-size', '10px')
                    .style('fill', '#333')
                    .text(d => {
                        const maxLength = Math.floor((maxTextWidth - 20) / 6); // Approximate characters that fit
                        return d.label.length > maxLength ? d.label.substring(0, maxLength - 3) + '...' : d.label;
                    })
                    .append('title')
                    .text(d => d.label);

                // Make legend scrollable if there are too many items
                if (chartData.datasets.length * (legendItemHeight + legendItemPadding) > legendHeight - legendPadding * 2) {
                    const scrollableArea = legend.append('foreignObject')
                        .attr('x', legendPadding)
                        .attr('y', legendPadding)
                        .attr('width', legendWidth - legendPadding * 2)
                        .attr('height', legendHeight - legendPadding * 2)
                        .append('xhtml:div')
                        .style('width', '100%')
                        .style('height', '100%')
                        .style('overflow-y', 'scroll');

                    scrollableArea.node().appendChild(legendContent.node());
                }

                // Modify the click event for legend items
                legendItems.style('cursor', 'pointer')
                    .on('click', function(event, d) {
                        const index = chartData.datasets.indexOf(d);
                        const legendItem = d3.select(this);
                        
                        if (visibleDatasets.includes(index)) {
                            // Toggle off the dataset
                            visibleDatasets = visibleDatasets.filter(i => i !== index);
                            legendItem.style('opacity', 0.5);
                        } else {
                            // Toggle on the dataset
                            visibleDatasets.push(index);
                            legendItem.style('opacity', 1);
                        }
                        
                        // If all datasets are hidden, show all of them
                        if (visibleDatasets.length === 0) {
                            visibleDatasets = chartData.datasets.map((_, i) => i);
                            legend.selectAll('.legend-item').style('opacity', 1);
                        }
                        
                        updateLines();
                    });

                // Adjust SVG width to accommodate the legend
                svg.attr('width', width + margin.left + margin.right + legendWidth + 10);
            } else {
                console.log("Not creating legend: Total view, Organisation breakdown, or small screen");
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
        console.log("View mode changed to:", viewMode);
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
    <h3 class="text-lg font-semibold mb-2">Trends over time for {quantityType}</h3>
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
        <div bind:this={chartContainer} class="chart-container flex-grow" style="height: auto; min-height: 300px;">
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