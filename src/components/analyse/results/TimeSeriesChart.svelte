<svelte:options customElement={{
    tag: 'time-series-chart',
    shadow: 'none'
  }} />

<script>
    import { onMount, afterUpdate, onDestroy } from 'svelte';
    import * as d3 from 'd3';
    import { resultsStore } from '../../../stores/resultsStore';

    export let data = [];
    export let quantityType = 'VMP Quantity';
    export let searchType = 'vmp';

    // Add reactive variables from resultsStore
    $: currentSearchType = $resultsStore.searchType;
    $: currentQuantityType = $resultsStore.quantityType;

    let chartDiv;
    let viewMode = 'Total';
    let availableViewModes = ['Total'];
    let organisations = [];
    let units = [];
    let ingredientUnitPairs = [];
    let vtms = [];
    let routes = [];
    let tooltip;
    let resizeTimer;
    let visibleDatasets = [];

    let chartContainer;
    let isSmallScreen = false;

    $: showNoDataMessage = data.length === 0;

    $: {
        console.log('Data received in TimeSeriesChart:', data);
        if (data.length > 0) {
            updateAvailableViewModes();
            if (!availableViewModes.includes(viewMode)) {
                viewMode = availableViewModes[0];
            }
            organisations = [...new Set(data.map(item => item.organisation__name))];
            units = [...new Set(data.flatMap(item => 
                item.data.map(d => d[2])
            ))];
            vtms = [...new Set(data.map(item => item.vmp__vtm__name || 'Unknown Product Group'))];
            routes = [...new Set(data.flatMap(item => 
                item.routes || []
            ).filter(route => route))];
            if (chartDiv) {
                updateChart();
            }
        } else {
            console.log('No data available for chart');
            if (chartDiv) {
                clearChart();
            }
        }
    }

    function clearChart() {
        if (chartDiv) {
            d3.select(chartDiv).selectAll('*').remove();
            
            // Add a message when no products are selected
            if (showNoDataMessage) {
                const width = chartContainer.clientWidth;
                const height = Math.min(400, window.innerHeight * 0.6);
                
                const svg = d3.select(chartDiv)
                    .append('svg')
                    .attr('width', width)
                    .attr('height', height);
                
                svg.append('text')
                    .attr('x', width / 2)
                    .attr('y', height / 2)
                    .attr('text-anchor', 'middle')
                    .attr('class', 'text-gray-500')
                    .text('No products selected - select products from the list above to view data');
            }
        }
    }

    function updateAvailableViewModes() {
        availableViewModes = ['Total', 'Product', 'Product Group'];
        
        // Only add Organisation mode if there is more than one organisation
        const uniqueOrgs = new Set(data.map(item => item.organisation__ods_code).filter(Boolean));
        if (uniqueOrgs.size > 1) {
            availableViewModes.push('Organisation');
        }
        
        // Match ProductList.svelte conditions for ingredients
        if (currentQuantityType === 'Ingredient Quantity' || currentSearchType === 'ingredient') {
            availableViewModes.push('Ingredient');
        } else {
            if (searchType === 'vmp' || searchType === 'vtm') {
                availableViewModes.push('Unit');
            }
            if (searchType === 'vtm') {
                availableViewModes.push('VTM');
            }
        }
        availableViewModes.push('Route');
    }

    function getIngredientName(item) {
        if (item.ingredient_name) {
            return item.ingredient_name;
        } else if (Array.isArray(item.ingredient_names) && item.ingredient_names.length > 0) {
            return item.ingredient_names[0];
        } else {
            return 'Unknown';
        }
    }

    function getBreakdownKey(item, viewMode) {
        switch (viewMode) {
            case 'Product':
                return item.vmp__name || 'Unknown Product';
            case 'Organisation':
                return item.organisation__ods_name || 'Unknown Organisation';
            case 'Unit':
                return item.data?.[0]?.[2] || 'Unknown Unit';
            case 'Ingredient':
                if (item.ingredients && item.ingredients.length > 0) {
                    return item.ingredients[0];
                }
                return 'Unknown Ingredient';
            case 'Product Group':
                return item.vmp__vtm__name || 'Unknown Product Group';
            case 'Route':
                return item.routes?.[0] || 'Other';
            default:
                return 'Total';
        }
    }

    function prepareChartData(data, viewMode) {
        console.log('Preparing chart data. View mode:', viewMode);
        
        if (data.length === 0) {
            return { labels: [], datasets: [] };
        }

        // Extract all unique dates from the nested data arrays
        const allDates = [...new Set(data.flatMap(item => 
            item.data.map(([date]) => date)
        ))].sort();

        // For organisation view, first group by organisation
        if (viewMode === 'Organisation') {
            const orgData = {};
            data.forEach(item => {
                const orgName = item.organisation__ods_name || 'Unknown Organisation';
                
                if (!orgData[orgName]) {
                    orgData[orgName] = {};
                }
                item.data.forEach(([date, quantity]) => {
                    if (!orgData[orgName][date]) {
                        orgData[orgName][date] = 0;
                    }
                    orgData[orgName][date] += parseFloat(quantity || 0);
                });
            });

            console.log('Organisations found:', Object.keys(orgData));
            
            const datasets = Object.entries(orgData).map(([org, dates], index) => ({
                label: org,
                data: allDates.map(date => dates[date] || 0),
                color: `hsl(${index * 360 / Object.keys(orgData).length}, 70%, 50%)`
            }));

            return {
                labels: allDates,
                datasets
            };
        }

        // For other views, use the existing grouping logic
        const groupedData = data.reduce((acc, item) => {
            const key = getBreakdownKey(item, viewMode);
            if (!acc[key]) {
                acc[key] = {};
            }
            
            item.data.forEach(([date, quantity]) => {
                if (viewMode === 'Route' && item.routes?.length) {
                    const routes = item.routes;
                    
                    // Skip if routes is empty
                    if (routes.length === 0) {
                        return;
                    }

                    const quantityPerRoute = parseFloat(quantity) / routes.length;
                    
                    routes.forEach(route => {
                        if (!acc[route]) acc[route] = {};
                        acc[route][date] = (acc[route][date] || 0) + quantityPerRoute;
                    });
                } else if (viewMode === 'Ingredient' && item.ingredients?.length) {
                    // Handle multiple ingredients similar to routes
                    const ingredients = item.ingredients;
                    const quantityPerIngredient = parseFloat(quantity) / ingredients.length;
                    
                    ingredients.forEach(ingredient => {
                        if (!acc[ingredient]) acc[ingredient] = {};
                        acc[ingredient][date] = (acc[ingredient][date] || 0) + quantityPerIngredient;
                    });
                } else {
                    acc[key][date] = (acc[key][date] || 0) + parseFloat(quantity || 0);
                }
            });
            return acc;
        }, {});

        if (viewMode === 'Total') {
            return {
                labels: allDates,
                datasets: [{
                    label: `Total ${quantityType} over time`,
                    data: allDates.map(date => 
                        Object.values(groupedData).reduce((sum, group) => 
                            sum + (group[date] || 0), 0
                        )
                    ),
                    color: 'rgb(75, 192, 192)'
                }]
            };
        } else {
            const breakdownKeys = Object.keys(groupedData);
            const datasets = breakdownKeys.map((key, index) => ({
                label: key,
                data: allDates.map(date => groupedData[key][date] || 0),
                color: `hsl(${index * 360 / breakdownKeys.length}, 70%, 50%)`
            }));
            return { labels: allDates, datasets };
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
            
            isSmallScreen = window.innerWidth < 768;
            const dateRange = d3.extent(chartData.labels, d => new Date(d));
            const yearDiff = (dateRange[1] - dateRange[0]) / (1000 * 60 * 60 * 24 * 365);

            const showLegend = viewMode !== 'Total' && viewMode !== 'Organisation' && !isSmallScreen;
            const legendWidth = showLegend ? 150 : 0;

            const margin = { 
                top: 20, 
                right: 20 + (showLegend ? 150 : 0), 
                bottom: yearDiff < 2 ? 120 : 50, // Increased bottom margin for monthly ticks
                left: 60 
            };

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
                .domain(dateRange)
                .range([0, width]);

            const y = d3.scaleLinear()
                .domain([0, d3.max(chartData.datasets, d => d3.max(d.data))]).nice()
                .range([height, 0]);

            // Modify x-axis ticks based on date range
            const xAxis = d3.axisBottom(x)
                .ticks(yearDiff < 2 ? d3.timeMonth.every(1) : d3.timeYear.every(1))
                .tickFormat(yearDiff < 2 ? d3.timeFormat('%b %Y') : d3.timeFormat('%Y'));

            // Create grid lines
            svg.append('g')
                .attr('class', 'grid')
                .attr('transform', `translate(0,${height})`)
                .call(d3.axisBottom(x)
                    .ticks(yearDiff < 2 ? d3.timeMonth.every(1) : d3.timeYear.every(1))
                    .tickSize(-height)
                    .tickFormat(''))
                .selectAll('line')
                .attr('stroke', 'lightgrey')
                .attr('stroke-dasharray', '2,2');

            svg.append('g')
                .attr('class', 'x-axis')
                .attr('transform', `translate(0,${height})`)
                .call(xAxis)
                .selectAll('text')
                .style('font-size', '12px')
                .style('text-anchor', yearDiff < 2 ? 'end' : 'middle')
                .attr('transform', yearDiff < 2 ? 'rotate(-45)' : null)
                .attr('dx', yearDiff < 2 ? '-0.8em' : null)
                .attr('dy', yearDiff < 2 ? '0.5em' : '0.7em');

            svg.append('text')
                .attr('transform', `translate(${width / 2},${height + (yearDiff < 2 ? 90 : 40)})`)
                .style('text-anchor', 'middle')
                .text('Date');

            // Add y-axis label
            svg.append('text')
                .attr('transform', 'rotate(-90)')
                .attr('y', -margin.left + 20)
                .attr('x', -(height / 2))
                .style('text-anchor', 'middle')
                .text(quantityType);

            // Add y-axis
            svg.append('g')
                .attr('class', 'y-axis')
                .call(d3.axisLeft(y)
                    .ticks(5)
                    .tickFormat(d3.format('.2s')))
                .selectAll('text')
                .style('font-size', '12px');

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
                const maxLegendWidth = 250; // Increased from 200
                const maxLegendHeight = height;

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
                    .attr('transform', (d, i) => `translate(0, ${i * (legendItemHeight * 1.5 + legendItemPadding)})`);

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
                    .each(function(d) {
                        const text = d3.select(this);
                        const words = d.label.split(/\s+/);
                        let line = '';
                        const lineHeight = 1.1; // ems
                        const y = text.attr('y');
                        const dy = parseFloat(text.attr('dy'));
                        let tspan = text.text(null).append('tspan').attr('x', 20).attr('y', y).attr('dy', dy + 'em');

                        for (let i = 0; i < words.length; i++) {
                            const testLine = line + words[i] + ' ';
                            const testWidth = this.getComputedTextLength();
                            if (testWidth > maxTextWidth && i > 0) {
                                tspan.text(line);
                                line = words[i] + ' ';
                                tspan = text.append('tspan').attr('x', 20).attr('y', y).attr('dy', `${++lineNumber * lineHeight + dy}em`).text(words[i]);
                            } else {
                                line = testLine;
                            }
                        }
                        tspan.text(line);
                    });

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

{#if showNoDataMessage}
    <div class="p-4">
        <h3 class="text-xl font-semibold mb-4">Time Series Analysis</h3>
        <div class="bg-gray-50 rounded-lg p-6">
            <div bind:this={chartContainer}>
                <div bind:this={chartDiv}></div>
            </div>
        </div>
    </div>
{:else if data.length > 0}
    <div class="p-4">
        <h3 class="text-xl font-semibold mb-4">Time Series Analysis</h3>
        <div class="space-y-4">
            <div class="flex flex-wrap gap-2">
                {#each availableViewModes as mode}
                    <button
                        class="px-3 py-1 rounded-full text-sm font-medium transition-colors"
                        class:bg-oxford-600={viewMode === mode}
                        class:text-white={viewMode === mode}
                        class:bg-gray-200={viewMode !== mode}
                        class:text-gray-700={viewMode !== mode}
                        on:click={() => viewMode = mode}
                    >
                        {mode}
                    </button>
                {/each}
            </div>
            <div class="bg-gray-50 rounded-lg p-6">
                <div bind:this={chartContainer}>
                    <div bind:this={chartDiv}></div>
                </div>
            </div>
        </div>
    </div>
{/if}

<style>
    .chart-container {
        position: relative;
        width: 100%;
    }
</style>
