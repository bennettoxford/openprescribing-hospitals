<svelte:options customElement={{
    tag: 'measure-component',
    shadow: 'none'
}} />

<script>
    import { onMount, afterUpdate, onDestroy } from 'svelte';
    import * as d3 from 'd3';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';

    export let measureData = '[]';
    export let deciles = '{}';

    let parsedData = [];
    let chartDiv;
    let chartContainer;
    let resizeObserver;
    let organisations = [];
    let selectedOrganisations = [];
    let usedOrganisationSelection = false;
    let tooltip;
    let selectedMode = 'organisation';
    let modeSelectWidth = 'auto';
    let regions = [];
    let legendItems = [];

    const regionColors = {
        'East Of England': '#1f77b4',
        'South East': '#ff7f0e',
        'Midlands': '#2ca02c',
        'North East And Yorkshire': '#d62728',
        'London': '#9467bd',
        'South West': '#8c564b',
        'North West': '#e377c2'
    };

    $: {
        try {
            if (typeof measureData === 'string') {
                let unescapedData = measureData.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                parsedData = JSON.parse(unescapedData);
            } else {
                parsedData = measureData;
            }
            organisations = [...new Set(parsedData.map(item => item.organisation))];
            if (chartDiv) {
                updateChart();
            }
        } catch (e) {
            console.error('Error parsing measureData:', e);
            parsedData = [];
        }

        try {
            if (typeof deciles === 'string') {
                let unescapedDeciles = deciles.replace(/\\u([\d\w]{4})/gi, (match, grp) => String.fromCharCode(parseInt(grp, 16)));
                deciles = JSON.parse(unescapedDeciles);
            } 
        } catch (e) {
            console.error('Error parsing deciles:', e);
            deciles = {};
        }

    
        regions = [...new Set(parsedData.map(item => item.region))];

        if (selectedMode === 'deciles') {
            legendItems = [
                { label: '1st-9th Percentile', style: 'border-blue-500 border-dotted' },
                { label: '10th-90th Percentile', style: 'border-blue-500 border-dashed' },
                { label: '50th Percentile', style: 'border-red-500 border-dashed' }
            ];
        } else if (selectedMode === 'region') {
            legendItems = Object.entries(regionColors).map(([region, color]) => ({
                label: region,
                style: `background-color: ${color}`
            }));
        } else {
            legendItems = [];
        }
    }

    function debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    function prepareChartData(data) {

        if (data.length === 0 && selectedMode === 'organisation') {
            return { labels: [], datasets: [] };
        }

        let filteredData;
        let breakdownKeys;

        if (selectedMode === 'organisation') {
            if (usedOrganisationSelection) {
                if (selectedOrganisations.length > 0) {
                    filteredData = data.filter(item => selectedOrganisations.includes(item.organisation));
                    breakdownKeys = selectedOrganisations;
                } else {
                    return { labels: [], datasets: [] };
                }
            } else {
                filteredData = data;
                breakdownKeys = organisations;
            }

            const groupedData = filteredData.reduce((acc, item) => {
                const key = item.month;
                if (!acc[key]) {
                    acc[key] = {};
                }
                const breakdownKey = item.organisation;
                if (!acc[key][breakdownKey]) {
                    acc[key][breakdownKey] = 0;
                }
                acc[key][breakdownKey] += parseFloat(item.quantity);
                return acc;
            }, {});


            const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));

            return {
                labels: sortedDates,
                datasets: breakdownKeys.map((key, index) => {
                    const dataPoints = sortedDates.map(date => {
                        if (!groupedData[date]) {
                            console.warn(`No data for date: ${date}`);
                            return 0;
                        }
                        if (!(key in groupedData[date])) {
                            console.warn(`No data for organisation ${key} on date ${date}`);
                            return 0;
                        }
                        return groupedData[date][key];
                    });
                    return {
                        label: key,
                        data: dataPoints,
                        color: `hsl(${index * 360 / breakdownKeys.length}, 70%, 50%)`,
                        strokeWidth: 2
                    };
                })
            };
        } else if (selectedMode === 'deciles') {
            const sortedDates = Object.keys(deciles).sort((a, b) => new Date(a) - new Date(b));
    
            const decileDatasets = Array.from({ length: 27 }, (_, i) => {
                let label;
                let color;
                let strokeWidth;
                let strokeDasharray;
                if (i < 9) {
                    label = `${i + 1}th Percentile`;
                    color = 'blue';
                    strokeWidth = 1;
                    strokeDasharray = '2,2'; // Dotted blue
                } else if (i < 17) {
                    label = `${(i - 9 + 1) * 10}th Percentile`;
                    color = 'blue';
                    strokeWidth = 2;
                    strokeDasharray = '4,2'; // Dashed blue
                } else if (i === 17) { // 50th percentile
                    label = '50th Percentile';
                    color = 'red';
                    strokeWidth = 4;
                    strokeDasharray = '4,2'; // Dashed red
                } else {
                    label = `${i - 17 + 90}th Percentile`;
                    color = 'blue';
                    strokeWidth = 1;
                    strokeDasharray = '2,2'; // Dotted blue
                }

                return {
                    label: label,
                    data: sortedDates.map(date => deciles[date][i] || 0),
                    color: color,
                    strokeWidth: strokeWidth,
                    strokeDasharray: strokeDasharray
                };
            });

            if (usedOrganisationSelection && selectedOrganisations.length > 0) {
                const filteredData = data.filter(item => selectedOrganisations.includes(item.organisation));
                const groupedData = filteredData.reduce((acc, item) => {
                    const key = item.month;
                    if (!acc[key]) {
                        acc[key] = {};
                    }
                    const breakdownKey = item.organisation;
                    if (!acc[key][breakdownKey]) {
                        acc[key][breakdownKey] = 0;
                    }
                    acc[key][breakdownKey] += parseFloat(item.quantity);
                    return acc;
                }, {});

                const organisationDatasets = selectedOrganisations.map((key, index) => ({
                    label: key,
                    data: sortedDates.map(date => groupedData[date][key] || 0),
                    color: `hsl(${index * 360 / selectedOrganisations.length}, 70%, 50%)`,
                    strokeWidth: 3 // Thicker lines for selected organisations
                }));

                return {
                    labels: sortedDates,
                    datasets: [...decileDatasets, ...organisationDatasets]
                };
            }

            return {
                labels: sortedDates,
                datasets: decileDatasets
            };
        } else if (selectedMode === 'region') {
            filteredData = data;
            breakdownKeys = Object.keys(regionColors);

            const groupedData = filteredData.reduce((acc, item) => {
                const key = item.month;
                if (!acc[key]) {
                    acc[key] = {};
                }
                const breakdownKey = item.region;
                if (!acc[key][breakdownKey]) {
                    acc[key][breakdownKey] = { sum: 0, count: 0 };
                }
                acc[key][breakdownKey].sum += parseFloat(item.quantity);
                acc[key][breakdownKey].count += 1;
                return acc;
            }, {});

            const sortedDates = Object.keys(groupedData).sort((a, b) => new Date(a) - new Date(b));

            return {
                labels: sortedDates,
                datasets: breakdownKeys.map(key => {
                    const dataPoints = sortedDates.map(date => {
                        if (!groupedData[date] || !groupedData[date][key]) {
                            return 0;
                        }
                        const { sum, count } = groupedData[date][key];
                        return count > 0 ? sum / count : 0;
                    });
                    return {
                        label: key,
                        data: dataPoints,
                        color: regionColors[key],
                        strokeWidth: 2
                    };
                })
            };
        }
    }

    function updateChart() {
        if (chartDiv && chartContainer) {
            const chartData = prepareChartData(parsedData);
            const containerWidth = chartContainer.clientWidth;
            const containerHeight = chartContainer.clientHeight;

            const margin = { top: 20, right: 30, bottom: 50, left: 50 };
            const width = containerWidth - margin.left - margin.right;
            const height = containerHeight - margin.top - margin.bottom;

            d3.select(chartDiv).selectAll('*').remove();

            const svg = d3.select(chartDiv)
                .append('svg')
                .attr('width', containerWidth)
                .attr('height', containerHeight)
                .append('g')
                .attr('transform', `translate(${margin.left},${margin.top})`);

            const x = d3.scaleTime()
                .domain(d3.extent(chartData.labels, d => new Date(d)))
                .range([0, width]);

            const yMax = selectedMode === 'deciles'
                ? 1
                : Math.max(1, d3.max(chartData.datasets, d => d3.max(d.data.map(value => Math.max(value, 0)))));

            const y = d3.scaleLinear()
                .domain([0, yMax]).nice()
                .range([height, 0]);

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

            svg.append('g')
                .attr('transform', `translate(0,${height})`)
                .call(d3.axisBottom(x)
                    .ticks(d3.timeYear.every(1))
                    .tickFormat(d3.timeFormat('%Y')))
                .selectAll('text')
                .style('font-size', '12px');

            svg.append('g')
                .call(d3.axisLeft(y))
                .selectAll('text')
                .style('font-size', '12px');

            // X-axis label
            svg.append('text')
                .attr('transform', `translate(${width / 2},${height + margin.bottom - 10})`)
                .style('text-anchor', 'middle')
                .text('Date');

            // Y-axis label
            svg.append('text')
                .attr('transform', 'rotate(-90)')
                .attr('y', 0 - margin.left)
                .attr('x', 0 - (height / 2))
                .attr('dy', '1em')
                .style('text-anchor', 'middle')
                .text('Proportion');

            const line = d3.line()
                .x(d => x(new Date(d.date)))
                .y(d => y(Math.max(d.value, 0)));

            const lines = svg.selectAll('.line')
                .data(chartData.datasets)
                .enter()
                .append('g')
                .attr('class', 'line');

            lines.append('path')
                .attr('fill', 'none')
                .attr('stroke', d => d.color)
                .attr('stroke-width', d => d.strokeWidth)
                .attr('stroke-dasharray', d => d.strokeDasharray)
                .attr('d', d => line(d.data.map((value, i) => ({ date: chartData.labels[i], value }))));

            if (selectedMode === 'organisation') {
                lines.append('text')
                    .datum(d => ({ name: d.label, value: d.data[d.data.length - 1] }))
                    .attr('transform', d => `translate(${x(new Date(chartData.labels[chartData.labels.length - 1]))},${y(Math.max(d.value, 0))})`)
                    .attr('x', 3)
                    .attr('dy', '0.35em')
                    .style('font', '10px sans-serif');
            }

            // Tooltip
            tooltip = d3.select('body')
                .append('div')
                .attr('class', 'tooltip p-2 bg-gray-800 text-white rounded shadow-lg text-sm')
                .style('position', 'absolute')
                .style('pointer-events', 'none')
                .style('opacity', 0);

            function showTooltip(event, d) {
                d3.select(this).attr('stroke-width', d.strokeWidth + 1); // Increase stroke-width by 1 on hover
                tooltip.style('opacity', 1);
                svg.selectAll('.line path').style('opacity', 0.2);
                d3.select(this).style('opacity', 1);
            }

            function moveTooltip(event, d) {
                const [xPos, yPos] = d3.pointer(event);
                const date = x.invert(xPos);
                const tooltipWidth = tooltip.node().offsetWidth;
                const tooltipHeight = tooltip.node().offsetHeight;

                const bisectDate = d3.bisector(d => new Date(d)).left;
                const index = bisectDate(chartData.labels, date);
                const nearestIndex = index > 0 && (index === chartData.labels.length || (date - new Date(chartData.labels[index - 1])) < (new Date(chartData.labels[index]) - date)) ? index - 1 : index;
                const nearestDate = new Date(chartData.labels[nearestIndex]);
                const nearestValue = d.data[nearestIndex];

                const orgName = d.label.length > 20 ? `${d.label.substring(0, 20)}...` : d.label;

                const leftPosition = xPos < width / 2 ? event.clientX + 10 : event.clientX - tooltipWidth - 10;
                const topPosition = yPos < height / 2 ? event.clientY : event.clientY - tooltipHeight;

                tooltip
                    .html(`<strong>Org:</strong> ${orgName}<br><strong>Date:</strong> ${d3.timeFormat('%b %Y')(nearestDate)}<br><strong>Proportion:</strong> ${nearestValue.toFixed(2)}`)
                    .style('left', `${Math.max(0, Math.min(leftPosition, window.innerWidth - tooltipWidth))}px`)
                    .style('top', `${Math.max(0, Math.min(topPosition, window.innerHeight - tooltipHeight))}px`);
            }

            function hideTooltip(event, d) {
                d3.select(this).attr('stroke-width', d.strokeWidth); // Reset stroke-width to original
                tooltip.style('opacity', 0);
                svg.selectAll('.line path').style('opacity', 1);
            }

            svg.selectAll('.line path')
                .on('mouseover', showTooltip)
                .on('mousemove', moveTooltip)
                .on('mouseout', hideTooltip)
                .on('click', function(event, d) {
                    showTooltip.call(this, event, d);
                    moveTooltip.call(this, event, d);
                });
        }
    }

    const debouncedUpdateChart = debounce(updateChart, 250);

    function handleOrganisationSelection(event) {
        selectedOrganisations = event.detail.selectedItems;
        usedOrganisationSelection = event.detail.usedOrganisationSelection;
        updateChart();
    }

    function handleModeChange(event) {
        selectedMode = event.target.value;
        updateChart();
    }

    function handleResize() {
        updateChart();
    }

    function adjustModeSelectWidth() {
        const select = document.getElementById('mode-select');
        if (select) {
            const tempSpan = document.createElement('span');
            tempSpan.style.visibility = 'hidden';
            tempSpan.style.position = 'absolute';
            tempSpan.style.whiteSpace = 'nowrap';
            tempSpan.innerHTML = select.options[select.selectedIndex].text;
            document.body.appendChild(tempSpan);
            const width = tempSpan.offsetWidth;
            document.body.removeChild(tempSpan);
            modeSelectWidth = `${width + 40}px`; // Add some padding
        }
    }

    $: selectedMode, adjustModeSelectWidth();

    onMount(() => {
        updateChart();
        resizeObserver = new ResizeObserver(handleResize);
        if (chartContainer) {
            resizeObserver.observe(chartContainer);
        }
        adjustModeSelectWidth();
    });

    onDestroy(() => {
        if (resizeObserver) {
            resizeObserver.disconnect();
        }
    });

    afterUpdate(() => {
        if (chartDiv) {
            debouncedUpdateChart();
        }
    });
</script>

<div class="flex flex-col">
    <div class="flex flex-wrap justify-between items-end mb-4">
        <div class="flex-grow mr-4 relative">
            <OrganisationSearch 
                items={organisations} 
                on:selectionChange={handleOrganisationSelection}
                overlayMode={true}
            />
        </div>
        <div class="flex-shrink-0 mr-8">
            <label for="mode-select" class="block text-sm font-medium text-gray-700 mb-1">Select Mode</label>
            <select 
                id="mode-select" 
                class="p-2 border border-gray-300 rounded-md bg-white" 
                on:change={handleModeChange}
                style="width: {modeSelectWidth};"
                bind:value={selectedMode}
            >
                <option value="organisation">Organisation</option>
                <option value="deciles">Deciles</option>
                <option value="region">Region</option>
            </select>
        </div>
    </div>

    <div class="flex-grow relative" style="min-height: 400px;">
        <div bind:this={chartContainer} class="chart-container absolute inset-0">
            {#if parsedData.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <div bind:this={chartDiv} class="w-full h-full"></div>
            {/if}
        </div>
    </div>

    {#if legendItems.length > 0}
    <div class="flex flex-wrap justify-center mt-4">
        {#each legendItems as item}
            <div class="flex items-center mr-4 mb-2">
                {#if selectedMode === 'deciles'}
                    <div class="w-8 h-0.5 border-t {item.style} mr-2"></div>
                {:else}
                    <div class="w-4 h-4 mr-2" style="{item.style}"></div>
                {/if}
                <span class="text-sm">{item.label}</span>
            </div>
        {/each}
    </div>
    {/if}
</div>


