<svelte:options customElement={{
    tag: 'measure-component',
    shadow: 'none'
}} />

<script>
    import { onMount, afterUpdate, onDestroy } from 'svelte';
    import * as d3 from 'd3';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';

    export let measureData = '[]';

    let parsedData = [];
    let chartDiv;
    let organisations = [];
    let selectedOrganisations = [];
    let usedOrganisationSelection = false;
    let tooltip;

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
    }

    function prepareChartData(data) {
        if (data.length === 0) {
            return { labels: [], datasets: [] };
        }

        let filteredData;
        let breakdownKeys;

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
            datasets: breakdownKeys.map((key, index) => ({
                label: key,
                data: sortedDates.map(date => groupedData[date][key] || 0),
                color: `hsl(${index * 360 / breakdownKeys.length}, 70%, 50%)`
            }))
        };
    }

    function updateChart() {
        if (chartDiv) {
            const chartData = prepareChartData(parsedData);
            const margin = { top: 20, right: 30, bottom: 50, left: 50 };
            const width = chartDiv.clientWidth - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;

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
                .domain([0, Math.max(1, d3.max(chartData.datasets, d => d3.max(d.data.map(value => Math.max(value, 0)))))])
                .nice()
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
                .attr('stroke-width', 2)
                .attr('d', d => line(d.data.map((value, i) => ({ date: chartData.labels[i], value }))));

            lines.append('text')
                .datum(d => ({ name: d.label, value: d.data[d.data.length - 1] }))
                .attr('transform', d => `translate(${x(new Date(chartData.labels[chartData.labels.length - 1]))},${y(Math.max(d.value, 0))})`)
                .attr('x', 3)
                .attr('dy', '0.35em')
                .style('font', '10px sans-serif');

            // Tooltip
            tooltip = d3.select(chartDiv)
                .append('div')
                .attr('class', 'tooltip p-2 bg-gray-800 text-white rounded shadow-lg text-sm')
                .style('position', 'absolute')
                .style('pointer-events', 'none')
                .style('opacity', 0);

            function showTooltip(event, d) {
                d3.select(this).attr('stroke-width', 3);
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
                    .style('left', `${leftPosition}px`)
                    .style('top', `${topPosition}px`);
            }

            function hideTooltip(event, d) {
                d3.select(this).attr('stroke-width', 2);
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

    function handleOrganisationSelection(event) {
        selectedOrganisations = event.detail.selectedItems;
        usedOrganisationSelection = event.detail.usedOrganisationSelection;
        updateChart();
    }

    onMount(() => {
        updateChart();

        const handleResize = () => {
            updateChart();
        };

        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
            if (chartDiv) {
                d3.select(chartDiv).selectAll('*').remove();
            }
        };
    });

    afterUpdate(() => {
        if (chartDiv) {
            updateChart();
        }
    });
</script>

<div class="flex flex-col">
    <div class="mb-4">
        <OrganisationSearch items={organisations} on:selectionChange={handleOrganisationSelection} />
    </div>
    
    <div class="flex">
        <div class="chart-container flex-grow" style="height: 400px;">
            {#if parsedData.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <div bind:this={chartDiv}></div>
            {/if}
        </div>
    </div>
</div>

