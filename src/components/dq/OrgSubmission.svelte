<svelte:options customElement={{
    tag: 'org-submission',
    shadow: 'none',
    props: {
        orgdata: { type: 'String' }
    }
  }} />

<script>
    import { onMount, afterUpdate, onDestroy } from 'svelte';
    import * as d3 from 'd3';

    export let orgData = '{}';

    let organisations = [];
    let months = [];
    let error = null;
    let chartContainer;
    let chartWidth;
    let chartHeight;

    let tooltip;
    let tooltipContent = '';

    const CURRENT_ORG_COLOR = "#2c3e50";
    const PREDECESSOR_ORG_COLOR = "#7f8c8d";
    const CURRENT_ORG_FONT_SIZE = "12px";
    const PREDECESSOR_ORG_FONT_SIZE = "10px";

    function unescapeUnicode(str) {
        return str.replace(/\\u([a-fA-F0-9]{4})/g, function(g, m1) {
            return String.fromCharCode(parseInt(m1, 16));
        });
    }

    let resizeTimer;

    function handleResize() {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => {
            createChart();
        }, 250);
    }

    function sanitizeClassName(name) {
        return name.replace(/[^a-z0-9]/gi, '-').toLowerCase();
    }

    function flattenOrganisations(orgs, level = 0) {
        let flattened = [];
        orgs.forEach(org => {
            flattened.push({ ...org, level });
            if (org.predecessors && org.predecessors.length > 0) {
                flattened = flattened.concat(flattenOrganisations(org.predecessors, level + 1));
            }
        });
        return flattened;
    }

    onMount(() => {
        console.log("Received orgdata:", orgData);
        try {
            const unescapedData = unescapeUnicode(orgData);
            organisations = JSON.parse(unescapedData);
            console.log("Parsed organisations:", organisations);
            
            if (organisations.length > 0) {
                months = Object.keys(organisations[0].data).sort();
            }
            console.log("Months:", months);
        } catch (e) {
            error = `Error parsing JSON data: ${e.message}`;
            console.error(error);
        }

        if (!error && organisations.length > 0) {
            setTimeout(createChart, 0);
        }
    });

    function createChart() {
        if (!chartContainer) return;

        d3.select(chartContainer).selectAll('*').remove();

        const flatOrgs = flattenOrganisations(organisations);

        chartWidth = chartContainer.clientWidth;
        chartHeight = Math.max(400, flatOrgs.length * 30);

        const margin = { top: 20, right: 30, bottom: 40, left: 350 };
        const width = chartWidth - margin.left - margin.right;
        const height = chartHeight - margin.top - margin.bottom;

        const svg = d3.select(chartContainer)
            .append('svg')
            .attr('width', chartWidth)
            .attr('height', chartHeight)
            .append('g')
            .attr('transform', `translate(${margin.left},${margin.top})`);

        // Add a new group for the separator lines
        const separatorGroup = svg.append('g')
            .attr('class', 'separators');

        const x = d3.scaleTime()
            .domain(d3.extent(months, d => new Date(d)))
            .range([0, width]);

        const y = d3.scaleBand()
            .domain(flatOrgs.map(org => org.name))
            .range([0, height])
            .padding(0.1);

        const yAxis = svg.append('g')
            .call(d3.axisLeft(y));

        let lastGroupEndIndex = -1;

        yAxis.selectAll('.tick')
            .each(function(d, i) {
                const tick = d3.select(this);
                const text = tick.select('text');
                const org = flatOrgs.find(org => org.name === d);
                
                text.attr('dy', '0.3em')
                    .style('text-anchor', 'end')
                    .style('fill', org.level === 0 ? CURRENT_ORG_COLOR : PREDECESSOR_ORG_COLOR)
                    .style('font-size', org.level === 0 ? CURRENT_ORG_FONT_SIZE : PREDECESSOR_ORG_FONT_SIZE)
                    .text(d => d.length > 40 ? d.substring(0, 37) + '...' : d);  // Truncate long names
                
                if (org && org.level > 0) {
                    text.attr('transform', `translate(${-20 * org.level},0)`);
                }

                // If this is a top-level org and not the first one, add a line above it
                if (org.level === 0 && lastGroupEndIndex !== -1) {
                    separatorGroup.append('line')
                        .attr('x1', -margin.left + 10)
                        .attr('y1', y(flatOrgs[i].name))
                        .attr('x2', width)
                        .attr('y2', y(flatOrgs[i].name))
                        .attr('stroke', '#e0e0e0')
                        .attr('stroke-width', 1);
                }

                // If this is a top-level org, update the lastGroupEndIndex
                if (org.level === 0) {
                    lastGroupEndIndex = i;
                }
            });

       
        yAxis.selectAll('.tick text')
            .attr('x', -10);

        flatOrgs.forEach((org, i) => {
            drawOrgData(svg, org, x, y, width);
        });

        tooltip = d3.select(chartContainer)
            .append("div")
            .attr("class", "absolute pointer-events-none opacity-0 bg-gray-800 text-white p-2 rounded shadow-lg text-sm z-10")
            .style("transition", "opacity 0.2s");

        console.log("Chart created with tooltips");
    }

    function drawOrgData(svg, org, x, y, width) {
        const orgData = months.map(month => ({
            date: new Date(month),
            hasData: org.data[month]
        }));

        const sanitizedOrgName = sanitizeClassName(org.name);

        svg.selectAll(`.org-${sanitizedOrgName}`)
            .data(orgData)
            .enter()
            .append('rect')
            .attr('class', `org-${sanitizedOrgName}`)
            .attr('x', d => x(d.date))
            .attr('y', y(org.name))
            .attr('width', width / (months.length))
            .attr('height', y.bandwidth())
            .attr('fill', d => d.hasData ? '#4CAF50' : '#FF5252')
            .attr('opacity', org.level === 0 ? 1 : 0.7)  // Slightly reduce opacity for predecessors
            .on("mouseover", function(event, d) {
                tooltip.style("opacity", 1);
                updateTooltipContent(event, d, org);
            })
            .on("mousemove", function(event, d) {
                updateTooltipContent(event, d, org);
            })
            .on("mouseout", function() {
                tooltip.style("opacity", 0);
            });
    }

    function updateTooltipContent(event, d, org) {
        const xPos = event.offsetX + 10;
        const yPos = event.offsetY - 10;

        tooltipContent = `
            <strong>Organisation:</strong> ${org.name}<br>
            <strong>Date:</strong> ${formatDate(d.date)}<br>
            <strong>Submitted:</strong> ${d.hasData ? 'Yes' : 'No'}<br>
            ${org.level > 0 ? '<strong>Predecessor Organisation</strong>' : ''}
            ${org.predecessors && org.predecessors.length > 0 ? '<strong>Has Predecessors</strong>' : ''}
        `;

        tooltip.html(tooltipContent)
            .style("left", `${xPos}px`)
            .style("top", `${yPos}px`)
            .style("transform", "translate(-50%, -100%)");
    }

    function formatDate(date) {
        return date.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
    }

    onMount(() => {
        createChart();
        window.addEventListener('resize', handleResize);
    });

    onDestroy(() => {
        window.removeEventListener('resize', handleResize);
    });

    afterUpdate(() => {
        createChart();
    });
</script>

<div class="flex flex-col w-full">
    {#if error}
        <p class="text-red-600">{error}</p>
    {:else if organisations.length === 0}
        <p class="text-gray-600">No data available</p>
    {:else}
        <div bind:this={chartContainer} class="relative w-full"></div>
    {/if}
</div>