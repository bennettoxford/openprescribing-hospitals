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
    const DATA_SUBMITTED_COLOR = "#005AB5";  // Blue
    const DATA_NOT_SUBMITTED_COLOR = "#DC3220";

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

    let expandedOrgs = new Set();

    function toggleExpansion(orgName) {
        const scrollPosition = window.pageYOffset;
        
        if (expandedOrgs.has(orgName)) {
            expandedOrgs.delete(orgName);
        } else {
            expandedOrgs.add(orgName);
        }
        expandedOrgs = expandedOrgs;
        
        // Use setTimeout to ensure this runs after the chart has been redrawn
        setTimeout(() => {
            createChart();
            window.scrollTo(0, scrollPosition);
        }, 0);
    }

    let sortBySubmission = false;
    let parsedOrgData = [];
    
    function toggleSort() {
        sortBySubmission = !sortBySubmission;
        organisations = flattenOrganisations(parsedOrgData);
        createChart();
    }

    function sortOrganisations(orgs) {
        if (!sortBySubmission) {
            return orgs;
        }

        return [...orgs].sort((a, b) => {
            // First compare by latest submission (non-submitting first)
            const aSubmission = a.latest_submission;
            const bSubmission = b.latest_submission;
            
            if (aSubmission !== bSubmission) {
                return aSubmission ? 1 : -1;
            }
            // Then alphabetically
            return a.name.localeCompare(b.name);
        });
    }

    function flattenOrganisations(orgs, level = 0, parentSubmission = null) {
        let flattened = [];
        // Only sort at top level
        const sortedOrgs = level === 0 ? sortOrganisations(orgs) : orgs;
        
        sortedOrgs.forEach(org => {
            // Use parent's submission status for predecessors when sorting
            const submissionStatus = level === 0 ? org.latest_submission : parentSubmission;
            flattened.push({ ...org, level, effective_submission: submissionStatus });
            
            if (org.predecessors && org.predecessors.length > 0 && expandedOrgs.has(org.name)) {
               
                flattened = flattened.concat(
                    flattenOrganisations(org.predecessors, level + 1, submissionStatus)
                );
            }
        });
        return flattened;
    }

    onMount(() => {
        console.log("Received orgdata:", orgData);
        try {
            const unescapedData = unescapeUnicode(orgData);
            parsedOrgData = JSON.parse(unescapedData);
            organisations = parsedOrgData;
            console.log("Parsed organisations:", organisations);
            
            if (organisations.length > 0) {
                months = Object.keys(organisations[0].data).sort();
            }
            console.log("Months:", months);

            setTimeout(createChart, 0);
        } catch (e) {
            error = `Error parsing JSON data: ${e.message}`;
            console.error(error);
        }
    });

    function createChart() {
        if (!chartContainer) return;

        d3.select(chartContainer).selectAll('*').remove();

        const flatOrgs = flattenOrganisations(organisations);  // Use the current organisations state

        chartWidth = chartContainer.clientWidth;
        chartHeight = Math.max(400, flatOrgs.length * 30);

        const margin = { top: 40, right: 50, bottom: 40, left: 350 };

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

                if (org.predecessors && org.predecessors.length > 0) {
                    const button = tick.append('foreignObject')
                        .attr('x', -margin.left)
                        .attr('y', -10)
                        .attr('width', 120)
                        .attr('height', 20)
                        .append('xhtml:button')
                        .attr('class', 'expand-collapse-btn')
                        .html(() => {
                            const arrow = expandedOrgs.has(org.name) ? '▲': '▼';
                            const text = expandedOrgs.has(org.name) ? 'Hide predecessors' : 'Show predecessors';
                            return `${arrow} ${text}`;
                        })
                        .on('click', (event) => {
                            event.preventDefault();
                            event.stopPropagation();
                            toggleExpansion(org.name);
                        });
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

        const getMiddleOfMonth = (date) => {
            const middleDate = new Date(date);
            middleDate.setDate(15);
            return middleDate;
        };

        const xAxis = svg.append('g')
            .attr('class', 'x-axis')
            .attr('transform', `translate(0, -10)`)
            .call(d3.axisTop(x)
                .tickValues([
                    getMiddleOfMonth(new Date(months[0])),
                    getMiddleOfMonth(new Date(months[months.length - 1]))
                ])
                .tickFormat(d3.timeFormat('%b %Y'))
                .tickSize(-12));

        xAxis.selectAll('.tick text')
            .attr('dy', '-0.5em')
            .style('font-size', '14px')
            .style('text-anchor', 'middle');

        xAxis.selectAll('.tick line')
            .attr('stroke', 'black')
            .attr('stroke-width', 2);

        xAxis.select('.domain').remove();

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
            .attr('fill', d => d.hasData ? DATA_SUBMITTED_COLOR : DATA_NOT_SUBMITTED_COLOR)
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
    <div class="flex items-center gap-4 mb-6 mr-8 justify-end">
        <button 
            class="inline-flex items-center justify-center px-4 py-2 text-sm font-medium 
                   bg-white border border-gray-300 rounded-md shadow-sm hover:bg-gray-50 
                   focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 
                   transition-all duration-200"
            on:click={toggleSort}
        >
            <svg class="mr-2 h-4 w-4 text-gray-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
            </svg>
            <span>{sortBySubmission ? 'Sort Alphabetically' : 'Sort by Latest Submission'}</span>
        </button>
    </div>
    
    {#if error}
        <p class="text-red-600">{error}</p>
    {:else if organisations.length === 0}
        <p class="text-gray-600">No data available</p>
    {:else}
        <div bind:this={chartContainer} class="relative w-full"></div>
    {/if}
</div>

<style>
    .expand-collapse-btn {
        font-size: 12px;
        padding: 2px 5px;
        background-color: #f0f0f0;
        border: 1px solid #ccc;
        border-radius: 3px;
        cursor: pointer;
        display: flex;
        align-items: center;
        gap: 4px;
    }
    .expand-collapse-btn:hover {
        background-color: #e0e0e0;
    }
    
</style>