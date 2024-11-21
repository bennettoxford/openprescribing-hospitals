<svelte:options customElement={{
    tag: 'org-submission',
    shadow: 'none',
    props: {
        orgdata: { type: 'String' },
        latestDates: { type: 'String' }
    }
  }} />

<script>
    import { onMount, afterUpdate, onDestroy } from 'svelte';
    import * as d3 from 'd3';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';

    export let orgData = '{}';
    export let latestDates = '{}';

    let organisations = [];
    let searchableOrgs = [];

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

    let sortType = 'missing_latest'; // Default sort

    
    function calculateMissingDataProportion(org) {
        // Get all months' VMP counts, including predecessors, but only for submitted months
        const monthlyTotals = months.map(month => {
            let total = 0;
            let hasSubmitted = org.data[month]?.has_submitted;
            
            if (hasSubmitted) {
                total = org.data[month]?.vmp_count || 0;
                
                // Add predecessor VMP counts if they exist
                if (org.predecessors) {
                    org.predecessors.forEach(pred => {
                        if (pred.data[month]?.has_submitted) {
                            total += pred.data[month]?.vmp_count || 0;
                        }
                    });
                }
            }
            return { total, hasSubmitted };
        });
        
        // Filter to only submitted months
        const submittedData = monthlyTotals.filter(m => m.hasSubmitted);
        
        // Find max VMP count from submitted months for scaling
        const maxVmpCount = Math.max(...submittedData.map(m => m.total));
        
        // Scale the submitted totals between 0 and 1
        const scaledTotals = submittedData.map(m => ({
            ...m,
            scaledTotal: maxVmpCount > 0 ? m.total / maxVmpCount : 0
        }));
        
        // Calculate median from scaled totals
        const sortedScaledTotals = [...scaledTotals].sort((a, b) => a.scaledTotal - b.scaledTotal);
        const medianScaled = sortedScaledTotals[Math.floor(sortedScaledTotals.length / 2)].scaledTotal;
        
        // Calculate average absolute deviation from the median
        return scaledTotals.reduce((sum, { scaledTotal }) => {
            return sum + Math.abs(scaledTotal - medianScaled);
        }, 0) / scaledTotals.length;
    }

    function calculateMissingMonths(org) {
        return months.reduce((count, month) => {
            return count + (org.data[month]?.has_submitted ? 0 : 1);
        }, 0);
    }

    let parsedOrgData = [];
    
    function toggleSort() {
        sortType = sortType === 'missing_latest' ? 'missing_proportion' : sortType === 'missing_proportion' ? 'alphabetical' : 'missing_latest';
        createChart();
    }

    let searchTerm = '';
    let filteredOrganisations = [];

    function filterOrganisations(orgs, searchTerms) {
        if (!searchTerms || searchTerms.length === 0) return orgs;
        
        // Only filter at the top level
        return orgs.filter(org => {
            const matchesOrg = searchTerms.some(term => 
                org.name.toLowerCase().includes(term.toLowerCase())
            );
            
            const matchesPredecessor = org.predecessors?.some(pred => 
                searchTerms.some(term => 
                    pred.name.toLowerCase().includes(term.toLowerCase())
                )
            );
            
            return matchesOrg || matchesPredecessor;
        });
    }

    $: {
        if (parsedOrgData.length > 0) {
            searchableOrgs = prepareOrganisationsForSearch(parsedOrgData);
            organisationSearchStore.setItems(searchableOrgs);
        }
    }

    function prepareOrganisationsForSearch(orgs) {
        let allOrgs = [];
        
        function collectOrgs(org) {
            allOrgs.push(org.name);
            if (org.predecessors) {
                org.predecessors.forEach(pred => {
                    collectOrgs(pred);
                });
            }
        }
        
        orgs.forEach(org => collectOrgs(org));
        return [...new Set(allOrgs)]; // Remove duplicates
    }

    let parsedLatestDates = {};

    onMount(() => {
        try {
            const unescapedData = unescapeUnicode(orgData);
            parsedOrgData = JSON.parse(unescapedData);
            parsedLatestDates = JSON.parse(unescapeUnicode(latestDates));
            organisations = parsedOrgData;
            filteredOrganisations = organisations;
            
            if (organisations.length > 0) {
                months = Object.keys(organisations[0].data).sort();
            }

            setTimeout(createChart, 0);
        } catch (e) {
            error = `Error parsing JSON data: ${e.message}`;
            console.error(error);
        }
    });

    function flattenOrganisations(orgs, level = 0) {
        let flattened = [];
        
        let organisationsToProcess = [...orgs];
        if (level === 0) {
            organisationsToProcess.sort((a, b) => {
                switch (sortType) {
                    case 'missing_months':
                        const aMissing = calculateMissingMonths(a);
                        const bMissing = calculateMissingMonths(b);
                        if (aMissing !== bMissing) {
                            return bMissing - aMissing; // More missing months first
                        }
                        break;
                        
                    case 'missing_latest':
                        const latestMonth = months[months.length - 1];
                        const aSubmitted = a.data[latestMonth]?.has_submitted;
                        const bSubmitted = b.data[latestMonth]?.has_submitted;
                        if (aSubmitted !== bSubmitted) {
                            return aSubmitted ? 1 : -1;
                        }
                        break;
                        
                    case 'missing_proportion':
                        const aProportion = calculateMissingDataProportion(a);
                        const bProportion = calculateMissingDataProportion(b);
                        if (aProportion !== bProportion) {
                            return bProportion - aProportion; // Larger proportions first
                        }
                        break;
                        
                    case 'alphabetical':
                        return a.name.localeCompare(b.name);
                }
                // Default to alphabetical as fallback
                return a.name.localeCompare(b.name);
            });
        }
        
        // Process each organisation and its predecessors
        for (const org of organisationsToProcess) {
            // Add the current organisation
            flattened.push({ ...org, level });
            
            // If expanded, immediately add predecessors without sorting
            if (expandedOrgs.has(org.name) && org.predecessors?.length > 0) {
                org.predecessors.forEach(pred => {
                    flattened.push({ ...pred, level: level + 1 });
                });
            }
        }
        
        return flattened;
    }

    function createChart() {
        if (!chartContainer) return;

        d3.select(chartContainer).selectAll('*').remove();

        const flatOrgs = flattenOrganisations(filteredOrganisations);

        chartWidth = chartContainer.clientWidth;
        const margin = { top: 70, right: 70, bottom: 40, left: 350 };
        
        chartHeight = flatOrgs.length * 30 + margin.top + margin.bottom;

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

        const dateLineGroup = svg.append('g')
            .attr('class', 'date-lines');

        function parseDate(dateStr) {
            if (!dateStr) return null;
            const [month, year] = dateStr.split(' ');
            return new Date(`${month} 1, ${year}`);
        }

        // Add vertical lines for each date type
        const dateTypes = {
            final: { color: '#117733', label: 'finalised' },
            wip: { color: '#332288', label: 'WIP' },
        };

        Object.entries(parsedLatestDates).forEach(([type, dateStr], index) => {
            if (!dateStr) return;
            
            const date = parseDate(dateStr);
            if (!date) return;

            const xPos = x(date);
            const style = dateTypes[type];
            const labelOffset = -25 - (index * 30);

            dateLineGroup.append('line')
                .attr('x1', xPos)
                .attr('y1', labelOffset + 25)
                .attr('x2', xPos)
                .attr('y2', height)
                .attr('stroke', style.color)
                .attr('stroke-width', 2.5)
                .attr('stroke-dasharray', '4,4');

            const label = dateLineGroup.append('text')
                .attr('x', xPos)
                .attr('y', labelOffset)
                .attr('text-anchor', 'middle')
                .attr('fill', style.color)
                .attr('font-size', '14px')
                .style('width', '80px');

            label.append('tspan')
                .attr('x', xPos)
                .text('Latest');

            label.append('tspan')
                .attr('x', xPos)
                .attr('dy', '1.2em')
                .text(`${style.label} data`);
        });

    }

    function drawOrgData(svg, org, x, y, width) {
        // Calculate the maximum VMP count for this org
        const maxVmpCount = Math.max(...Object.values(org.data).map(d => d.vmp_count));
        
        // Create a scale for the bar heights
        const heightScale = d3.scaleLinear()
            .domain([0, maxVmpCount])
            .range([0, y.bandwidth()]);

        const orgData = months.map(month => ({
            date: new Date(month),
            hasData: org.data[month].has_submitted,
            vmpCount: org.data[month].vmp_count
        }));

        const sanitizedOrgName = sanitizeClassName(org.name);

        // First, create the visible data rectangles
        svg.selectAll(`.org-${sanitizedOrgName}-visible`)
            .data(orgData)
            .enter()
            .append('rect')
            .attr('class', `org-${sanitizedOrgName}-visible`)
            .attr('x', d => x(d.date))
            .attr('y', d => d.hasData 
                ? y(org.name) + y.bandwidth() - heightScale(d.vmpCount)
                : y(org.name)
            )
            .attr('width', width / (months.length))
            .attr('height', d => d.hasData 
                ? heightScale(d.vmpCount)
                : y.bandwidth()
            )
            .attr('fill', d => d.hasData ? DATA_SUBMITTED_COLOR : DATA_NOT_SUBMITTED_COLOR)
            .attr('opacity', org.level === 0 ? 1 : 0.7);

        // Then, create invisible rectangles for better hover detection
        svg.selectAll(`.org-${sanitizedOrgName}-hover`)
            .data(orgData)
            .enter()
            .append('rect')
            .attr('class', `org-${sanitizedOrgName}-hover`)
            .attr('x', d => x(d.date))
            .attr('y', d => y(org.name))
            .attr('width', width / (months.length))
            .attr('height', y.bandwidth())
            .attr('fill', 'transparent')
            .attr('pointer-events', 'all')
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
            <strong>Number of products:</strong> ${d.vmpCount}<br>
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

    function handleSearchSelect(event) {
        const { selectedItems } = event.detail;
        organisationSearchStore.updateSelection(selectedItems);
        
        if (selectedItems.length === 0) {
            filteredOrganisations = organisations;
        } else {
            filteredOrganisations = organisations.filter(org => {
                // Check if this org is directly selected
                if (selectedItems.includes(org.name)) {
                    return true;
                }
                
                // Check if any of this org's predecessors are selected
                const hasPredecessorSelected = org.predecessors?.some(pred => 
                    selectedItems.includes(pred.name)
                );
                
                if (hasPredecessorSelected) {
                    expandedOrgs.add(org.name);
                    return true;
                }
                
                return false;
            });
        }
        createChart();
    }

    $: {
        filteredOrganisations = filterOrganisations(organisations, searchTerm);
    }

    let isOrganisationDropdownOpen = false;

    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
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
    <div class="w-full max-w-[600px] relative z-50 mb-4">
        {#if searchableOrgs.length > 0}
            <OrganisationSearch 
                source={organisationSearchStore}
                overlayMode={true}
                on:selectionChange={handleSearchSelect}
                on:dropdownToggle={handleOrganisationDropdownToggle}
            />
        {:else}
            <div class="text-sm text-gray-500">Loading organisations...</div>
        {/if}
    </div>

    <div class="flex items-center mb-2 mr-8 justify-end">
        <select 
            bind:value={sortType}
            class="inline-flex items-center justify-center px-4 py-2 text-sm font-medium 
                   bg-white border border-gray-300 rounded-md shadow-sm hover:bg-gray-50 
                   focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 
                   transition-all duration-200 h-[38px]"
            on:change={() => createChart()}
        >
            <option value="missing_latest">Sort by submission status in latest month</option>
            <option value="missing_months">Sort by number of months with no data submission</option>
            <option value="missing_proportion">Sort by deviation from the median number of products</option>
            <option value="alphabetical">Sort alphabetically</option>
        </select>
    </div>
    
    {#if error}
        <p class="text-red-600">{error}</p>
    {:else if organisations.length === 0}
        <p class="text-gray-600">No data available</p>
    {:else}
        <div bind:this={chartContainer} class="relative w-full min-h-[300px]"></div>
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