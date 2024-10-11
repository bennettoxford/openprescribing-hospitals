<script>
  import { chartOptions, regionColors } from '../../utils/chartConfig.js';
  import { onMount, afterUpdate, onDestroy } from 'svelte';
  import * as d3 from 'd3';

  export let chartData;
  export let selectedMode;
  export let width;
  export let height;

  let chartDiv;
  let tooltip;

  $: if (width && height && chartData && chartData.labels && chartData.datasets) {
    updateChart();
  }

  function updateChart() {
    if (!chartDiv || !width || !height || !chartData || !chartData.labels || !chartData.datasets) return;

    const { margin } = chartOptions;
    const chartWidth = width - margin.left - margin.right;
    const chartHeight = height - margin.top - margin.bottom;

    d3.select(chartDiv).selectAll('*').remove();

    const svg = d3.select(chartDiv)
      .append('svg')
      .attr('width', width)
      .attr('height', height)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleTime()
      .domain(d3.extent(chartData.labels, d => new Date(d)))
      .range([0, chartWidth]);

    const yMax = selectedMode === 'deciles'
      ? 1
      : Math.max(1, d3.max(chartData.datasets, d => d3.max(d.data.map(value => Math.max(value, 0)))));

    const y = d3.scaleLinear()
      .domain([0, yMax]).nice()
      .range([chartHeight, 0]);

    // Add grid lines
    svg.append('g')
      .attr('class', 'grid')
      .attr('transform', `translate(0,${chartHeight})`)
      .call(d3.axisBottom(x)
        .ticks(d3.timeYear.every(1))
        .tickSize(-chartHeight)
        .tickFormat(''))
      .selectAll('line')
      .attr('stroke', 'lightgrey')
      .attr('stroke-dasharray', '2,2');

    svg.append('g')
      .attr('class', 'grid')
      .call(d3.axisLeft(y)
        .tickSize(-chartWidth)
        .tickFormat(''))
      .selectAll('line')
      .attr('stroke', 'lightgrey')
      .attr('stroke-dasharray', '2,2');

    svg.selectAll('.grid path')
      .style('display', 'none');

    // Add axes
    svg.append('g')
      .attr('transform', `translate(0,${chartHeight})`)
      .call(d3.axisBottom(x)
        .ticks(d3.timeYear.every(1))
        .tickFormat(d3.timeFormat('%Y')))
      .selectAll('text')
      .style('font-size', '12px');

    svg.append('g')
      .call(d3.axisLeft(y))
      .selectAll('text')
      .style('font-size', '12px');

    // Add axis labels
    svg.append('text')
      .attr('transform', `translate(${chartWidth / 2},${chartHeight + margin.bottom - 10})`)
      .style('text-anchor', 'middle')
      .text('Date');

    svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('y', 0 - margin.left)
      .attr('x', 0 - (chartHeight / 2))
      .attr('dy', '1em')
      .style('text-anchor', 'middle')
      .text('Proportion');

    // Draw lines
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

    // Remove or comment out this block to remove labels
    /*
    if (selectedMode === 'organisation') {
      lines.append('text')
        .datum(d => ({ name: d.label, value: d.data[d.data.length - 1] }))
        .attr('transform', d => `translate(${x(new Date(chartData.labels[chartData.labels.length - 1]))},${y(Math.max(d.value, 0))})`)
        .attr('x', 3)
        .attr('dy', '0.35em')
        .style('font', '10px sans-serif')
        .text(d => d.name);
    }
    */

    // Tooltip
    tooltip = d3.select('body')
      .append('div')
      .attr('class', chartOptions.tooltipClass)
      .style('position', 'absolute')
      .style('pointer-events', 'none')
      .style('opacity', 0);

    function showTooltip(event, d) {
      d3.select(this).attr('stroke-width', d.strokeWidth + 1);
      tooltip.style('opacity', 1);
      svg.selectAll('.line path').style('opacity', 0.2);
      d3.select(this).style('opacity', 1);
    }

    function moveTooltip(event, d) {
      const [xPos, yPos] = d3.pointer(event);
      const date = x.invert(xPos);
      const tooltipWidth = tooltip.node()?.offsetWidth || 0;
      const tooltipHeight = tooltip.node()?.offsetHeight || 0;

      const bisectDate = d3.bisector(d => new Date(d)).left;
      const index = bisectDate(chartData.labels, date);
      const nearestIndex = index > 0 && (index === chartData.labels.length || (date - new Date(chartData.labels[index - 1])) < (new Date(chartData.labels[index]) - date)) ? index - 1 : index;
      const nearestDate = new Date(chartData.labels[nearestIndex]);
      const nearestValue = d.data[nearestIndex];

      // Check if d.label exists before trying to access its length
      const orgName = d.label ? (d.label.length > 20 ? `${d.label.substring(0, 20)}...` : d.label) : 'Unknown';

      const leftPosition = xPos < chartWidth / 2 ? event.clientX + 10 : event.clientX - tooltipWidth - 10;
      const topPosition = yPos < chartHeight / 2 ? event.clientY : event.clientY - tooltipHeight;

      tooltip
        .html(`<strong>Org:</strong> ${orgName}<br><strong>Date:</strong> ${d3.timeFormat('%b %Y')(nearestDate)}<br><strong>Proportion:</strong> ${nearestValue?.toFixed(2) || 'N/A'}`)
        .style('left', `${Math.max(0, Math.min(leftPosition, window.innerWidth - tooltipWidth))}px`)
        .style('top', `${Math.max(0, Math.min(topPosition, window.innerHeight - tooltipHeight))}px`);
    }

    function hideTooltip(event, d) {
      d3.select(this).attr('stroke-width', d.strokeWidth);
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

  onMount(() => {
    updateChart();
  });

  afterUpdate(() => {
    updateChart();
  });

  onDestroy(() => {
    if (tooltip) {
      tooltip.remove();
    }
  });
</script>

<div bind:this={chartDiv} class="w-full h-full"></div>