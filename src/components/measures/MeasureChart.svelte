<script>
  import { chartOptions, regionColors } from '../../utils/chartConfig.js';
  import { onMount, afterUpdate, onDestroy } from 'svelte';
  import * as d3 from 'd3';
  import { selectedMode, filteredData } from '../../stores/measureChartStore.js';

  export let width;
  export let height;

  let chartDiv;
  let tooltip;

  $: {
    console.log('Width:', width, 'Height:', height);
    console.log('Filtered Data:', $filteredData);
    if (width && height && $filteredData && $filteredData.labels && $filteredData.labels.length > 0) {
      updateChart();
    } else {
      console.log('Skipping chart update due to missing data or dimensions');
    }
  }

  function updateChart() {
    if (!chartDiv || !width || !height || !$filteredData || !$filteredData.labels || $filteredData.labels.length === 0) {
      console.log('Skipping chart update due to missing data or dimensions');
      return;
    }

    console.log('Updating chart with data:', $filteredData);

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
      .domain(d3.extent($filteredData.labels, d => new Date(d)))
      .range([0, chartWidth]);

    const yMax = d3.max($filteredData.datasets, d => d3.max(d.data, v => v !== null ? v : -Infinity));

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
      .attr('stroke-opacity', 0.5)
      .attr('stroke-dasharray', '2,2');

    svg.append('g')
      .attr('class', 'grid')
      .call(d3.axisLeft(y)
        .tickSize(-chartWidth)
        .tickFormat(''))
      .selectAll('line')
      .attr('stroke', 'lightgrey')
      .attr('stroke-opacity', 0.5)
      .attr('stroke-dasharray', '2,2');

    svg.selectAll('.grid path')
      .style('display', 'none');

    // Add x-axis
    svg.append('g')
      .attr('transform', `translate(0,${chartHeight})`)
      .call(d3.axisBottom(x)
        .ticks(d3.timeYear.every(1))
        .tickFormat(d3.timeFormat('%Y')))
      .selectAll('text')
      .style('font-size', '12px');

    // Add y-axis
    svg.append('g')
      .call(d3.axisLeft(y))
      .selectAll('text')
      .style('font-size', '12px');

    // Add x-axis label
    svg.append('text')
      .attr('class', 'x-axis-label')
      .attr('text-anchor', 'middle')
      .attr('x', chartWidth / 2)
      .attr('y', chartHeight + margin.bottom - 5)
      .style('font-size', '14px')
      .text('Date');

    // Add y-axis label
    svg.append('text')
      .attr('class', 'y-axis-label')
      .attr('text-anchor', 'middle')
      .attr('transform', `rotate(-90)`)
      .attr('x', -chartHeight / 2)
      .attr('y', -margin.left + 15)
      .style('font-size', '14px')
      .text('Proportion');

    // Draw lines
    const line = d3.line()
      .x((d, i) => x(new Date($filteredData.labels[i])))
      .y(d => y(d))
      .defined(d => d !== null && !isNaN(d));

    svg.selectAll('.line')
      .data($filteredData.datasets)
      .enter()
      .append('path')
      .attr('class', 'line')
      .attr('d', d => line(d.data))
      .attr('fill', 'none')
      .attr('stroke', d => d.color || d3.schemeCategory10[i % 10])
      .attr('stroke-width', d => d.strokeWidth || 2)
      .attr('stroke-dasharray', d => d.strokeDasharray || 'none');

    tooltip = d3.select('body')
      .append('div')
      .attr('class', 'tooltip')
      .style('position', 'absolute')
      .style('pointer-events', 'none')
      .style('opacity', 0)
      .style('background-color', 'rgba(255, 255, 255, 0.9)')
      .style('border', '1px solid #ccc')
      .style('border-radius', '4px')
      .style('padding', '8px')
      .style('font-size', '12px')
      .style('box-shadow', '0 2px 4px rgba(0,0,0,0.1)');

    function showTooltip(event, d) {
      d3.select(this).attr('stroke-width', d.strokeWidth + 1);
      tooltip.style('opacity', 1);
      svg.selectAll('.line').style('opacity', 0.2);
      d3.select(this).style('opacity', 1);
    }

    function moveTooltip(event, d) {
      const [xPos, yPos] = d3.pointer(event);
      const date = x.invert(xPos);

      const bisectDate = d3.bisector(d => new Date(d)).left;
      const index = bisectDate($filteredData.labels, date);
      const nearestIndex = index > 0 && (index === $filteredData.labels.length || (date - new Date($filteredData.labels[index - 1])) < (new Date($filteredData.labels[index]) - date)) ? index - 1 : index;
      const nearestDate = new Date($filteredData.labels[nearestIndex]);
      const nearestValue = d.data[nearestIndex];

      const orgName = d.label ? (d.label.length > 20 ? `${d.label.substring(0, 20)}...` : d.label) : 'Unknown';

      const tooltipX = event.pageX + 10;
      const tooltipY = event.pageY - 10;

      tooltip
        .html(`<strong>${orgName}</strong><br>Date: ${d3.timeFormat('%b %Y')(nearestDate)}<br>Proportion: ${nearestValue?.toFixed(2) || 'N/A'}`)
        .style('left', `${tooltipX}px`)
        .style('top', `${tooltipY}px`);
    }

    function hideTooltip(event, d) {
      d3.select(this).attr('stroke-width', d.strokeWidth);
      tooltip.style('opacity', 0);
      svg.selectAll('.line').style('opacity', 1);
    }

    svg.selectAll('.line')
      .on('mouseover', showTooltip)
      .on('mousemove', moveTooltip)
      .on('mouseout', hideTooltip)
      .on('click', function(event, d) {
        showTooltip.call(this, event, d);
        moveTooltip.call(this, event, d);
      });
  }

  onMount(() => {
    if (width && height) {
      updateChart();
    }
  });

  afterUpdate(() => {
    if (width && height) {
      updateChart();
    }
  });

  onDestroy(() => {
    if (tooltip) {
      tooltip.remove();
    }
  });
</script>

<style>
  .tooltip {
    transition: opacity 0.2s ease-in-out;
  }
</style>

<div bind:this={chartDiv} class="w-full h-full"></div>