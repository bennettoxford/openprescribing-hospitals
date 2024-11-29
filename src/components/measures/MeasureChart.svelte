<script>
  import { onMount, onDestroy } from 'svelte';
  import * as d3 from 'd3';
  import { filteredData, getOrganisationColor, selectedMode } from '../../stores/measureChartStore.js';
  import { regionColors } from '../../utils/chartConfig.js';

  let chartDiv;
  let tooltip;
  let brush;
  let xScale;
  let yScale;
  let chartHeight;
  let chartWidth;
  let margin;
  let svg;

  $: isPercentileMode = $selectedMode === 'percentiles';

  let brushing = false;

  let isZoomedIn = false;

  function updateZoomState() {
    const xDomain = xScale.domain();
    const yDomain = yScale.domain();
    const initialXDomain = d3.extent($filteredData.labels, d => new Date(d));
    const initialYDomain = [0, 100];

    isZoomedIn = xDomain[0] > initialXDomain[0] || xDomain[1] < initialXDomain[1] ||
                 yDomain[0] > initialYDomain[0] || yDomain[1] < initialYDomain[1];
  }

  function updateXAxis(selection, duration = 750) {
    // Calculate the time span in months
    const [start, end] = xScale.domain();
    const monthsDiff = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24 * 30.44);

    // Choose tick settings based on time span
    const tickSettings = monthsDiff <= 24 
      ? { interval: d3.timeMonth.every(1), format: "%b %Y" }
      : { interval: d3.timeYear.every(1), format: '%Y' };

    // Update axis with different label orientations based on time span
    selection.select('g.x-axis')
      .transition()
      .duration(duration)
      .call(d3.axisBottom(xScale)
        .ticks(tickSettings.interval)
        .tickFormat(d3.timeFormat(tickSettings.format)))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px')
        .style('text-anchor', monthsDiff <= 24 ? 'start' : 'middle')
        .attr('dx', monthsDiff <= 24 ? '0.8em' : '0')
        .attr('dy', monthsDiff <= 24 ? '-0.3em' : '0.71em')
        .attr('transform', monthsDiff <= 24 ? 'rotate(90)' : 'rotate(0)'));

    // Update grid
    selection.select('g.x-grid')
      .transition()
      .duration(duration)
      .call(d3.axisBottom(xScale)
        .ticks(tickSettings.interval)
        .tickSize(-chartHeight)
        .tickFormat(''))
      .call(g => g.select('.domain').remove())
      .call(g => g.selectAll('.tick line')
        .attr('stroke', '#e0e0e0')
        .attr('stroke-opacity', 0.7));
  }

  function updateYAxis(selection, duration = 750) {
    selection.select('g.y-axis')
      .transition()
      .duration(duration)
      .call(d3.axisLeft(yScale)
        .ticks(10)
        .tickFormat(d3.format('.1f')))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px'));

    // Update y-grid
    selection.select('g.y-grid')
      .transition()
      .duration(duration)
      .call(d3.axisLeft(yScale)
        .ticks(10)
        .tickSize(-chartWidth)
        .tickFormat(''))
      .call(g => g.select('.domain').remove())
      .call(g => g.selectAll('.tick line')
        .attr('stroke', '#e0e0e0')
        .attr('stroke-opacity', 0.7));
  }

  function createChart() {
    d3.select(chartDiv).selectAll('*').remove();

    margin = { top: 20, right: 40, bottom: 100, left: 70 };
    const width = chartDiv.clientWidth;
    const height = 400;
    chartWidth = width - margin.left - margin.right;
    chartHeight = height - margin.top - margin.bottom;

    svg = d3.select(chartDiv)
      .append('svg')
      .attr('width', width)
      .attr('height', height)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Add clipPath definition
    svg.append('defs')
      .append('clipPath')
      .attr('id', 'clip')
      .append('rect')
      .attr('width', chartWidth)
      .attr('height', chartHeight)
      .attr('x', 0)
      .attr('y', 0);

    // Create a group for the clipped content
    const chartArea = svg.append('g')
      .attr('clip-path', 'url(#clip)');

    // Create groups in specific order (bottom to top)
    const percentileAreasGroup = chartArea.append('g')
      .attr('class', 'percentile-areas-group');
    
    const brushGroup = chartArea.append('g')
      .attr('class', 'brush');

    const linesGroup = chartArea.append('g')
      .attr('class', 'lines-group');

    // Update brush pointer-events
    brushGroup.style('pointer-events', 'all');
    
    // Ensure lines have pointer-events
    linesGroup.style('pointer-events', 'all');

    xScale = d3.scaleTime()
      .domain(d3.extent($filteredData.labels, d => new Date(d)))
      .range([0, chartWidth]);

    yScale = d3.scaleLinear()
      .domain([0, 100])
      .range([chartHeight, 0]);

    // Add x-axis and grid initially
    const xAxis = svg.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0,${chartHeight})`);

    const xGrid = svg.append('g')
      .attr('class', 'x-grid')
      .attr('transform', `translate(0,${chartHeight})`);

    // Initial axis setup
    updateXAxis(svg, 0);

    // Add y-axis
    svg.append('g')
      .attr('class', 'y-axis')
      .call(d3.axisLeft(yScale)
        .ticks(10)
        .tickFormat(d3.format('.1f')))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px'));

    svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('y', 0 - margin.left)
      .attr('x', 0 - (chartHeight / 2))
      .attr('dy', '1em')
      .style('text-anchor', 'middle')
      .style('font-size', '16px')
      .text('%');

    // Add y-grid
    const yGrid = svg.append('g')
      .attr('class', 'y-grid');

    // Define the line generator function
    const line = d3.line()
      .x((d, i) => xScale(new Date($filteredData.labels[i])))
      .y(d => d !== null ? yScale(d * 100) : null)
      .defined(d => d !== null)
      .curve(d3.curveLinear);

    // Define the area generator function (for percentiles)
    const area = d3.area()
      .x((d, i) => xScale(new Date($filteredData.labels[i])))
      .y0(d => d.lower !== null ? yScale(d.lower * 100) : yScale(0))
      .y1(d => d.upper !== null ? yScale(d.upper * 100) : yScale(0))
      .defined(d => d.lower !== null && d.upper !== null)
      .curve(d3.curveLinear);

    // Modify the brush setup
    brush = d3.brush()
      .extent([[0, 0], [chartWidth, chartHeight]])
      .on('start', (event) => {
        brushing = true;
        tooltip.style('opacity', 0);
      })
      .on('brush', (event) => {
        if (!event.selection) return;
        tooltip.style('opacity', 0);
      })
      .on('end', (event) => {
        brushing = false;
        if (event.selection) {
          brushed(event);
        }
      });

    // Apply the brush to the brush group
    brushGroup.call(brush);

    function brushed(event) {
      if (!event.selection) return;
      
      const [[x0, y0], [x1, y1]] = event.selection;
      xScale.domain([xScale.invert(x0), xScale.invert(x1)]);
      yScale.domain([yScale.invert(y1), yScale.invert(y0)]);

      // Update axes with new domains
      updateXAxis(svg);
      updateYAxis(svg);

      // Update lines
      linesGroup.selectAll('.line')
        .transition()
        .duration(750)
        .attr('d', d => line(d.data));

      // Update percentile areas specifically
      if ($selectedMode === 'percentiles') {
        percentileAreasGroup.selectAll('path')
          .transition()
          .duration(750)
          .attr('d', d => {
            return d3.area()
              .x((d, i) => xScale(new Date($filteredData.labels[i])))
              .y0(d => d.lower !== null ? yScale(d.lower * 100) : yScale(0))
              .y1(d => d.upper !== null ? yScale(d.upper * 100) : yScale(0))
              .defined(d => d.lower !== null && d.upper !== null)
              .curve(d3.curveLinear)(d);
          });
      }

      // Update points positions
      linesGroup.selectAll('.data-point')
        .transition()
        .duration(750)
        .attr('cx', function(d) {
          return xScale(d.date);
        })
        .attr('cy', function(d) {
          return yScale(d.value * 100);
        });

      // Update hover circles positions
      linesGroup.selectAll('.hover-circle')
        .transition()
        .duration(750)
        .attr('cx', function(d) {
          return xScale(d.date);
        })
        .attr('cy', function(d) {
          return yScale(d.value * 100);
        });

      // Clear the brush
      brushGroup.call(brush.move, null);

      updateZoomState();
    }

    // Draw shaded areas for percentiles inside the clipped area FIRST
    if ($selectedMode === 'percentiles') {
      const percentileDatasets = $filteredData.datasets.filter(d => d.label.includes('Percentile') && d.fill);
      
      percentileDatasets.sort((a, b) => {
        const aRange = a.label.match(/\d+/g).map(Number);
        const bRange = b.label.match(/\d+/g).map(Number);
        return (bRange[1] - bRange[0]) - (aRange[1] - aRange[0]);
      });

      percentileDatasets.forEach((dataset) => {
        percentileAreasGroup.append('path')
          .datum(dataset.data)
          .attr('fill', dataset.color)
          .attr('fill-opacity', dataset.fillOpacity)
          .attr('d', area);
      });
    }

    // THEN draw lines and set up tooltips (this ensures they're on top)
    linesGroup.selectAll('.line')
      .data($filteredData.datasets.filter(d => !d.fill))
      .enter()
      .append('path')
      .attr('class', (d, i) => `line line-${i} ${d.isPercentileLine ? 'percentile-line' : ''} ${d.isTrust ? 'trust-line' : ''}`)
      .attr('d', d => line(d.data))
      .attr('fill', 'none')
      .attr('stroke', (d, i) => d.color || getOrganisationColor(i, isPercentileMode))
      .attr('stroke-width', 2.5)
      .attr('stroke-opacity', d => {
        if (d.isPercentileLine && d.label !== 'Median (50th Percentile)') {
          return 0;
        }
        return d.strokeOpacity || 1;
      })
      .attr('stroke-dasharray', d => d.strokeDasharray || 'none');

    // Add points for each data point, excluding non-median percentile lines
    linesGroup.selectAll('.line-group')
      .data($filteredData.datasets.filter(d => {
        // Include if:
        // 1. Not a fill (area)
        // 2. Either not a percentile line OR is the median OR is a trust line
        return !d.fill && (!d.isPercentileLine || d.label === 'Median (50th Percentile)' || d.isTrust);
      }))
      .enter()
      .append('g')
      .attr('class', (d, i) => `line-group group-${i}`)
      .each(function(dataset, datasetIndex) {
        const group = d3.select(this);
        
        group.selectAll('.data-point')
          .data(dataset.data.map((value, index) => ({
            value,
            date: new Date($filteredData.labels[index]),
            color: dataset.color || getOrganisationColor(dataset.index, isPercentileMode),
            datasetIndex
          })))
          .enter()
          .filter(d => d.value !== null)
          .append('g')
          .attr('class', 'point-group')
          .each(function(d) {
            const pointGroup = d3.select(this);
            const hoverRadius = 12;
            
            // Add hover circle (initially invisible)
            pointGroup.append('circle')
              .attr('class', 'hover-circle')
              .attr('cx', d => xScale(d.date))
              .attr('cy', d => yScale(d.value * 100))
              .attr('r', hoverRadius)
              .attr('fill', d => d.color)
              .attr('opacity', 0)
              .attr('pointer-events', 'none');

            // Add main point
            pointGroup.append('circle')
              .attr('class', `data-point point-${datasetIndex}`)
              .attr('cx', d => xScale(d.date))
              .attr('cy', d => yScale(d.value * 100))
              .attr('r', 3)
              .attr('fill', d => d.color)
              .on('mousemove', function(event, d) {
                const point = d3.select(this);
                const cx = +point.attr('cx');
                const cy = +point.attr('cy');
                const mouseX = d3.pointer(event)[0];
                const mouseY = d3.pointer(event)[1];
                
                const distance = Math.sqrt(
                  Math.pow(mouseX - cx, 2) + 
                  Math.pow(mouseY - cy, 2)
                );
                
                if (distance <= hoverRadius) {
                  // Dim all lines and points
                  chartArea.selectAll('.line').style('opacity', 0.2);
                  chartArea.selectAll('.data-point').style('opacity', 0.2);
                  
                  // Highlight current line and its points
                  chartArea.select(`.line-${datasetIndex}`).style('opacity', 1);
                  chartArea.selectAll(`.point-${datasetIndex}`).style('opacity', 1);
                  
                  d3.select(this.parentNode)
                    .select('.hover-circle')
                    .transition()
                    .duration(200)
                    .attr('opacity', 0.2);
                  moveTooltip(event, dataset);
                  tooltip.style('opacity', 1);
                } else {
                  // Reset all lines and points to full opacity
                  chartArea.selectAll('.line').style('opacity', 1);
                  chartArea.selectAll('.data-point').style('opacity', 1);
                  
                  d3.select(this.parentNode)
                    .select('.hover-circle')
                    .transition()
                    .duration(200)
                    .attr('opacity', 0);
                  hideTooltip();
                }
              })
              .on('mouseout', function(event, d) {
                // Reset all lines and points to full opacity
                chartArea.selectAll('.line').style('opacity', 1);
                chartArea.selectAll('.data-point').style('opacity', 1);
                
                d3.select(this.parentNode)
                  .select('.hover-circle')
                  .transition()
                  .duration(200)
                  .attr('opacity', 0);
                hideTooltip();
              });
          });
      });

    // Add class names to axes for easier selection
    svg.select('g.grid').attr('class', 'grid x');
    svg.selectAll('g.axis').attr('class', g => `axis ${g.attr('class')}`);

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
      if (brushing) return;
      d3.select(this).attr('stroke-width', 3);
      tooltip.style('opacity', 1);
      chartArea.selectAll('.line').style('opacity', 0.2);
      d3.select(this).style('opacity', 1);

      let displayName, displayCode;
      if (d.isTrust) {
        [displayCode, displayName] = d.label.split('|');
      } else {
        displayName = d.label;
        displayCode = '';
      }

      const tooltipX = event.pageX + 10;
      const tooltipY = event.pageY - 10;

      const xPos = d3.pointer(event, this)[0];
      const date = xScale.invert(xPos);
      const bisectDate = d3.bisector(d => new Date(d)).left;
      const index = bisectDate($filteredData.labels, date);
      const value = d.data[index];
      const numerator = d.numerator ? d.numerator[index] : null;
      const denominator = d.denominator ? d.denominator[index] : null;

      tooltip
        .html(`<strong>${displayName || 'Unknown'}</strong>${displayCode ? `<br>ODS Code: ${displayCode.trim()}` : ''}<br>Date: ${
        d3.timeFormat('%b %Y')(date)}<br>Percentage: ${value !== null ? (value * 100).toFixed(1) : 'N/A'}%${
        numerator !== null && denominator !== null ? `<br>Numerator: ${numerator.toFixed(2)}<br>Denominator: ${denominator.toFixed(2)}` : ''}${
        d.org_count ? `<br>Organisations included: ${d.org_count}` : ''
        }`)
        .style('left', `${tooltipX}px`)
        .style('top', `${tooltipY}px`);
    }

    function moveTooltip(event, d) {
      if (brushing) return;
      const [xPos] = d3.pointer(event, this);
      const date = xScale.invert(xPos);

      const bisectDate = d3.bisector(d => new Date(d)).left;
      const index = bisectDate($filteredData.labels, date);
      const nearestIndex = index > 0 && (
        index === $filteredData.labels.length || 
        (date - new Date($filteredData.labels[index - 1])) < (new Date($filteredData.labels[index]) - date)
      ) ? index - 1 : index;
      const nearestDate = new Date($filteredData.labels[nearestIndex]);
      const nearestValue = d.data[nearestIndex];
      const numerator = d.numerator ? d.numerator[nearestIndex] : null;
      const denominator = d.denominator ? d.denominator[nearestIndex] : null;

      let displayName, displayCode;
      if ($selectedMode === 'organisation') {
        [displayCode, displayName] = d.label.split('|');
      } else if ($selectedMode === 'national') {
        displayName = 'National';
        displayCode = '';
      } else {
        displayName = d.label;
        displayCode = '';
      }

      const tooltipX = event.pageX + 10;
      const tooltipY = event.pageY - 10;

      tooltip
        .html(`<strong>${displayName || 'Unknown'}</strong>${displayCode ? `<br>ODS Code: ${displayCode.trim()}` : ''}<br>Date: ${
        d3.timeFormat('%b %Y')(nearestDate)}<br>Percentage: ${nearestValue !== null ? (nearestValue * 100).toFixed(1) : 'N/A'}%${
        numerator !== null && denominator !== null ? `<br>Numerator: ${numerator.toFixed(2)}<br>Denominator: ${denominator.toFixed(2)}` : ''}${
        d.org_count ? `<br>Organisations included: ${d.org_count}` : ''
        }`)
        .style('left', `${tooltipX}px`)
        .style('top', `${tooltipY}px`);
    }

    function hideTooltip() {
      if (brushing) return;
      d3.select(this).attr('stroke-width', 2.5);
      tooltip.style('opacity', 0);
      chartArea.selectAll('.line').style('opacity', 1);
    }

    // Add mousedown event to start brushing
    brushGroup.on('mousedown', () => {
      brushing = true;
      tooltip.style('opacity', 0); // Hide tooltip when starting to brush
    });

    // Add mouseup event to stop brushing
    d3.select('body').on('mouseup', () => {
      brushing = false;
    });
  }

  function handleResize() {
    if (chartDiv && $filteredData?.labels?.length > 0) {
      createChart();
    }
  }

  function resizeAction(node) {
    const resizeObserver = new ResizeObserver(() => {
      handleResize();
    });

    resizeObserver.observe(node);

    return {
      destroy() {
        resizeObserver.disconnect();
      }
    };
  }

  onMount(() => {
    if ($filteredData?.labels?.length > 0) {
      createChart();
    }
  });

  onDestroy(() => {
    if (tooltip) {
      tooltip.remove();
    }
  });

  $: if ($filteredData?.labels?.length > 0) {
    handleResize();
  }

  // Modify resetZoom function to reset both x and y scales
  function resetZoom() {
    if (!xScale || !yScale || !svg) return;
    
    xScale.domain(d3.extent($filteredData.labels, d => new Date(d)));
    yScale.domain([0, 100]);
    
    // Update axes with full domains
    updateXAxis(svg);
    updateYAxis(svg);

    // Define line generator
    const line = d3.line()
      .x((d, i) => xScale(new Date($filteredData.labels[i])))
      .y(d => d !== null ? yScale(d * 100) : null)
      .defined(d => d !== null)
      .curve(d3.curveLinear);

    // Update lines
    svg.select('g[clip-path]')
      .select('.lines-group')
      .selectAll('.line')
      .transition()
      .duration(750)
      .attr('d', d => line(d.data));

    // Update areas if in percentile mode
    if ($selectedMode === 'percentiles') {
      svg.select('g[clip-path]')
        .select('.percentile-areas-group')
        .selectAll('path')
        .transition()
        .duration(750)
        .attr('d', d => {
          return d3.area()
            .x((d, i) => xScale(new Date($filteredData.labels[i])))
            .y0(d => d.lower !== null ? yScale(d.lower * 100) : yScale(0))
            .y1(d => d.upper !== null ? yScale(d.upper * 100) : yScale(0))
            .defined(d => d.lower !== null && d.upper !== null)
            .curve(d3.curveLinear)
            (d);
        });
    }

    // Update data points positions
    svg.select('g[clip-path]')
      .select('.lines-group')
      .selectAll('.data-point')
      .transition()
      .duration(750)
      .attr('cx', d => xScale(d.date))
      .attr('cy', d => yScale(d.value * 100));

    // Update hover circles positions
    svg.select('g[clip-path]')
      .select('.lines-group')
      .selectAll('.hover-circle')
      .transition()
      .duration(750)
      .attr('cx', d => xScale(d.date))
      .attr('cy', d => yScale(d.value * 100));

    updateZoomState();
  }
</script>

<style>
  .tooltip {
    transition: opacity 0.2s ease-in-out;
  }

  .line, .data-point {
    transition: opacity 0.2s ease-in-out;
  }

  circle {
    transition: r 0.2s ease-in-out;
  }

  .percentile-line {
    pointer-events: all;
    stroke-opacity: 0;
  }

  .brush {
    pointer-events: all;
  }

  .brush .overlay {
    fill: none;
    pointer-events: all;
  }

  .lines-group {
    pointer-events: all;
  }

  .line {
    pointer-events: all;
    transition: stroke-width 0.2s ease-in-out;
  }

  .zoom-overlay {
    cursor: default;
  }

  .zoom-overlay:active {
    cursor: crosshair;
  }

  .data-point {
    transition: r 0.2s ease-in-out;
  }

  .hover-circle {
    pointer-events: none;
  }

</style>

<div class="relative w-full h-[450px]">
  <div bind:this={chartDiv} use:resizeAction class="w-full h-[450px]"></div>
  {#if isZoomedIn}
    <button 
      class="absolute top-2 right-2 px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 rounded-md shadow-sm"
      on:click={resetZoom}
    >
      Reset Zoom
    </button>
  {/if}
</div>
