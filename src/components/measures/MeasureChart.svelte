<script>
  import { onMount, onDestroy } from 'svelte';
  import * as d3 from 'd3';
  import { filteredData, getOrganisationColor, selectedMode } from '../../stores/measureChartStore.js';

  let chartDiv;
  let tooltip;

  function createChart() {
    d3.select(chartDiv).selectAll('*').remove();

    const margin = { top: 20, right: 20, bottom: 40, left: 70 };
    const width = chartDiv.clientWidth;
    const height = 400;
    const chartWidth = width - margin.left - margin.right;
    const chartHeight = height - margin.top - margin.bottom;

    const svg = d3.select(chartDiv)
      .append('svg')
      .attr('width', width)
      .attr('height', height)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleTime()
      .domain(d3.extent($filteredData.labels, d => new Date(d)))
      .range([0, chartWidth]);

    const y = d3.scaleLinear()
      .domain([0, 100])
      .range([chartHeight, 0]);

    // Add grid
    svg.append('g')
      .attr('class', 'grid')
      .attr('transform', `translate(0,${chartHeight})`)
      .call(d3.axisBottom(x)
        .ticks(d3.timeYear.every(1))
        .tickSize(-chartHeight)
        .tickFormat(''))
      .call(g => g.select('.domain').remove())
      .call(g => g.selectAll('.tick line')
        .attr('stroke', '#e0e0e0')
        .attr('stroke-opacity', 0.7));

    svg.append('g')
      .attr('class', 'grid')
      .call(d3.axisLeft(y)
        .tickSize(-chartWidth)
        .tickFormat(''))
      .call(g => g.select('.domain').remove())
      .call(g => g.selectAll('.tick line')
        .attr('stroke', '#e0e0e0')
        .attr('stroke-opacity', 0.7));

    svg.append('g')
      .attr('transform', `translate(0,${chartHeight})`)
      .call(d3.axisBottom(x).ticks(d3.timeYear.every(1)).tickFormat(d3.timeFormat('%Y')))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px'));

    // Update y-axis
    svg.append('g')
      .call(d3.axisLeft(y)
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

    const line = d3.line()
      .x((d, i) => x(new Date($filteredData.labels[i])))
      .y(d => d !== null ? y(d * 100) : null)
      .defined(d => d !== null);

    const area = d3.area()
      .x((d, i) => x(new Date($filteredData.labels[i])))
      .y0(d => d.lower !== null ? y(d.lower * 100) : y(0))
      .y1(d => d.upper !== null ? y(d.upper * 100) : y(0))
      .defined(d => d.lower !== null && d.upper !== null);

    // Draw shaded areas for percentiles
    if ($selectedMode === 'percentiles') {
      const percentileDatasets = $filteredData.datasets.filter(d => d.label.includes('Percentile') && d.fill);
      
      // Sort datasets to ensure correct layering (widest range first)
      percentileDatasets.sort((a, b) => {
        const aRange = a.label.match(/\d+/g).map(Number);
        const bRange = b.label.match(/\d+/g).map(Number);
        return (bRange[1] - bRange[0]) - (aRange[1] - aRange[0]);
      });

      percentileDatasets.forEach((dataset) => {
        svg.append('path')
          .datum(dataset.data)
          .attr('fill', dataset.color)
          .attr('fill-opacity', dataset.fillOpacity)
          .attr('d', area);
      });
    }


    svg.selectAll('.line')
      .data($filteredData.datasets.filter(d => !d.fill))
      .enter()
      .append('path')
      .attr('class', d => `line ${d.isPercentileLine ? 'percentile-line' : ''} ${d.isTrust ? 'trust-line' : ''}`)
      .attr('d', d => line(d.data))
      .attr('fill', 'none')
      .attr('stroke', (d, i) => d.color || getTrustColor(i, isPercentileMode))
      .attr('stroke-width', 2.5)
      .attr('stroke-opacity', d => {
        if (d.isPercentileLine && d.label !== 'Median (50th Percentile)') {
          return 0;
        }
        return d.strokeOpacity || 1;
      })
      .attr('stroke-dasharray', d => {
        if (d.label === 'Median (50th Percentile)') {
          return '5,5';
        }
        return d.strokeDasharray || 'none';
      });

 
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
      const isTrust = d.isTrust;
      d3.select(this).attr('stroke-width', 3);
      tooltip.style('opacity', 1);
      svg.selectAll('.line').style('opacity', 0.2);
      d3.select(this).style('opacity', 1);

      let displayName, displayCode;
      if (isTrust) {
        [displayCode, displayName] = d.label.split('|');
      } else {
        displayName = d.label;
        displayCode = '';
      }

      const tooltipX = event.pageX + 10;
      const tooltipY = event.pageY - 10;

      const xPos = d3.pointer(event, this)[0];
      const date = x.invert(xPos);
      const bisectDate = d3.bisector(d => new Date(d)).left;
      const index = bisectDate($filteredData.labels, date);
      const value = d.data[index];
      const numerator = d.numerator ? d.numerator[index] : null;
      const denominator = d.denominator ? d.denominator[index] : null;

      tooltip
        .html(`<strong>${displayName || 'Unknown'}</strong>${displayCode ? `<br>ODS Code: ${displayCode.trim()}` : ''}<br>Date: ${
        d3.timeFormat('%b %Y')(date)}<br>Percentage: ${value?.toFixed(2) || 'N/A'}${
        numerator !== null && denominator !== null ? `<br>Numerator: ${numerator.toFixed(2)}<br>Denominator: ${denominator.toFixed(2)}` : ''}${
        d.org_count ? `<br>Organisations included: ${d.org_count}` : ''
        }`)
        .style('left', `${tooltipX}px`)
        .style('top', `${tooltipY}px`);
    }

    function moveTooltip(event, d) {
      const [xPos, yPos] = d3.pointer(event);
      const date = x.invert(xPos);

      const bisectDate = d3.bisector(d => new Date(d)).left;
      const index = bisectDate($filteredData.labels, date);
      const nearestIndex = index > 0 && (index === $filteredData.labels.length || (date - new Date($filteredData.labels[index - 1])) < (new Date($filteredData.labels[index]) - date)) ? index - 1 : index;
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
        d3.timeFormat('%b %Y')(nearestDate)}<br>Percentage: ${nearestValue?.toFixed(1) || 'N/A'}%${
        numerator !== null && denominator !== null ? `<br>Numerator: ${numerator.toFixed(2)}<br>Denominator: ${denominator.toFixed(2)}` : ''}${
        d.org_count ? `<br>Organisations included: ${d.org_count}` : ''
        }`)
        .style('left', `${tooltipX}px`)
        .style('top', `${tooltipY}px`);
    }

    function hideTooltip() {
      d3.select(this).attr('stroke-width', 2.5);
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
</script>

<style>
  .tooltip {
    transition: opacity 0.2s ease-in-out;
  }

  .line {
    transition: stroke-width 0.2s ease-in-out;
  }

  circle {
    transition: r 0.2s ease-in-out;
  }

  .percentile-line {
    pointer-events: all;
    stroke-opacity: 0;
  }

</style>

<div bind:this={chartDiv} use:resizeAction class="w-full h-[400px]"></div>
