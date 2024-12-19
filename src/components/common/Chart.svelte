<script>
  import { onMount, onDestroy, createEventDispatcher } from 'svelte';
  import * as d3 from 'd3';
  import { chartStore } from '../../stores/chartStore.js';
  
  const dispatch = createEventDispatcher();

  export let data = { labels: [], datasets: [] };
  export let mode = 'percentiles';
  export let yAxisLabel = '';
  export let percentileConfig = {
    medianColor: '#DC3220',
    rangeColor: 'rgb(0, 90, 181)'
  };
  export let store = chartStore;
  export let formatTooltipContent = null;
  
  $: config.visibleItems = new Set(data.datasets.map(d => d.label));
  
  $: ({ 
    data = { labels: [], datasets: [] }, 
    zoomState = { xDomain: null, yDomain: null, isZoomedIn: false }, 
    dimensions = { height: 400, margin: { top: 10, right: 20, bottom: 30, left: 50 } }, 
    config = { 
      visibleItems: new Set(),
      yAxisRange: null
    } 
  } = $store || {});
  
  function handleZoom(xDomain, yDomain) {
    store.updateZoom(xDomain, yDomain);
  }
  
  function calculateYDomain(data, config) {
    const yBehavior = config.yAxisBehavior || {};
    const dataExtent = d3.extent(data.datasets.flatMap(d => d.data));
    
    let [min, max] = dataExtent;
    
    if (yBehavior.forceZero) {
        min = 0;
    }
    
    if (yBehavior.padTop && typeof yBehavior.padTop === 'number') {
        max = max * yBehavior.padTop;
    }
    
    if (config.yAxisRange) {
        [min, max] = config.yAxisRange;
    }
    
    return [min, max];
  }

  function resetZoom() {
    if (!xScale || !yScale || !svg) return;

    const initialXDomain = d3.extent(data.labels, d => new Date(d));
    
    let initialYDomain;
    if (config.yAxisBehavior?.resetToInitial) {
        initialYDomain = calculateYDomain(data, config);
    } else {
        initialYDomain = d3.extent(visibleDatasets.flatMap(d => d.data));
    }
    
    xScale.domain(initialXDomain);
    yScale.domain(initialYDomain);
    
    svg.select('g.x-axis')
      .call(d3.axisBottom(xScale))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px'));

    svg.select('g.y-axis')
      .call(d3.axisLeft(yScale))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px'));

  
    updateXAxis(svg);
    updateYAxis(svg);

    const line = d3.line()
      .x((d, i) => xScale(new Date(data.labels[i])))
      .y(d => {
        if (d === null || d === undefined || isNaN(d)) return null;
        return yScale(d);
      })
      .defined(d => {
        return d !== null && d !== undefined && !isNaN(d) && yScale(d) !== undefined;
      });

    svg.selectAll('.line')
      .attr('d', d => line(d.data));

    if (config.mode === 'percentiles') {
      svg.select('.percentile-areas-group')
        .selectAll('path')
        .attr('d', d => {
          return d3.area()
            .x((d, i) => xScale(new Date(data.labels[i])))
            .y0(d => d.lower !== null ? yScale(d.lower) : yScale(0))
            .y1(d => d.upper !== null ? yScale(d.upper) : yScale(0))
            .defined(d => d.lower !== null && d.upper !== null)
            .curve(d3.curveLinear)
            (d);
        });
    }
    store.resetZoom();

  }

  let chartDiv;
  let tooltip;
  let brush;
  let xScale;
  let yScale;
  let chartHeight;
  let chartWidth;
  let margin;
  let svg;
  let isZoomedIn = false;
  let brushing = false;
  let brushGroup;
  let prevMode;
  let line;

  $: visibleDatasets = data?.datasets?.filter(dataset => {
    if (dataset.hidden) {
        return false;
    }
    if (dataset.alwaysVisible) {
        return true;
    }
    return config.visibleItems.has(dataset.label);
  });

  function formatValue(value) {
    if (value === null || value === undefined || isNaN(value)) return 'No data';
    return value.toFixed(1) + '%';
  }

  function updateZoomState() {
    const xDomain = xScale?.domain();
    const yDomain = yScale?.domain();
    if (!xDomain || !yDomain) return;

    const initialXDomain = d3.extent(data.labels, d => new Date(d));
    const initialYDomain = config.yAxisRange || d3.extent(data.datasets.flatMap(d => d.data));

    const newIsZoomedIn = xDomain[0] > initialXDomain[0] || 
                         xDomain[1] < initialXDomain[1] ||
                         yDomain[0] > initialYDomain[0] || 
                         yDomain[1] < initialYDomain[1];

    if (newIsZoomedIn) {
      store.updateZoom(xDomain, yDomain);
    } else {
      store.resetZoom();
    }
  }

  function updateXAxis(selection, duration = 0) {
    const [start, end] = xScale.domain();
    const monthsDiff = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24 * 30.44);

    const tickSettings = monthsDiff <= 24 
      ? { interval: d3.timeMonth.every(1), format: "%m/%y" }
      : { interval: d3.timeYear.every(1), format: '%Y' };

    selection.select('g.x-axis')
      .call(d3.axisBottom(xScale)
        .ticks(tickSettings.interval)
        .tickFormat(d3.timeFormat(tickSettings.format)))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px')
        .style('text-anchor', monthsDiff <= 24 ? 'start' : 'middle')
        .attr('transform', monthsDiff <= 24 ? 'rotate(45)' : 'rotate(0)')
        .attr('dx', monthsDiff <= 24 ? '0.5em' : '0'));

    selection.select('g.x-grid')
      .call(d3.axisBottom(xScale)
        .ticks(tickSettings.interval)
        .tickSize(-chartHeight)
        .tickFormat(''))
      .call(g => g.select('.domain').remove())
      .call(g => g.selectAll('.tick line')
        .attr('stroke', '#e0e0e0')
        .attr('stroke-opacity', 0.7));
  }

  function updateYAxis(selection, duration = 0) {
    selection.select('g.y-axis')
      .call(d3.axisLeft(yScale)
        .ticks(10)
        .tickFormat(value => config.yAxisTickFormat ? config.yAxisTickFormat(value) : formatValue(value)))
      .call(g => g.selectAll('.tick text')
        .style('font-size', '14px'));

    selection.select('g.y-grid')
      .call(d3.axisLeft(yScale)
        .ticks(10)
        .tickSize(-chartWidth)
        .tickFormat(''))
      .call(g => g.select('.domain').remove())
      .call(g => g.selectAll('.tick line')
        .attr('stroke', '#e0e0e0')
        .attr('stroke-opacity', 0.7));
  }

  function handleResize() {
    if (chartDiv && data?.labels?.length > 0) {
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

  function createChart() {
    if (!chartDiv) return;

    d3.select(chartDiv).selectAll('*').remove();

    margin = dimensions.margin;
    const width = chartDiv.clientWidth || 0;
    const height = chartDiv.clientHeight || 0;
    
    if (width === 0 || height === 0) return;

    chartWidth = width - margin.left - margin.right;
    chartHeight = height - margin.top - margin.bottom;

    svg = d3.select(chartDiv)
        .append('svg')
        .attr('width', '100%')
        .attr('height', height)
        .style('overflow', 'visible')
        .append('g')
        .attr('transform', `translate(${margin.left},${margin.top})`);

    svg.append('defs')
      .append('clipPath')
      .attr('id', 'clip')
      .append('rect')
      .attr('width', chartWidth)
      .attr('height', chartHeight)
      .attr('x', 0)
      .attr('y', 0);
  
    const chartArea = svg.append('g')
      .attr('clip-path', 'url(#clip)');

    const percentileAreasGroup = chartArea.append('g')
      .attr('class', 'percentile-areas-group');
    
    let isMouseDown = false;
    let startPoint = null;

    brushGroup = chartArea.append('g')
        .attr('class', 'brush')
        .attr('clip-path', 'url(#clip)');

    brush = d3.brush()
        .extent([[0, 0], [chartWidth, chartHeight]]);

    brushGroup.call(brush);

    const hoverOverlay = chartArea.append('rect')
        .attr('class', 'hover-overlay')
        .attr('width', chartWidth)
        .attr('height', chartHeight)
        .attr('fill', 'none')
        .style('pointer-events', 'all')
        .style('cursor', 'crosshair')
        .on('mousedown', function(event) {
            event.preventDefault();
            // Hide tooltips and crosshair when starting brush
            hideTooltip();
            hideCrosshair();
            resetHighlight();
            
            isMouseDown = true;
            startPoint = d3.pointer(event, this);
            brush.move(brushGroup, [[startPoint[0], startPoint[1]], [startPoint[0], startPoint[1]]]);
        })
        .on('mousemove', function(event) {
            if (isMouseDown) {
                const currentPoint = d3.pointer(event, this);
                const x0 = Math.min(startPoint[0], currentPoint[0]);
                const x1 = Math.max(startPoint[0], currentPoint[0]);
                const y0 = Math.min(startPoint[1], currentPoint[1]);
                const y1 = Math.max(startPoint[1], currentPoint[1]);
                
                brush.move(brushGroup, [[x0, y0], [x1, y1]]);
            } else {
                handleMouseMove(event);
            }
        })
        .on('mouseup', function(event) {
            if (!isMouseDown) return;
            
            const selection = d3.brushSelection(brushGroup.node());
            if (selection) {
                const [[x0, y0], [x1, y1]] = selection;
                
                // If it's a click or small selection
                if (Math.abs(x1 - x0) < 5 && Math.abs(y1 - y0) < 5) {
                    // Create a default selection box around the click point
                    const centerX = (x0 + x1) / 2;
                    const centerY = (y0 + y1) / 2;
                    const boxSize = 100;
                    
                    const newSelection = [
                        [centerX - boxSize/2, centerY - boxSize/2],
                        [centerX + boxSize/2, centerY + boxSize/2]
                    ];
                    
                    handleBrush({ selection: newSelection });
                } else {
                    handleBrush({ selection });
                }
            }
            
            setTimeout(() => {
                brushGroup.call(brush.clear);
            }, 0);
            
            isMouseDown = false;
            startPoint = null;
        })
        .on('mouseleave', () => {
            if (!isMouseDown) {
                hideCrosshair();
                hideTooltip();
                resetHighlight();
            }
        });

    hoverOverlay.raise();

    const linesGroup = chartArea.append('g')
      .attr('class', 'lines-group');

    xScale = d3.scaleTime()
      .domain(d3.extent(data.labels, d => new Date(d)))
      .range([0, chartWidth]);

    const yDomain = calculateYDomain(data, config);
    yScale = d3.scaleLinear()
        .domain(yDomain)
        .range([chartHeight, 0]);

    const xAxis = svg.append('g')
      .attr('class', 'x-axis')
      .attr('transform', `translate(0,${chartHeight})`);

    const xGrid = svg.append('g')
      .attr('class', 'x-grid')
      .attr('transform', `translate(0,${chartHeight})`);

    updateXAxis(svg, 0);

    svg.append('g')
      .attr('class', 'y-axis');

    svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('y', 0 - margin.left)
      .attr('x', 0 - (chartHeight / 2))
      .attr('dy', '1em')
      .style('text-anchor', 'middle')
      .style('font-size', '16px')
      .text(config.yAxisLabel);

    svg.append('g')
      .attr('class', 'y-grid');

    updateYAxis(svg, 0);

    line = d3.line()
      .x((d, i) => xScale(new Date(data.labels[i])))
      .y(d => {
        if (d === null || d === undefined || isNaN(d)) return null;
        return yScale(d);
      })
      .defined(d => {
        return d !== null && d !== undefined && !isNaN(d) && yScale(d) !== undefined;
      });

    const area = d3.area()
      .x((d, i) => xScale(new Date(data.labels[i])))
      .y0(d => d.lower !== null ? yScale(d.lower) : yScale(0))
      .y1(d => d.upper !== null ? yScale(d.upper) : yScale(0))
      .defined(d => d.lower !== null && d.upper !== null)
      .curve(d3.curveLinear);

  
    if (config.mode === 'percentiles') {
      const percentileDatasets = data.datasets.filter(d => d.label.includes('Percentile') && d.fill);
      
      percentileDatasets.sort((a, b) => {
        const aRange = a.label.match(/\d+/g).map(Number);
        const bRange = b.label.match(/\d+/g).map(Number);
        return (bRange[1] - bRange[0]) - (aRange[1] - aRange[0]);
      });

      percentileDatasets.forEach((dataset) => {
        percentileAreasGroup.append('path')
          .datum(dataset.data)
          .attr('fill', dataset.color || config.percentileConfig.rangeColor)
          .attr('fill-opacity', dataset.fillOpacity || 0.2)
          .attr('d', area);
      });
    }

    linesGroup.selectAll('.line')
      .data(data.datasets.filter(d => {
        if (config.mode === 'percentiles') {
          return d.label === 'Median (50th Percentile)' || d.isTrust;
        }
        return !d.fill;
      }))
      .enter()
      .append('path')
      .attr('class', (d, i) => `line line-${i}`)
      .attr('d', d => {
        if (!d.data || !Array.isArray(d.data)) return null;
        const validData = d.data.map((value, i) => {
          const parsedValue = parseFloat(value);
          return isNaN(parsedValue) ? null : parsedValue;
        });
        return line(validData);
      })
      .attr('fill', 'none')
      .attr('stroke', d => d.color || (d.isPercentileLine ? config.percentileConfig.medianColor : '#000'))
      .attr('stroke-width', 2.5)
      .attr('stroke-opacity', d => d.strokeOpacity || 1)
      .attr('stroke-dasharray', d => d.strokeDasharray || 'none');

    tooltip = d3.select('body')
      .append('div')
      .attr('class', 'tooltip')
      .style('position', 'absolute')
      .style('pointer-events', 'none')
      .style('opacity', 0)
      .style('background-color', 'rgba(255, 255, 255, 0.95)')
      .style('border', '1px solid #ccc')
      .style('border-radius', '4px')
      .style('padding', '6px 8px')
      .style('box-shadow', '0 2px 4px rgba(0,0,0,0.1)')
      .style('z-index', '1000')
      .style('max-width', '300px');

    updateZoomState();

    const crosshairGroup = svg.append('g')
      .attr('class', 'crosshair-group')
      .style('display', 'none');

    crosshairGroup.append('line')
      .attr('class', 'crosshair-vertical')
      .attr('y1', 0)
      .attr('y2', chartHeight)
      .style('stroke', '#666')
      .style('stroke-width', 1)
      .style('stroke-dasharray', '3,3');

    crosshairGroup.append('line')
      .attr('class', 'crosshair-horizontal')
      .attr('x1', 0)
      .attr('x2', chartWidth)
      .style('stroke', '#666')
      .style('stroke-width', 1)
      .style('stroke-dasharray', '3,3');


    linesGroup.selectAll('.hover-group')
        .data(data.datasets.filter(d => {
            if (config.mode === 'percentiles') {
                return !d.fill && (d.label === 'Median (50th Percentile)' || d.isTrust);
            }
            return !d.fill;
        }))
        .enter()
        .append('g')
        .attr('class', (d, i) => `hover-group group-${i}`);

    svg.selectAll('.line, .percentile-area')
        .on('mouseover', (event, d) => {
            showTooltip(event, d);
        })
        .on('mouseout', () => {
            hideTooltip();
        });
  }

  function handleBrush(event) {
    if (!event.selection) return;
    
    const [[x0, y0], [x1, y1]] = event.selection;

    const newXDomain = [x0, x1].map(xScale.invert);
    const newYDomain = [y1, y0].map(yScale.invert);
    
    xScale.domain(newXDomain);
    yScale.domain(newYDomain);

    updateChartElements();
    updateXAxis(svg);
    updateYAxis(svg);
    
    handleZoom(newXDomain, newYDomain);
    
    isZoomedIn = true;

    dispatch('zoomChange', {
        xDomain: newXDomain,
        yDomain: newYDomain
    });
  }

  function handleMouseMove(event) {
    if (brushing) return;

    const [mouseX, mouseY] = d3.pointer(event);
    const maxDistance = 100;
    let closestPoint = null;
    let minDistance = Infinity;
    let closestDatasetIndex = -1;

    const datasetsToCheck = data.datasets.filter(dataset => {
        if (dataset.alwaysVisible) return true;
        if (dataset.hidden) return false;
        if (config.mode === 'percentiles') {
            return !dataset.fill && (dataset.label === 'Median (50th Percentile)' || dataset.isTrust);
        }
        return !dataset.fill;
    });

    datasetsToCheck.forEach((dataset, datasetIndex) => {
        dataset.data.forEach((value, i) => {
            if (value === null || value === undefined) return;

            const xPos = xScale(new Date(data.labels[i]));
            const yPos = yScale(value);
            
            const distance = Math.sqrt(
                Math.pow(mouseX - xPos, 2) + 
                Math.pow(mouseY - yPos, 2)
            );

            if (distance < minDistance && distance < maxDistance) {
                minDistance = distance;
                closestPoint = {
                    date: new Date(data.labels[i]),
                    value: value,
                    dataset: dataset,
                    index: i
                };
                closestDatasetIndex = datasetIndex;
            }
        });
    });

    if (closestPoint) {
        showTooltip(event, closestPoint);
        updateCrosshair(event, closestPoint);
        highlightDataset(closestDatasetIndex);
    } else {
        hideCrosshair();
        hideTooltip();
        resetHighlight();
    }
  }

  function updateChartElements() {
    if (!svg || !data || !xScale || !yScale) return;

    const linesGroup = svg.select('.lines-group');
    const lines = linesGroup.selectAll('.line')
        .data(visibleDatasets.filter(d => !d.fill), d => d.label);

    if (config.mode === 'percentiles') {
        const areasGroup = svg.select('.percentile-areas-group');
        const areas = areasGroup.selectAll('path')
            .data(visibleDatasets.filter(d => d.fill), d => d.label);

        areas.attr('d', d => {
            return d3.area()
                .x((d, i) => xScale(new Date(data.labels[i])))
                .y0(d => d.lower !== null ? yScale(d.lower) : yScale(0))
                .y1(d => d.upper !== null ? yScale(d.upper) : yScale(0))
                .defined(d => d.lower !== null && d.upper !== null)
                .curve(d3.curveLinear)
                (d.data);
        });

        areas.enter()
            .append('path')
            .attr('class', 'percentile-area')
            .attr('fill', d => d.color)
            .attr('fill-opacity', d => d.fillOpacity || 0.2)
            .attr('d', d => {
                return d3.area()
                    .x((_, i) => xScale(new Date(data.labels[i])))
                    .y0(d => d.lower !== null ? yScale(d.lower) : yScale(0))
                    .y1(d => d.upper !== null ? yScale(d.upper) : yScale(0))
                    .defined(d => d.lower !== null && d.upper !== null)
                    .curve(d3.curveLinear)
                    (d.data);
            });

        areas.exit().remove();
    }

    const line = d3.line()
        .x((_, i) => xScale(new Date(data.labels[i])))
        .y(d => {
            if (d === null || d === undefined || isNaN(d)) return null;
            return yScale(d);
        })
        .defined(d => d !== null && d !== undefined && !isNaN(d));

    lines.attr('d', d => line(d.data))
        .attr('stroke', d => d.color)
        .attr('stroke-width', d => d.strokeWidth || 2);

    lines.enter()
        .append('path')
        .attr('class', 'line')
        .attr('fill', 'none')
        .attr('stroke', d => d.color)
        .attr('stroke-width', d => d.strokeWidth || 2)
        .attr('d', d => line(d.data));

    lines.exit().remove();

    svg.selectAll('.hover-group').each(function(dataset) {
        const group = d3.select(this);
        const isVisible = dataset.alwaysVisible || !dataset.hidden;
        group.style('display', isVisible ? 'block' : 'none');
    });

    const crosshair = svg.select('.crosshair-group');
    if (crosshair.style('display') !== 'none') {
        const currentPoint = crosshair.datum();
        if (currentPoint) {
            crosshair.select('.crosshair-vertical')
                .attr('x1', xScale(currentPoint.date))
                .attr('x2', xScale(currentPoint.date));

            crosshair.select('.crosshair-horizontal')
                .attr('y1', yScale(currentPoint.value))
                .attr('y2', yScale(currentPoint.value));
        }
    }

    svg.selectAll('.hover-group').each(function(dataset) {
        const group = d3.select(this);
        
        group.selectAll('.hover-area')
            .attr('cx', (_, i) => xScale(new Date(data.labels[i])))
            .attr('cy', (_, i) => dataset.data[i] !== null ? yScale(dataset.data[i]) : null);
    });

    svg.selectAll('.line')
        .filter(d => d.isOrganisation)
        .on('mouseover', (event, d) => {
            d.data.forEach((value, index) => {
                const pointData = {
                    date: new Date(data.labels[index]),
                    value: value,
                    dataset: d,
                    index: index
                };
                showTooltip(event, pointData);
            });
        })
        .on('mouseout', () => {
            hideTooltip();
        });

    svg.selectAll('.percentile-line')
        .on('mouseover', null)
        .on('mouseout', null);
  }

  function renderTooltip(data) {
    if (!Array.isArray(data)) return '';
    
    return data.map(item => {
        if (item.text) {
            return `<div class="${item.class || ''}">${item.text}</div>`;
        }
        return `<div class="tooltip-row">${item.label}: ${item.value}</div>`;
    }).join('');
  }

  function showTooltip(event, d) {
    if (!d || !d.dataset) return;

    let tooltipContent;
    if (formatTooltipContent) {
        tooltipContent = formatTooltipContent(d);
        tooltip
            .html(renderTooltip(tooltipContent))
            .style('left', `${event.pageX + 12}px`)
            .style('top', `${event.pageY + 12}px`)
            .style('opacity', 1);
    }
  }

  function hideTooltip() {
    if (!tooltip) return;
    tooltip.style('opacity', 0);
  }

  function highlightDataset(datasetIndex) {
    if (!svg) return;

    svg.selectAll('.line, .data-point')
      .style('opacity', 0.2);

    svg.select(`.line-${datasetIndex}`)
      .style('opacity', 1)
      .style('stroke-width', '3');

    svg.selectAll(`.point-${datasetIndex}`)
      .style('opacity', 1)
      .attr('r', 4);
  }

  function resetHighlight() {
    if (!svg) return;

    svg.selectAll('.line')
      .style('opacity', 1)
      .style('stroke-width', '2');

    svg.selectAll('.data-point')
      .style('opacity', 1)
      .attr('r', 3);
  }


  function updateCrosshair(event, d) {
    const crosshair = svg.select('.crosshair-group')
        .style('display', 'block')
        .datum(d);

    crosshair.select('.crosshair-vertical')
        .attr('x1', xScale(d.date))
        .attr('x2', xScale(d.date));

    crosshair.select('.crosshair-horizontal')
        .attr('y1', yScale(d.value))
        .attr('y2', yScale(d.value));
  }

  function hideCrosshair() {
    svg.select('.crosshair-group')
      .style('display', 'none');
  }

  onMount(() => {
    if (data?.labels?.length > 0) {
      createChart();
      updateChartElements();
    }
  });

  onDestroy(() => {
    if (tooltip) {
      tooltip.remove();
    }
  });

  $: if (data?.labels?.length > 0 && chartDiv) {
    if (!svg || prevMode !== config.mode) {
      createChart();
      prevMode = config.mode;
    } else {
      updateChartElements();
    }
  }

  $: {
    if ($store.zoomState.xDomain && $store.zoomState.yDomain) {
      isZoomedIn = true;
    } else {
      isZoomedIn = false;
    }
  }

  $: if (data?.datasets?.length > 0 && svg) {
    xScale.domain(d3.extent(data.labels, d => new Date(d)));
    yScale.domain(calculateYDomain(data, config));

    updateXAxis(svg);
    updateYAxis(svg);

    const line = d3.line()
      .x((d, i) => xScale(new Date(data.labels[i])))
      .y(d => {
        if (d === null || d === undefined || isNaN(d)) return null;
        return yScale(d);
      })
      .defined(d => {
        return d !== null && d !== undefined && !isNaN(d) && yScale(d) !== undefined;
      });

    svg.selectAll('.line').remove();

    svg.select('.lines-group')
      .selectAll('.line')
      .data(data.datasets)
      .enter()
      .append('path')
      .attr('class', (d, i) => `line line-${i}`)
      .attr('d', d => line(d.data))
      .attr('fill', 'none')
      .attr('stroke', d => d.color || '#000')
      .attr('stroke-width', d => d.strokeWidth || 2)
      .attr('stroke-opacity', d => d.strokeOpacity || 1);

    updateChartElements(0);
  }

  $: if ($store.zoomState.xDomain && $store.zoomState.yDomain && xScale && yScale) {
    xScale.domain($store.zoomState.xDomain);
    yScale.domain($store.zoomState.yDomain);
    updateChartElements();
    updateXAxis(svg);
    updateYAxis(svg);
  }

  $: if (data && config) {
    updateChartElements();
  }

</script>

<div class="relative w-full" style="height: {dimensions.height}px">
  <div bind:this={chartDiv} use:resizeAction class="w-full h-full"></div>
  {#if $store.zoomState.isZoomedIn}
    <button 
      class="absolute top-6 right-10 px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 rounded-md shadow-sm"
      on:click={resetZoom}
    >
      Reset Zoom
    </button>
  {/if}
</div>

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

  .data-point {
    transition: r 0.2s ease-in-out;
  }

  .hover-circle {
    pointer-events: none;
  }

  :global(.tooltip) {
    transition: opacity 0.2s ease-in-out;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  }

  :global(.tooltip .font-medium) {
    font-weight: 600;
    font-size: 14px;
    margin-bottom: 2px;
  }

  :global(.tooltip .text-base) {
    font-size: 14px;
    line-height: 1.2;
  }

  :global(.tooltip .text-gray-600) {
    color: #4B5563;
  }

  :global(.tooltip .tooltip-row) {
    line-height: 1.1;
    margin-bottom: 1px;
  }

  :global(.tooltip .tooltip-row:last-child) {
    margin-bottom: 0;
  }

  :global(.crosshair-group line) {
    pointer-events: none;
  }

  :global(.hover-area) {
    cursor: pointer;
  }

  :global(.brush) {
    pointer-events: all !important;
  }

  :global(.brush .overlay) {
    pointer-events: all !important;
    cursor: crosshair !important;
  }

  :global(.brush .selection) {
    stroke: #666;
    fill: rgba(100, 100, 100, 0.2);
    stroke-width: 1px;
  }

  :global(.hover-overlay) {
    cursor: crosshair;
  }

  :global(.brush .handle) {
    pointer-events: none;
  }
</style>
