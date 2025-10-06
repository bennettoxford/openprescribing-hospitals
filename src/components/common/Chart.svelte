<script>
  import { createEventDispatcher } from 'svelte';
  import { onMount } from 'svelte';

  import * as Highcharts from 'highcharts';
  import HCMore from 'highcharts/highcharts-more';
  import Exporting from 'highcharts/modules/exporting';
  import ExportData from 'highcharts/modules/export-data';
  import Accessibility from 'highcharts/modules/accessibility';
  import Boost from 'highcharts/modules/boost';
  import Annotations from 'highcharts/modules/annotations';
  import { Chart} from '@highcharts/svelte';
  import { chartStore } from '../../stores/chartStore.js';

  
  const dispatch = createEventDispatcher();

  export let data = { labels: [], datasets: [] };
  export let mode = 'trust';
  export let yAxisLabel = '';
  export let percentileConfig = {
    medianColor: '#DC3220',
    rangeColor: 'rgb(0, 90, 181)'
  };
  export let store = chartStore;
  export let formatTooltipContent = null;
  export let chartOptions = {};
  export let annotations = '[]';
  
  let showAnnotations = true;

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

  $: visibleDatasets = data?.datasets?.filter(dataset => {
    if (dataset.hidden) return false;
    if (dataset.alwaysVisible) return true;
    return config.visibleItems.has(dataset.label);
  });

  $: shouldUseBoost = visibleDatasets && visibleDatasets.length > 20 && 
                    config.mode !== 'trust';

  $: chartAnnotations = showAnnotations ? createChartAnnotations(annotations, config.yAxisRange || [0, 100]) : [];
  
  function toggleAnnotations() {
    showAnnotations = !showAnnotations;
  }
  
  $: exportMenuItems = [
    'downloadCSV',
    {
      text: 'View in full screen',
      onclick: function () {
        this.fullscreen.toggle();
      }
    },
    ...(annotations !== '[]' ? [{
      text: showAnnotations ? 'Hide annotations' : 'Show annotations',
      onclick: function () {
        toggleAnnotations();
      }
    }] : [])
  ];

  function parseAnnotations(annotationsData) {
    try {
      if (!annotationsData || annotationsData === '[]' ) {
        return [];
      }
 
      if (typeof annotationsData === 'string') {
        return JSON.parse(annotationsData);
      } else {
        return [];
      }
    } catch (error) {
      console.error('Failed to parse annotations:', error);
      return [];
    }
  }

  function createChartAnnotations(annotationsData, yAxisRange = [0, 100]) {
    const parsedAnnotations = parseAnnotations(annotationsData);
    
    if (!parsedAnnotations || parsedAnnotations.length === 0) {
      return [];
    }
    
    const annotations = [];
    
    parsedAnnotations.forEach((annotation, index) => {
      const annotationDate = new Date(annotation.date).getTime();
      
      // First annotation: Fixed vertical line
      annotations.push({
        id: `annotation-line-${index}-${annotation.date}`,
        draggable: false,
        shapes: [{
          type: 'path',
          points: [{
            xAxis: 0,
            yAxis: 0,
            x: annotationDate,
            y: yAxisRange[0]
          }, {
            xAxis: 0,
            yAxis: 0,
            x: annotationDate,
            y: yAxisRange[1]
          }],
          stroke: annotation.colour || '#DC3220',
          strokeWidth: 2,
          dashStyle: 'Dash',
          fill: 'none'
        }]
      });
      
      // Second annotation: Draggable label
      annotations.push({
        id: `annotation-label-${index}-${annotation.date}`,
        draggable: 'y',
        labelOptions: {
          allowOverlap: true,
          backgroundColor: 'rgba(255, 255, 255, 0.9)',
          borderColor: annotation.colour || '#DC3220',
          borderRadius: 4,
          borderWidth: 1,
          style: {
            color: '#333',
            fontSize: '12px',
            fontWeight: 'normal',
            width: 'auto',
            minWidth: '200px',
            maxWidth: '500px',
            whiteSpace: 'normal',
            textAlign: 'left',
            cursor: 'ns-resize'
          },
          useHTML: true,
          shape: 'rect',
        },
        labels: [{
          point: {
            xAxis: 0,
            yAxis: 0,
            x: annotationDate,
            y: yAxisRange[1] * 0.9
          },
          text: annotation.description ? 
            `<div style="padding: 6px 10px; min-width: 200px; max-width: 500px; word-wrap: break-word;">
              <div style="font-weight: 600; margin-bottom: 3px; line-height: 1.2;">${annotation.label}</div>
              <div style="color: #666; font-size: 11px; line-height: 1.3; word-wrap: break-word;">${annotation.description}</div>
            </div>` : 
            `<div style="font-weight: 600;padding: 6px 10px; min-width: 200px;">${annotation.label}</div>`,
          useHTML: true
        }]
      });
    });
    
    return annotations;
  }

  $: finalChartOptions = {
    ...chartOptions,
    chart: {
      type: 'line',
      height: 500,
      zoomType: 'xy',
      panning: true,
      panKey: 'shift',
      accessibility: { enabled: true },
      resetZoomButton: {
        position: {
          align: 'right',
          verticalAlign: 'top',
          x: -10,
          y: 10
        },
        theme: {
          fill: '#2563eb',
          stroke: '#1e40af',
          r: 4,
          states: {
            hover: {
              fill: '#1d4ed8',
              stroke: '#1e3a8a'
            }
          },
          style: {
            color: '#ffffff',
            fontWeight: 'bold'
          }
        }
      },
      selectionMarkerFill: 'rgba(37, 99, 235, 0.4)',
      style: {
        cursor: 'crosshair'
      },
      events: {
        selection: (event) => {
          if (event.resetSelection) {
            store.resetZoom();
            dispatch('zoomChange', {
              xDomain: null,
              yDomain: null
            });
            return;
          }
          
          store.updateZoom(event.xAxis[0].min, event.xAxis[0].max);
          dispatch('zoomChange', {
            xDomain: [event.xAxis[0].min, event.xAxis[0].max],
            yDomain: [event.yAxis[0].min, event.yAxis[0].max]
          });
        },
        pan: function(event) {
          store.updateZoom(
            event.target.xAxis[0].min,
            event.target.xAxis[0].max
          );
          dispatch('zoomChange', {
            xDomain: [event.target.xAxis[0].min, event.target.xAxis[0].max],
            yDomain: [event.target.yAxis[0].min, event.target.yAxis[0].max]
          });
        }
      },
      spacingRight: 20,
      boost: shouldUseBoost ? {
        enabled: true,
        useGPUTranslations: true,
        usePreallocated: true,
        seriesThreshold: 20,
        allowForce: true
      } : {
        enabled: false
      }
    },
    title: {
      text: undefined
    },
    legend: {
      enabled: true,
      align: 'right',
      verticalAlign: 'top',
      layout: 'vertical',
      maxHeight: 400,
      navigation: {
        activeColor: '#2563eb',
        animation: false,
        arrowSize: 12,
        inactiveColor: '#94a3b8',
        style: {
          fontWeight: 'bold',
          color: '#333',
          fontSize: '14px'
        }
      },
      itemStyle: {
        fontSize: '14px',
        fontWeight: 'normal',
        cursor: 'pointer',
        textOverflow: 'ellipsis',
        width: '180px'
      },
      useHTML: true,
      backgroundColor: '#ffffff',
      borderWidth: 0,
      y: 30,
      symbolWidth: 20,
      symbolHeight: 12,
    },
    responsive: {
      rules: [{
        condition: {
          maxWidth: 960
        },
        chartOptions: {
          chart: {
            height: 550,
            spacingBottom: 30
          },
          legend: {
            align: 'center',
            verticalAlign: 'bottom',
            layout: 'horizontal',
            maxHeight: 130,
            y: 40,
            x: 0,
            itemStyle: {
              width: 120
            },
            itemWidth: 160,
            itemMarginTop: 4,
            itemMarginBottom: 4,
            padding: 20
          }
        }
      }]
    },
    xAxis: {
      type: 'datetime',
      minRange: 30 * 24 * 3600 * 1000,
      tickPixelInterval: 100,
      labels: {
        style: {
          fontSize: '14px'
        }
      },
      gridLineWidth: 1,
      gridLineColor: '#e0e0e0'
    },
    yAxis: {
      min: 0,
      minRange: 0.001,
      ...chartOptions.yAxis,
      tickAmount: undefined,
      tickInterval: undefined,
      title: {
        text: config.yAxisLabel || yAxisLabel,
        style: {
          fontSize: '16px'
        }
      },
      labels: {
        style: {
          fontSize: '14px'
        },
        formatter: function() {
          if (config.yAxisTickFormat) {
            return config.yAxisTickFormat(this.value, this.axis.max - this.axis.min);
          }
          
          // Get the axis range based on current view
          const range = this.axis.max - this.axis.min;
          
          // Determine decimal places based on range
          let decimals = 1;
          if (range <= 0.1) decimals = 3;
          else if (range <= 1) decimals = 2;
          
          return `${this.value.toFixed(decimals)}%`;
        }
      },
      gridLineWidth: 1,
      gridLineColor: '#e0e0e0'
    },
    tooltip: {
      shared: false,
      useHTML: true,
      snap: 10,
      distance: 20,
      hideDelay: 0,
      positioner: function (labelWidth, labelHeight, point) {
        const chart = this.chart;
        const plotLeft = chart.plotLeft;
        const plotRight = chart.plotLeft + chart.plotWidth;
        const plotTop = chart.plotTop;
        const plotBottom = chart.plotTop + chart.plotHeight;
        
        let x = point.plotX + plotLeft - (labelWidth / 2);
        let y = point.plotY + plotTop;

      
        if (x + labelWidth > plotRight) {
          x = plotRight - labelWidth - 10;
        }
        
        if (x < plotLeft) {
          x = plotLeft + 10;
        }

        if (y + labelHeight > plotBottom) {
          y = y - labelHeight - 10;
        } else {
          y = y + 10;
        }
        y = Math.max(plotTop, Math.min(plotBottom - labelHeight, y));

        return {
          x: x,
          y: y
        };
      },
      formatter: function() {
        if (!formatTooltipContent) return false;
        
        if (this.series.type === 'arearange') return false;
        
        if (this.point.dist > 20) return false;
        
        const point = {
          date: this.x,
          value: this.y,
          dataset: {
            label: this.series.name,
            isProduct: this.series.options.isProduct,
            isOrganisation: this.series.options.isOrganisation,
            isProductGroup: this.series.options.isProductGroup,
            isUnit: this.series.options.isUnit,
            isIngredient: this.series.options.isIngredient,
            numerator: this.series.options.numerator,
            denominator: this.series.options.denominator
          },
          index: this.point.index
        };
        
        return formatTooltipContent(point)
          .map(item => {
            if (item.text) {
              return `<div class="${item.class || ''}">${item.text}</div>`;
            }
            return `<div class="tooltip-row">${item.label}: ${item.value}</div>`;
          })
          .join('');
      }
    },
    plotOptions: {
      series: {
        animation: false,
        findNearestPointBy: 'xy',
        stickyTracking: false,
        marker: {
          enabled: shouldUseBoost ? false : false
        },
        states: {
          hover: {
            enabled: true,
            brightness: function() {
              return this.type === 'arearange' ? 0 : 0.1;
            }
          },
          inactive: {
            opacity: 1
          }
        },
        point: {
          events: {
            mouseOver: function() {
              if (this.dist > 20 || this.series.type === 'arearange') {
                this.series.chart.tooltip.hide();
              }
            }
          }
        },
        boostThreshold: shouldUseBoost ? 1 : 2000,
        cropThreshold: shouldUseBoost ? 1000 : 300,
        turboThreshold: shouldUseBoost ? 1000 : 1000
      },
      line: {
        lineWidth: shouldUseBoost ? 1 : 2,
        marker: {
          enabled: false,
          radius: shouldUseBoost ? 2 : 3
        }
      },
      arearange: {
        states: {
          hover: {
            enabled: false
          },
          inactive: {
            opacity: 1
          }
        }
      }
    },
    series: visibleDatasets.map(dataset => ({
      name: dataset.label,
      type: dataset.isRange ? 'arearange' : 'line',
      data: dataset.isRange 
        ? dataset.data.map((y, i) => [
            new Date(data.labels[i]).getTime(),
            y.lower,
            y.upper
          ])
        : dataset.data.map((y, i) => [
            new Date(data.labels[i]).getTime(),
            y
          ]),
      isProduct: dataset.isProduct,
      isOrganisation: dataset.isOrganisation,
      isProductGroup: dataset.isProductGroup,
      isUnit: dataset.isUnit,
      isIngredient: dataset.isIngredient,
      numerator: dataset.numerator,
      denominator: dataset.denominator,
      color: dataset.color,
      fillOpacity: dataset.fillOpacity,
      lineWidth: dataset.isRange ? 0 : (dataset.strokeWidth || 2),
      visible: !dataset.hidden,
      showInLegend: true,
      zIndex: dataset.isRange ? 1 : 2,
      events: {
        legendItemClick: function(e) {
          if (dataset.alwaysVisible) {
            e.preventDefault();
            return false;
          }
        }
      }
    })),
    annotations: chartAnnotations,
    exporting: {
      enabled: true,
      buttons: {
        contextButton: {
          menuItems: exportMenuItems
        }
      },
      csv: {
        dateFormat: '%Y-%m-%d',
        decimalPoint: '.',
        itemDelimiter: ',',
        lineDelimiter: '\n'
      },
      fallbackToExportServer: false
    }
  };

  $: if ($store.zoomState.xDomain && $store.zoomState.yDomain) {
    finalChartOptions.xAxis.min = $store.zoomState.xDomain[0];
    finalChartOptions.xAxis.max = $store.zoomState.xDomain[1];
    finalChartOptions.yAxis.min = $store.zoomState.yDomain[0];
    finalChartOptions.yAxis.max = $store.zoomState.yDomain[1];
  }

  let isTouchDevice = false;
  
  onMount(() => {
    isTouchDevice = ('ontouchstart' in window) || 
                   (navigator.maxTouchPoints > 0) || 
                   (navigator.msMaxTouchPoints > 0);
  });
</script>

<div class="relative w-full" style="height: {dimensions.height}px">
  <div class="px-3 py-1 text-sm text-gray-700 flex justify-end">
    <div class="flex items-center gap-3">
      <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 text-amber-500" viewBox="0 0 20 20" fill="currentColor">
        <path d="M11 3a1 1 0 10-2 0v1a1 1 0 102 0V3zM15.657 5.757a1 1 0 00-1.414-1.414l-.707.707a1 1 0 001.414 1.414l.707-.707zM18 10a1 1 0 01-1 1h-1a1 1 0 110-2h1a1 1 0 011 1zM5.05 6.464A1 1 0 106.464 5.05l-.707-.707a1 1 0 00-1.414 1.414l.707.707zM5 10a1 1 0 01-1 1H3a1 1 0 110-2h1a1 1 0 011 1zM8 16v-1h4v1a2 2 0 11-4 0zM12 14c.015-.34.208-.646.477-.859a4 4 0 10-4.954 0c.27.213.462.519.476.859h4.002z" />
      </svg>
      {#if isTouchDevice}
        <span>Pinch to zoom, two-finger drag to pan</span>
      {:else}
        <span>Click and drag to zoom, <kbd class="px-1.5 py-0.5 bg-gray-100 border border-gray-300 rounded text-xs">Shift</kbd> + drag to pan</span>
      {/if}
    </div>
  </div>
  <Chart options={finalChartOptions} highcharts={Highcharts} />
</div>

<style>
  :global(.tooltip) {
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
</style>
