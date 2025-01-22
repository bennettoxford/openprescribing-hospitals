<script>
  import { createEventDispatcher } from 'svelte';

  import * as Highcharts from 'highcharts';
  import HCMore from 'highcharts/highcharts-more';
  import Exporting from 'highcharts/modules/exporting';
  import ExportData from 'highcharts/modules/export-data';
  import Accessibility from 'highcharts/modules/accessibility';
  import { Chart} from '@highcharts/svelte';
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
  export let chartOptions = {};

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

  $: finalChartOptions = {
    ...chartOptions,
    chart: {
      height: dimensions.height,
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
        }
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
      spacingRight: 20
    },
    title: {
      text: undefined
    },
    legend: {
      enabled: true,
      align: 'right',
      verticalAlign: 'top',
      layout: 'vertical',
      maxHeight: 300,
      navigation: {
        activeColor: '#2563eb',
        animation: true,
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
      y: 30
    },
    responsive: {
      rules: [{
        condition: {
          maxWidth: 500
        },
        chartOptions: {
          chart: {
            height: dimensions.height + 100,
            marginBottom: 170,
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
              width: 90
            },
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
            isRoute: this.series.options.isRoute,
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
          enabled: false
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
      data: dataset.data.map((y, i) => ({
        x: new Date(data.labels[i]),
        y: dataset.isRange ? undefined : y,
        low: dataset.isRange ? y.lower : undefined,
        high: dataset.isRange ? y.upper : undefined
      })),
      isProduct: dataset.isProduct,
      isOrganisation: dataset.isOrganisation,
      isProductGroup: dataset.isProductGroup,
      isUnit: dataset.isUnit,
      isRoute: dataset.isRoute,
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
    exporting: {
      enabled: true,
      buttons: {
        contextButton: {
          menuItems: [
            'downloadCSV',
            {
              text: 'View in full screen',
              onclick: function () {
                this.fullscreen.toggle();
              }
            }
          ]
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
</script>

<div class="relative w-full" style="height: {dimensions.height}px">
  <Chart options={finalChartOptions} highcharts={Highcharts} />
</div>

<style>
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
</style>
