<svelte:options customElement={{
    tag: 'measure-mini-chart',
    props: {
        chartdata: { type: 'String', reflect: true },
        mode: { type: 'String', reflect: true },
        ispercentage: { type: 'String', reflect: true },
        quantitytype: { type: 'String', reflect: true },
        slug: { type: 'String', reflect: true },
        minTrustsForPercentiles: { type: 'Number', reflect: true, attribute: 'min-trusts-for-percentiles' }
    },
    shadow: 'none'
}} />


<script>
  import { onDestroy } from 'svelte';
  import * as Highcharts from 'highcharts';
  import 'highcharts/highcharts-more';
  import { regionColors, chartConfig } from '../../utils/chartConfig.js';
  import { getChartForSlug, isLoadingCharts } from '../../stores/measuresListStore.js';

  export let chartdata = '{}';
  export let mode = 'national';
  export let ispercentage = 'false';
  export let quantitytype = '';
  export let slug = '';
  export let minTrustsForPercentiles = 30;

  let chartContainer;
  let chart;
  let prevChartdata = '';
  let prevMode = '';

  $: chartForSlug = slug ? getChartForSlug(slug) : null;
  $: storeChart = chartForSlug ? $chartForSlug : null;
  $: effectiveChartdata = (storeChart?.data && Object.keys(storeChart.data).length > 0)
    ? JSON.stringify(storeChart.data)
    : chartdata;
  $: effectiveMode = storeChart?.mode ?? mode;

  $: isPercentageBool = ispercentage === 'true';

  $: yAxisRange = isPercentageBool ? { min: 0, max: 100 } : { min: 0 };

  $: if (chartContainer && effectiveChartdata) {
    try {
      const parsedData = JSON.parse(effectiveChartdata);
      if (parsedData && Object.keys(parsedData).length > 0) {
        if (!chart) {
          chart = Highcharts.chart(chartContainer, buildChartOptions(parsedData, effectiveMode));
          prevChartdata = effectiveChartdata;
          prevMode = effectiveMode;
        } else if (effectiveChartdata !== prevChartdata || effectiveMode !== prevMode) {
          updateChart();
        }
      }
    } catch (e) {
      console.error('Failed to create/update chart', e);
    }
  }

  function updateChart() {
    const modeChanged = effectiveMode !== prevMode;

    let oldData = {};
    try { oldData = prevChartdata ? JSON.parse(prevChartdata) : {}; } catch (_) {}

    prevChartdata = effectiveChartdata;
    prevMode = effectiveMode;

    try {
      const parsedData = JSON.parse(effectiveChartdata);
      if (!parsedData || Object.keys(parsedData).length === 0) return;

      const structureChanged =
        modeChanged ||
        (effectiveMode === 'trust' && !!parsedData.trustData !== !!oldData.trustData) ||
        (effectiveMode === 'trust' && !!parsedData.trustSeries !== !!oldData.trustSeries) ||
        (effectiveMode === 'region' && parsedData.highlightRegion !== oldData.highlightRegion);

      const options = buildChartOptions(parsedData, effectiveMode);

      if (structureChanged) {
        chart.destroy();
        chart = Highcharts.chart(chartContainer, options);
      } else {
        chart.update(options, true);
      }
    } catch (error) {
      console.error('MeasureMiniChart: failed to update', error);
    }
  }

  function formatUnits(v) {
    const prefix = quantitytype === 'indicative_cost' ? '£' : '';

    if (v >= 1_000_000) return `${prefix}${strip(v / 1_000_000)}M`;
    if (v >= 1_000)     return `${prefix}${strip(v / 1_000)}K`;
    if (prefix)         return `${prefix}${v.toFixed(0)}`;
    return v >= 10 ? v.toFixed(0) : v.toFixed(1);
  }

  function strip(n) {
    const s = n.toFixed(1);
    return s.endsWith('.0') ? s.slice(0, -2) : s;
  }

  const PERCENTILE_BANDS = [
    { lower: 5,  upper: 95, opacity: 0.1 },
    { lower: 15, upper: 85, opacity: 0.2 },
    { lower: 25, upper: 75, opacity: 0.4 },
    { lower: 35, upper: 65, opacity: 0.6 },
    { lower: 45, upper: 55, opacity: 0.8 },
  ];

  function buildChartOptions(data, chartMode) {
    const base = {
      chart: {
        height: 280,
        margin: [10, 10, 45, 40],
        spacing: [0, 0, 0, 0],
        backgroundColor: 'transparent'
      },
      title: { text: null },
      credits: { enabled: false },
      legend: { enabled: false },
      exporting: { enabled: false },
      tooltip: { enabled: false },
      plotOptions: {
        series: {
          animation: false,
          marker: { enabled: false },
          states: { hover: { enabled: false }, inactive: { opacity: 1 } }
        }
      },
      xAxis: {
        type: 'datetime',
        lineColor: '#e5e7eb', lineWidth: 1,
        tickColor: '#9ca3af', tickWidth: 1,
        gridLineWidth: 1, gridLineColor: '#e5e7eb',
        tickInterval: 365 * 24 * 3600 * 1000,
        startOnTick: false, endOnTick: false,
        labels: { style: { fontSize: '10px', fontWeight: '500', color: '#6b7280' } }
      },
      yAxis: {
        ...yAxisRange,
        ...(isPercentageBool
          ? { startOnTick: true, endOnTick: false, tickPositions: [0, 25, 50, 75, 100] }
          : { tickAmount: 4 }),
        gridLineWidth: 1, gridLineColor: '#e5e7eb',
        title: { text: null },
        labels: {
          style: { fontSize: '10px', fontWeight: '500', color: '#6b7280' },
          formatter() {
            return isPercentageBool ? `${this.value.toFixed(0)}%` : formatUnits(this.value);
          }
        }
      }
    };

    const builders = { national: buildNationalSeries, region: buildRegionSeries, trust: buildTrustSeries };
    const buildSeries = builders[chartMode];
    return { ...base, series: buildSeries ? buildSeries(data) : [] };
  }

  function buildNationalSeries(data) {
    return [{ type: 'line', data: data.data || [], color: '#005AB5', lineWidth: 3 }];
  }

  function buildRegionSeries(data) {
    return Object.entries(data.regions || {}).map(([name, regionData]) => ({
      type: 'line',
      name,
      data: regionData.data || [],
      color: regionColors[name] || '#9ca3af',
      lineWidth: data.highlightRegion === name ? 4 : 2,
      opacity: data.highlightRegion && data.highlightRegion !== name ? 0.3 : 1,
      zIndex: data.highlightRegion === name ? 2 : 1,
      enableMouseTracking: false
    }));
  }

  function buildTrustSeries(data) {
    const series = [];
    const trustCount = data.trust_count;
    const showPercentileBands = trustCount === undefined || trustCount >= minTrustsForPercentiles;

    if (showPercentileBands) {
      for (const { lower, upper, opacity } of PERCENTILE_BANDS) {
        const lo = data.percentiles?.[lower];
        const hi = data.percentiles?.[upper];
        if (!lo?.length || !hi?.length) continue;
        const len = Math.min(lo.length, hi.length);
        const rangeData = Array.from({ length: len }, (_, i) => [lo[i][0], lo[i][1], hi[i][1]]);
        series.push({
          type: 'arearange',
          data: rangeData,
          color: '#005AB5', fillOpacity: opacity, lineWidth: 0,
          zIndex: 1, enableMouseTracking: false
        });
      }
    } else if (data.trustSeries && Object.keys(data.trustSeries).length > 0) {
      Object.entries(data.trustSeries).forEach(([name, lineData], i) => {
        series.push({
          type: 'line',
          name,
          data: lineData,
          color: chartConfig.allColours[i % chartConfig.allColours.length],
          lineWidth: 2,
          zIndex: 1,
          enableMouseTracking: false
        });
      });
    }

    // Median line: only when showing percentile bands
    const showMedian = showPercentileBands;
    if (showMedian && data.percentiles?.[50]) {
      series.push({ type: 'line', data: data.percentiles[50], color: '#DC3220', lineWidth: 2, zIndex: 2 });
    }

    // Selected trust overlay
    if (data.trustData) {
      series.push({ type: 'line', data: data.trustData, color: '#D97706', lineWidth: 3, zIndex: 3 });
    }

    return series;
  }

  onDestroy(() => {
    if (chart) {
      chart.destroy();
      chart = null;
    }
  });
</script>

<div bind:this={chartContainer} class="w-full h-full" class:opacity-50={$isLoadingCharts} class:pointer-events-none={$isLoadingCharts}></div>
