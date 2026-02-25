import { writable, derived, get } from 'svelte/store';
import { regionColors, TRUST_OVERLAY_COLOR } from '../utils/chartConfig.js';
import { organisationSearchStore } from '../stores/organisationSearchStore';

export const orgdata = writable([]);
export const regiondata = writable([]);
export const icbdata = writable([]);
export const percentiledata = writable([]);
export const selectedMode = writable('national');
export const selectedTrusts = writable([]);
export const selectedICBs = writable([]);
export const usedOrganisationSelection = writable(false);
export const visibleRegions = writable(new Set());
export const visibleTrusts = writable(new Set());
export const visibleICBs = writable(new Set());
export const organisationColorMap = writable(new Map());
export const showPercentiles = writable(true);
export const nationaldata = writable([]);

const organisationColors = [
  '#332288', '#117733', '#44AA99', '#88CCEE', 
  '#DDCC77', '#CC6677', '#AA4499', '#882255'
];

export function getOrganisationColor(index) {
  return organisationColors[index % organisationColors.length];
}

const createDataArray = (data, allDates, field) => {
  if (!data || !Array.isArray(data)) {
    return allDates.map(() => 0);
  }

  const dataMap = new Map(data.map(d => [d.month, d[field]]));
  return allDates.map(date => dataMap.get(date) || 0);
};

export function getOrAssignColor(orgName, index = null) {
    const colorMap = get(organisationColorMap);
    if (!colorMap.has(orgName)) {
        const newColor = getOrganisationColor(index ?? colorMap.size);
        colorMap.set(orgName, newColor);
        organisationColorMap.set(colorMap);
    }
    return colorMap.get(orgName);
}

export function getDatasetVisibility(dataset, mode, visibleTrusts, showPercentiles) {
    const visibleTrustsSet = visibleTrusts instanceof Set ? visibleTrusts : new Set(visibleTrusts);
    
    if (mode === 'trust') {
        if (dataset.isPercentile) {
            return !(!showPercentiles);
        }
        if (dataset.isTrust) {
            return visibleTrustsSet.has(dataset.label);
        }
        return !dataset.hidden;
    }
    return !dataset.hidden;
}

export const filteredData = derived(
  [selectedMode, orgdata, regiondata, icbdata, percentiledata, visibleTrusts, visibleICBs, visibleRegions, showPercentiles, nationaldata],
  ([$selectedMode, $orgdata, $regiondata, $icbdata, $percentiledata, $visibleTrusts, $visibleICBs, $visibleRegions, $showPercentiles, $nationaldata]) => {
    let labels = [];
    let datasets = [];

    const sortDates = (a, b) => new Date(a) - new Date(b);

    switch ($selectedMode) {
      case 'region':
        labels = $regiondata.length > 0 ? 
          $regiondata[0].data.map(d => d.month).sort(sortDates) : [];
        datasets = $regiondata
          .filter(region => $visibleRegions.size === 0 || $visibleRegions.has(region.name))
          .map((region, index) => {
            const sortedData = region.data.sort((a, b) => sortDates(a.month, b.month));
            return {
              label: region.name,
              data: sortedData.map(d => d.quantity),
              numerator: sortedData.map(d => d.numerator),
              denominator: sortedData.map(d => d.denominator),
              color: regionColors[region.name] || getOrganisationColor(index),
              hidden: false
            };
          });
        break;
      case 'icb':
        labels = $icbdata.length > 0 ? 
          $icbdata[0].data.map(d => d.month).sort(sortDates) : [];
        
        datasets = $icbdata
          .filter(icb => $visibleICBs.size === 0 || $visibleICBs.has(icb.name))
          .map((icb) => {
            const sortedData = icb.data.sort((a, b) => sortDates(a.month, b.month));
            return {
              label: icb.name,
              data: sortedData.map(d => d.quantity),
              numerator: sortedData.map(d => d.numerator),
              denominator: sortedData.map(d => d.denominator),
              color: getOrAssignColor(icb.name),
              spanGaps: true,
              hidden: false
            };
          });
        break;
      case 'trust':
        const groupedPercentiles = $percentiledata.reduce((acc, item) => {
          if (!acc[item.month]) {
            acc[item.month] = {};
          }
          acc[item.month][item.percentile] = item.quantity;
          return acc;
        }, {});

        labels = Object.keys(groupedPercentiles).sort(sortDates);

        const percentileRanges = [
          { range: [45, 55], opacity: 0.8 },
          { range: [35, 65], opacity: 0.6 },
          { range: [25, 75], opacity: 0.4 },
          { range: [15, 85], opacity: 0.2 },
          { range: [5, 95], opacity: 0.1 }
        ];

        const visibleNonPredecessors = Array.from($visibleTrusts).filter(trust => {
            const isPredecessor = Array.from(get(organisationSearchStore).predecessorMap.entries())
                .some(([successor, predecessors]) => predecessors.includes(trust));
            return !isPredecessor;
        });

        datasets = [
          {
            label: 'Median (50th percentile)',
            data: labels.map(month => groupedPercentiles[month]?.[50] || 0),
            color: '#DC3220',
            strokeWidth: 2,
            fill: false,
            alwaysVisible: true,
            isPercentile: true,
            hidden: !$showPercentiles
          },
          ...percentileRanges.map(({ range: [lower, upper], opacity }) => ({
            label: `${lower}th-${upper}th percentiles`,
            data: labels.map(month => ({
              lower: groupedPercentiles[month]?.[lower] || 0,
              upper: groupedPercentiles[month]?.[upper] || 0
            })),
            color: '#005AB5',
            strokeWidth: 0,
            fill: true,
            fillOpacity: opacity,
            alwaysVisible: true,
            isPercentile: true,
            isRange: true,
            hidden: !$showPercentiles
          })),
          ...visibleNonPredecessors.map((org, index) => {
            const orgDataPoints = $orgdata[org]?.data?.reduce((acc, d) => {
              acc[d.month] = d;
              return acc;
            }, {}) || {};

            // First trust consistenly uses the same colour as the list overlay
            const trustColor = index === 0 ? TRUST_OVERLAY_COLOR : getOrAssignColor(org);

            return {
              label: org,
              data: labels.map(date => orgDataPoints[date]?.quantity || 0),
              numerator: labels.map(date => orgDataPoints[date]?.numerator || 0),
              denominator: labels.map(date => orgDataPoints[date]?.denominator || 0),
              color: trustColor,
              strokeWidth: 2,
              isTrust: true,
              isOrganisation: true,
              spanGaps: true,
              hidden: false
            };
          })
        ].filter(Boolean);

        const visibleNonPredecessorsSet = new Set(visibleNonPredecessors);

        datasets = datasets.map(dataset => ({
            ...dataset,
            hidden: !getDatasetVisibility(dataset, $selectedMode, visibleNonPredecessorsSet, $showPercentiles)
        }));
        break;
      case 'national':
        if ($nationaldata && $nationaldata.data && $nationaldata.data.length > 0) {
          labels = $nationaldata.data.map(d => d.month).sort(sortDates);
          
          datasets = [{
            label: 'National',
            data: labels.map(month => {
              const dataPoint = $nationaldata.data.find(d => d.month === month);
              return dataPoint ? dataPoint.quantity : 0;
            }),
            numerator: labels.map(month => {
              const dataPoint = $nationaldata.data.find(d => d.month === month);
              return dataPoint ? dataPoint.numerator : 0;
            }),
            denominator: labels.map(month => {
              const dataPoint = $nationaldata.data.find(d => d.month === month);
              return dataPoint ? dataPoint.denominator : 0;
            }),
            color: '#005AB5',
            strokeWidth: 3
          }];
        }
        break;
    }

    return { labels, datasets };
  }
);

export function updatePercentilesVisibility(showPercentiles) {
    const currentData = get(filteredData);
    if (!currentData) return currentData;

    return {
        ...currentData,
        datasets: currentData.datasets.map(dataset => ({
            ...dataset,
            hidden: !getDatasetVisibility(dataset, get(selectedMode), get(visibleTrusts), showPercentiles)
        }))
    };
}
