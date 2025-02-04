import { writable, derived, get } from 'svelte/store';
import { regionColors, percentilesLegend } from '../utils/chartConfig.js';
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

const organisationColors = [
  '#332288', '#117733', '#44AA99', '#88CCEE', 
  '#DDCC77', '#CC6677', '#AA4499', '#882255'
];

export function getOrganisationColor(index) {
  return organisationColors[index % organisationColors.length];
}

const trustColors = [
  '#332288', '#117733', '#44AA99', '#88CCEE', 
  '#DDCC77', '#CC6677', '#AA4499', '#882255'
];



export function getOrganisationIndex(orgName, allOrgs) {
    return Array.isArray(allOrgs) ? 
        allOrgs.indexOf(orgName) : 
        Object.keys(allOrgs).indexOf(orgName);
}

const createDataArrayWithNulls = (data, allDates, field) => {
  if (!data || !Array.isArray(data)) {
    return allDates.map(() => null);
  }

  const dataMap = new Map(data.map(d => [d.month, d[field]]));
  return allDates.map(date => {
    const value = dataMap.get(date);
    return value !== undefined ? value : null;
  });
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
    
    if (mode === 'percentiles') {
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
  [selectedMode, orgdata, regiondata, icbdata, percentiledata, visibleTrusts, visibleICBs, visibleRegions, showPercentiles],
  ([$selectedMode, $orgdata, $regiondata, $icbdata, $percentiledata, $visibleTrusts, $visibleICBs, $visibleRegions, $showPercentiles]) => {
    let labels = [];
    let datasets = [];

    const sortDates = (a, b) => new Date(a) - new Date(b);

    switch ($selectedMode) {
      case 'trust':
        if (typeof $orgdata === 'object' && !Array.isArray($orgdata)) {
          const allDates = [...new Set(Object.values($orgdata).flatMap(org => {
            return org.data && Array.isArray(org.data) ? org.data.map(d => d.month) : [];
          }))].sort(sortDates);
          
          labels = allDates;
          datasets = Array.from($visibleTrusts).map((trust) => {
            const trustData = $orgdata[trust]?.data || [];
            return {
              label: trust,
              data: createDataArrayWithNulls(trustData, allDates, 'quantity'),
              numerator: createDataArrayWithNulls(trustData, allDates, 'numerator'),
              denominator: createDataArrayWithNulls(trustData, allDates, 'denominator'),
              color: getOrAssignColor(trust),
              spanGaps: true,
              hidden: false,
              isTrust: true
            };
          });
        }
        break;
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
      case 'percentiles':
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
            label: 'Median (50th Percentile)',
            data: labels.map(month => groupedPercentiles[month]?.[50] ?? null),
            color: '#DC3220',
            strokeWidth: 2,
            fill: false,
            alwaysVisible: true,
            isPercentile: true,
            hidden: !$showPercentiles
          },
          ...percentileRanges.map(({ range: [lower, upper], opacity }) => ({
            label: `${lower}th-${upper}th Percentile`,
            data: labels.map(month => ({
              lower: groupedPercentiles[month]?.[lower] ?? null,
              upper: groupedPercentiles[month]?.[upper] ?? null
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
          ...visibleNonPredecessors.map(org => {
            const orgDataPoints = $orgdata[org]?.data?.reduce((acc, d) => {
              acc[d.month] = d;
              return acc;
            }, {}) || {};

            return {
              label: org,
              data: labels.map(date => orgDataPoints[date]?.quantity ?? null),
              numerator: labels.map(date => orgDataPoints[date]?.numerator ?? null),
              denominator: labels.map(date => orgDataPoints[date]?.denominator ?? null),
              color: getOrAssignColor(org),
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
        if ($regiondata.length > 0) {
          labels = $regiondata[0].data.map(d => d.month).sort(sortDates);
          
          datasets = [{
            label: 'National',
            data: labels.map(month => {
              const dataPoint = $regiondata[0].data.find(d => d.month === month);
              return dataPoint ? dataPoint.quantity : null;
            }),
            numerator: labels.map(month => {
              const dataPoint = $regiondata[0].data.find(d => d.month === month);
              return dataPoint ? dataPoint.numerator : null;
            }),
            denominator: labels.map(month => {
              const dataPoint = $regiondata[0].data.find(d => d.month === month);
              return dataPoint ? dataPoint.denominator : null;
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

export const legendItems = derived(selectedMode, $selectedMode => {
  if ($selectedMode === 'deciles') {
    return decilesLegend;
  } else if ($selectedMode === 'region') {
    return Object.entries(regionColors).map(([region, color]) => ({
      label: region,
      style: `background-color: ${color}`
    }));
  }
  return [];
});

function groupBy(array, key) {
  return array.reduce((result, currentValue) => {
    (result[currentValue[key]] = result[currentValue[key]] || []).push(currentValue);
    return result;
  }, {});
}

export function formatTooltipValue(value, numerator, denominator) {
    if (value === null || value === undefined) return 'No data';
    
    let formattedValue = value.toFixed(1) + '%';
    if (numerator !== undefined && denominator !== undefined) {
        formattedValue += ` (${numerator}/${denominator})`;
    }
    return formattedValue;
}

export function getTooltipContent(d) {
    const date = new Date(d.date);
    const formattedDate = date.toLocaleString('en-GB', { month: 'long', year: 'numeric' });
    const value = formatTooltipValue(d.value * 100, d.dataset.numerator?.[d.index], d.dataset.denominator?.[d.index]);
    
    return `
        <div class="font-medium">${d.dataset.label}</div>
        <div class="text-sm text-gray-600">${formattedDate}</div>
        <div class="text-sm">${value}</div>
    `;
}

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
