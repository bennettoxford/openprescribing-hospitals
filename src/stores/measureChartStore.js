import { writable, derived } from 'svelte/store';
import { regionColors, percentilesLegend } from '../utils/chartConfig.js';

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

export function getTrustColor(index) {
  return trustColors[index % trustColors.length];
}

export function getOrganisationIndex(orgName, allOrgs) {
    return Array.isArray(allOrgs) ? 
        allOrgs.indexOf(orgName) : 
        Object.keys(allOrgs).indexOf(orgName);
}

export const filteredData = derived(
  [selectedMode, orgdata, regiondata, icbdata, percentiledata, visibleTrusts, visibleICBs, visibleRegions],
  ([$selectedMode, $orgdata, $regiondata, $icbdata, $percentiledata, $visibleTrusts, $visibleICBs, $visibleRegions]) => {
    let labels = [];
    let datasets = [];

    const sortDates = (a, b) => new Date(a) - new Date(b);

    const createDataArrayWithNulls = (data, allDates, field) => {
      const dataMap = new Map(data.map(d => [d.month, d[field]]));
      return allDates.map(date => {
        const value = dataMap.get(date);
        return value !== undefined ? value : null;
      });
    };

    switch ($selectedMode) {
      case 'trust':
        if (typeof $orgdata === 'object' && !Array.isArray($orgdata)) {
          const allDates = [...new Set(Object.values($orgdata).flatMap(org => org.map(d => d.month)))].sort(sortDates);
          labels = allDates;
          const trustNames = Object.keys($orgdata);
          datasets = trustNames
            .filter(trust => $visibleTrusts.size === 0 || $visibleTrusts.has(trust))
            .map((trust) => {
              const trustData = $orgdata[trust];
              return {
                label: trust,
                data: createDataArrayWithNulls(trustData, allDates, 'quantity'),
                numerator: createDataArrayWithNulls(trustData, allDates, 'numerator'),
                denominator: createDataArrayWithNulls(trustData, allDates, 'denominator'),
                color: getOrganisationColor(getOrganisationIndex(trust, $orgdata)),
                spanGaps: true
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
              color: regionColors[region.name] || getOrganisationColor(index)
            };
          });
        break;
      case 'icb':
        labels = $icbdata.length > 0 ? 
          $icbdata[0].data.map(d => d.month).sort(sortDates) : [];
        
        const icbIndices = new Map($icbdata.map((icb, index) => [icb.name, index]));
        
        datasets = $icbdata
          .filter(icb => $visibleICBs.size === 0 || $visibleICBs.has(icb.name))
          .map((icb) => {
            const sortedData = icb.data.sort((a, b) => sortDates(a.month, b.month));
            return {
              label: icb.name,
              data: sortedData.map(d => d.quantity),
              numerator: sortedData.map(d => d.numerator),
              denominator: sortedData.map(d => d.denominator),
              color: getOrganisationColor(icbIndices.get(icb.name))
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

        labels = Object.keys(groupedPercentiles).sort((a, b) => new Date(a) - new Date(b));

        const percentileRanges = [
          { range: [45, 55], opacity: 0.5 },
          { range: [35, 65], opacity: 0.4 },
          { range: [25, 75], opacity: 0.3 },
          { range: [15, 85], opacity: 0.2 },
          { range: [5, 95], opacity: 0.1 }
        ];

        datasets = [
          // Median line
          {
            label: 'Median (50th Percentile)',
            data: labels.map(month => groupedPercentiles[month][50] !== undefined ? groupedPercentiles[month][50] : null),
            color: '#DC3220',
            strokeWidth: 2,
            fill: false
          },
          // Shaded areas
          ...percentileRanges.map(({ range: [lower, upper], opacity }) => ({
            label: `${lower}th-${upper}th Percentile`,
            data: labels.map(month => ({
              lower: groupedPercentiles[month][lower] !== undefined ? groupedPercentiles[month][lower] : null,
              upper: groupedPercentiles[month][upper] !== undefined ? groupedPercentiles[month][upper] : null
            })),
            color: '#005AB5',
            strokeWidth: 0,
            fill: true,
            fillOpacity: opacity
          })),
          // Invisible lines for percentile boundaries
          ...percentileRanges.flatMap(({ range: [lower, upper] }) => [
            {
              label: `${lower}th Percentile`,
              data: labels.map(month => groupedPercentiles[month][lower] !== undefined ? groupedPercentiles[month][lower] : null),
              color: '#005AB5',
              strokeWidth: 1,
              strokeOpacity: 0,
              isPercentileLine: true
            },
            {
              label: `${upper}th Percentile`,
              data: labels.map(month => groupedPercentiles[month][upper] !== undefined ? groupedPercentiles[month][upper] : null),
              color: '#005AB5',
              strokeWidth: 1,
              strokeOpacity: 0,
              isPercentileLine: true
            }
          ])
        ];

        // Add selected organisations
        if ($visibleTrusts.size > 0) {
          Array.from($visibleTrusts).forEach((org, index) => {
            if ($orgdata[org]) {
              datasets.push({
                label: org,
                data: labels.map(date => {
                  const dataPoint = $orgdata[org].find(d => d.month === date);
                  return dataPoint ? dataPoint.quantity : null;
                }),
                numerator: labels.map(date => {
                  const dataPoint = $orgdata[org].find(d => d.month === date);
                  return dataPoint ? dataPoint.numerator : null;
                }),
                denominator: labels.map(date => {
                  const dataPoint = $orgdata[org].find(d => d.month === date);
                  return dataPoint ? dataPoint.denominator : null;
                }),
                color: getOrganisationColor(datasets.length),
                strokeWidth: 2,
                isOrganisation: true
              });
            }
          });
        }
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
      case 'trust':
        if (typeof $orgdata === 'object' && !Array.isArray($orgdata)) {

          const allDates = [...new Set(Object.values($orgdata).flatMap(trust => trust.map(d => d.month)))].sort(sortDates);
          labels = allDates;
          
          let trustsToShow = $visibleTrusts.size > 0 ? Array.from($visibleTrusts) : Object.keys($orgdata);
          datasets = trustsToShow.map((trust, index) => {
            const trustData = $orgdata[trust];
            if (!trustData) return null;
            return {
              label: trust,
              data: createDataArrayWithNulls(trustData, allDates),
              color: getTrustColor(index)
            };
          }).filter(Boolean);
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
