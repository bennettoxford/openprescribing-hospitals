import { writable, derived } from 'svelte/store';
import { regionColors, decilesLegend } from '../utils/chartConfig.js';

export const measureData = writable([]);
export const deciles = writable({});
export const selectedMode = writable('organisation');
export const selectedItems = writable([]);
export const usedOrganisationSelection = writable(false);

export const organisations = derived(measureData, $measureData => 
  [...new Set($measureData.map(item => item.organisation))]
);

export const regions = derived(measureData, $measureData => 
  [...new Set($measureData.map(item => item.region))]
);

export const icbs = derived(measureData, $measureData => 
  [...new Set($measureData.map(item => item.icb))]
);

export const filteredData = derived(
  [measureData, selectedMode, selectedItems, usedOrganisationSelection, organisations, regions, icbs, deciles],
  ([$measureData, $selectedMode, $selectedItems, $usedOrganisationSelection, $organisations, $regions, $icbs, $deciles]) => {
    console.log('Deriving filteredData:', {
      measureData: $measureData,
      selectedMode: $selectedMode,
      selectedItems: $selectedItems,
      usedOrganisationSelection: $usedOrganisationSelection
    });

    let labels = [];
    let datasets = [];

    if ($measureData.length === 0 && Object.keys($deciles).length === 0) {
      return { labels, datasets };
    }

    if ($selectedMode === 'deciles') {
      labels = Object.keys($deciles).sort((a, b) => new Date(a) - new Date(b));
      
      // Create datasets for each decile
      for (let i = 0; i < 27; i++) {
        let label, color, strokeWidth, strokeDasharray;
        
        if (i < 9) {
          label = `${i + 1}th Percentile`;
          color = 'blue';
          strokeWidth = 1;
          strokeDasharray = '2,2'; // Dotted blue
        } else if (i < 18 && i !== 13) {
          label = `${(i - 9 + 1) * 10}th Percentile`;
          color = 'blue';
          strokeWidth = 2;
          strokeDasharray = '4,2'; // Dashed blue
        } else if (i === 13) { // 50th percentile
          label = '50th Percentile';
          color = 'red';
          strokeWidth = 4;
          strokeDasharray = '4,2'; // Dashed red
        } else {
          label = `${i - 17 + 90}th Percentile`;
          color = 'blue';
          strokeWidth = 1;
          strokeDasharray = '2,2'; // Dotted blue
        }

        datasets.push({
          label: label,
          data: labels.map(date => $deciles[date][i] || null),
          color: color,
          strokeWidth: strokeWidth,
          strokeDasharray: strokeDasharray
        });
      }

      // Add selected organisations if any
      if ($usedOrganisationSelection && $selectedItems.length > 0) {
        const orgData = $measureData.filter(item => $selectedItems.includes(item.organisation));
        const groupedOrgData = groupBy(orgData, 'organisation');

        $selectedItems.forEach((org, index) => {
          datasets.push({
            label: org,
            data: labels.map(date => {
              const items = groupedOrgData[org]?.filter(item => item.month === date) || [];
              return items.length > 0 ? items.reduce((sum, i) => sum + parseFloat(i.quantity), 0) / items.length : null;
            }),
            color: `hsl(${index * 360 / $selectedItems.length}, 70%, 50%)`,
            strokeWidth: 3
          });
        });
      }
    } else {
      // Sort dates and create labels
      labels = [...new Set($measureData.map(item => item.month))].sort((a, b) => new Date(a) - new Date(b));

      let groupedData = {};
      let itemsToUse = $measureData;

      if ($usedOrganisationSelection && $selectedItems.length > 0) {
        itemsToUse = $measureData.filter(item => 
          $selectedMode === 'organisation' ? $selectedItems.includes(item.organisation) :
          $selectedMode === 'icb' ? $selectedItems.includes(item.icb) :
          $selectedItems.includes(item.region)
        );
      }

      itemsToUse.forEach(item => {
        const key = $selectedMode === 'organisation' ? item.organisation :
                    $selectedMode === 'icb' ? item.icb : item.region;
        if (!groupedData[key]) {
          groupedData[key] = {};
        }
        if (!groupedData[key][item.month]) {
          groupedData[key][item.month] = { sum: 0, count: 0 };
        }
        groupedData[key][item.month].sum += parseFloat(item.quantity);
        groupedData[key][item.month].count += 1;
      });

      const keys = Object.keys(groupedData);
      datasets = keys.map((key, index) => ({
        label: key,
        data: labels.map(date => {
          if (!groupedData[key][date]) return null;
          const { sum, count } = groupedData[key][date];
          return count > 0 ? sum / count : null;
        }),
        color: $selectedMode === 'region' ? regionColors[key] || `hsl(${index * 360 / keys.length}, 70%, 50%)` : `hsl(${index * 360 / keys.length}, 70%, 50%)`,
        strokeWidth: 2
      }));
    }

    console.log('Derived filteredData:', { labels, datasets });
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
