export const regionColors = {
    'North East And Yorkshire': '#332288',
    'North West': '#117733',
    'Midlands': '#88CCEE',
    'East Of England': '#DDCC77',
    'London': '#CC6677',
    'South East': '#AA4499',
    'South West': '#882255'
};

export const chartOptions = {
    margin: { top: 20, right: 30, bottom: 50, left: 50 },
    minHeight: 400,
    tooltipClass: 'tooltip p-2 bg-gray-800 text-white rounded shadow-lg text-sm'
};

export const percentilesLegend = [
    { 
        label: 'Median (50th Percentile)', 
        color: '#DC3220',
        strokeWidth: 2.5,
        strokeDasharray: '5,5',
        isLine: true
    },
    { 
        label: '45th-55th Percentile', 
        color: '#005AB5', 
        fillOpacity: 0.8, 
        isArea: true 
    },
    { 
        label: '35th-45th and 55th-65th Percentile', 
        color: '#005AB5', 
        fillOpacity: 0.6, 
        isArea: true 
    },
    { 
        label: '25th-35th and 65th-75th Percentile', 
        color: '#005AB5', 
        fillOpacity: 0.4, 
        isArea: true 
    },
    { 
        label: '15th-25th and 75th-85th Percentile', 
        color: '#005AB5', 
        fillOpacity: 0.2, 
        isArea: true 
    },
    { 
        label: '5th-15th and 85th-95th Percentile', 
        color: '#005AB5', 
        fillOpacity: 0.1, 
        isArea: true 
    }
];

export const modeOptions = [
    { value: 'organisation', label: 'Organisation' },
    { value: 'deciles', label: 'Deciles' },
    { value: 'region', label: 'Region' },
    { value: 'icb', label: 'ICB' }
];

export const chartConfig = {
  allColours: [
    '#332288',
    '#117733',
    '#44AA99',
    '#88CCEE',
    '#DDCC77',
    '#CC6677',
    '#AA4499',
    '#882255'
  ],
  restrictedColours: [
    '#332288',
    '#117733',
    '#DDCC77',
    '#CC6677',
    '#AA4499',
    '#882255'
  ]
};

export function getOrganisationColor(organisationIndex, isOverlayingPercentiles = false) {
  const colours = isOverlayingPercentiles ? chartConfig.restrictedColours : chartConfig.allColours;
  return colours[organisationIndex % colours.length];
}
