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
    { label: 'Median', color: '#DC3220', strokeWidth: 3, strokeDasharray: '4,2' },
    { label: 'Percentile', color: '#005AB5', strokeWidth: 1, strokeDasharray: '4,2' },
];

export const modeOptions = [
    { value: 'organisation', label: 'Organisation' },
    { value: 'deciles', label: 'Deciles' },
    { value: 'region', label: 'Region' },
    { value: 'icb', label: 'ICB' }
];

export const chartConfig = {
    colours: [
      '#332288',
      '#117733',
      '#44AA99',
      '#88CCEE',
      '#DDCC77',
      '#CC6677',
      '#AA4499',
      '#882255'
    ],
  };
  
  export function getOrganisationColor(organisationIndex) {
    return chartConfig.colours[organisationIndex % chartConfig.colours.length];
  }
  