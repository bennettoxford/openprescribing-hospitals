export const regionColors = {
    'East Of England': '#1f77b4',
    'South East': '#ff7f0e',
    'Midlands': '#2ca02c',
    'North East And Yorkshire': '#d62728',
    'London': '#9467bd',
    'South West': '#8c564b',
    'North West': '#e377c2'
};

export const chartOptions = {
    margin: { top: 20, right: 30, bottom: 50, left: 50 },
    minHeight: 400,
    tooltipClass: 'tooltip p-2 bg-gray-800 text-white rounded shadow-lg text-sm'
};

export const decilesLegend = [
    { label: '1st-9th Percentile', style: 'border-blue-500 border-dotted' },
    { label: '10th-90th Percentile', style: 'border-blue-500 border-dashed' },
    { label: '50th Percentile', style: 'border-red-500 border-dashed' }
];

export const modeOptions = [
    { value: 'organisation', label: 'Organisation' },
    { value: 'deciles', label: 'Deciles' },
    { value: 'region', label: 'Region' },
    { value: 'icb', label: 'ICB' }
];
