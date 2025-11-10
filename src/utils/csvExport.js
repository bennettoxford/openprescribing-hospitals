import JSZip from 'jszip';

/**
 * Convert percentiles data to CSV format
 * @param {Array} percentilesData - The percentiles data from results store
 * @returns {string} CSV formatted percentiles data
 */
export function convertPercentilesToCSV(percentilesData) {
    if (!percentilesData || !Array.isArray(percentilesData) || percentilesData.length === 0) {
        return '';
    }

    const percentilesByMonth = {};

    percentilesData.forEach(item => {
        const month = item.month || '';
        if (!percentilesByMonth[month]) {
            percentilesByMonth[month] = [];
        }
        percentilesByMonth[month].push(item);
    });

    const headers = ['Date', '5th Percentile', '15th Percentile', '25th Percentile', '35th Percentile', '45th Percentile', '50th Percentile', '55th Percentile', '65th Percentile', '75th Percentile', '85th Percentile', '95th Percentile'];

    const rows = [headers.join(',')];

    const sortedMonths = Object.keys(percentilesByMonth).sort();

    sortedMonths.forEach(month => {
        const monthPercentiles = percentilesByMonth[month];

        const monthValues = [month];

        [5, 15, 25, 35, 45, 50, 55, 65, 75, 85, 95].forEach(percentile => {
            const percentileData = monthPercentiles.find(p => p.percentile === percentile);
            const value = percentileData ? percentileData.quantity : '';
            monthValues.push(value);
        });

        rows.push(monthValues.join(','));
    });

    return rows.join('\n');
}

/**
 * Normalise data by aggregating predecessor trusts into their successors
 * @param {Array} data - The raw analysis data
 * @param {Map} predecessorMap - Map of successor -> predecessors
 * @returns {Array} Normalised data with predecessors aggregated into successors
 */
function normalisePredecessors(data, predecessorMap) {
    if (!predecessorMap || predecessorMap.size === 0) {
        return data; // No predecessors to normalise
    }

    if (!Array.isArray(data)) {
        console.warn('Invalid data format for predecessor normalisation:', data);
        return data;
    }

    const normalisedData = new Map(); // Key: normalised org key, Value: aggregated data item

    data.forEach(item => {
        if (!item.organisation__ods_code || !item.vmp__code) {
            return;
        }

        const orgName = item.organisation__ods_name;
        let normalisedOrgName = orgName;
        let normalisedOrgCode = item.organisation__ods_code;
        let normalisedRegion = item.organisation__region;
        let normalisedIcb = item.organisation__icb;

        for (const [successor, predecessors] of predecessorMap.entries()) {
            if (predecessors && Array.isArray(predecessors) && predecessors.includes(orgName)) {
                const successorData = data.find(d => d.organisation__ods_name === successor);
                if (successorData) {
                    normalisedOrgName = successor;
                    normalisedOrgCode = successorData.organisation__ods_code;
                    normalisedRegion = successorData.organisation__region;
                    normalisedIcb = successorData.organisation__icb;
                }
                break;
            }
        }

        const normalisedKey = `${normalisedOrgCode}_${item.vmp__code}`;

        if (normalisedData.has(normalisedKey)) {
  
            const existingItem = normalisedData.get(normalisedKey);

            const existingDataMap = new Map();
            if (existingItem.data && Array.isArray(existingItem.data)) {
                existingItem.data.forEach(([date, quantity, unit]) => {
                    if (date && (quantity !== null && quantity !== undefined)) {
                        existingDataMap.set(date, { quantity: parseFloat(quantity) || 0, unit: unit || '' });
                    }
                });
            }

            if (item.data && Array.isArray(item.data)) {
                item.data.forEach(([date, quantity, unit]) => {
                    if (date && (quantity !== null && quantity !== undefined)) {
                        const parsedQuantity = parseFloat(quantity) || 0;
                        if (existingDataMap.has(date)) {
                            existingDataMap.get(date).quantity += parsedQuantity;
                        } else {
                            existingDataMap.set(date, { quantity: parsedQuantity, unit: unit || '' });
                        }
                    }
                });
            }

            existingItem.data = Array.from(existingDataMap.entries())
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([date, { quantity, unit }]) => [date, quantity, unit]);

        } else {
            const normalisedItem = {
                ...item,
                organisation__ods_name: normalisedOrgName,
                organisation__ods_code: normalisedOrgCode,
                organisation__region: normalisedRegion,
                organisation__icb: normalisedIcb,
            };

            if (normalisedItem.data && Array.isArray(normalisedItem.data)) {
                normalisedItem.data.sort(([a], [b]) => a.localeCompare(b));
            }

            normalisedData.set(normalisedKey, normalisedItem);
        }
    });

    return Array.from(normalisedData.values());
}

/**
 * Convert analysis data to CSV format
 * @param {Array} data - The analysis data
 * @param {Array} excludedVmps - List of excluded VMP codes
 * @param {Array} selectedTrusts - List of selected trust names
 * @param {Map} predecessorMap - Map of successor -> predecessors for normalisation
 * @returns {string} CSV formatted data
 */
export function convertToCSV(data, excludedVmps = [], selectedTrusts = null, predecessorMap = null) {
    if (!data || !Array.isArray(data) || data.length === 0) {
        return '';
    }

    // Filter data based on excluded VMPs and selected trusts
    const filteredData = data.filter(item => {
        
        if (excludedVmps && excludedVmps.length > 0) {
            if (excludedVmps.includes(String(item.vmp__code))) {
                return false;
            }
        }

        if (selectedTrusts && selectedTrusts.length > 0) {
            if (!selectedTrusts.includes(item.organisation__ods_name)) {
                return false;
            }
        }

        return true;
    });

    if (filteredData.length === 0) {
        return '';
    }

    const headers = [
        'Date',
        'VMP Code',
        'VMP Name',
        'VTM Name',
        'Trust Code',
        'Trust Name',
        'Region',
        'ICB',
        'Value',
        'Unit',
        'Ingredients'
    ];

    const rows = [headers.join(',')];

    filteredData.forEach(item => {
        const vmpCode = item.vmp__code || '';
        const vmpName = item.vmp__name || '';
        const vtmName = item.vmp__vtm__name || '';
        const trustCode = item.organisation__ods_code || '';
        const trustName = item.organisation__ods_name || '';
        const region = item.organisation__region || '';
        const icb = item.organisation__icb || '';
        const ingredients = Array.isArray(item.ingredient_names) ? item.ingredient_names.join('; ') : '';

        if (item.data && Array.isArray(item.data)) {
            item.data.forEach(dataPoint => {
                if (dataPoint && dataPoint.length >= 2) {
                    const date = dataPoint[0] || '';
                    const value = dataPoint[1] !== null && dataPoint[1] !== undefined ? dataPoint[1] : '';
                    const unit = dataPoint[2] || '';

                    const escapedRow = [
                        date,
                        vmpCode,
                        `"${vmpName.replace(/"/g, '""')}"`,
                        `"${vtmName.replace(/"/g, '""')}"`,
                        trustCode,
                        `"${trustName.replace(/"/g, '""')}"`,
                        `"${region.replace(/"/g, '""')}"`,
                        `"${icb.replace(/"/g, '""')}"`,
                        value,
                        unit,
                        `"${ingredients.replace(/"/g, '""')}"`
                    ];

                    rows.push(escapedRow.join(','));
                }
            });
        }
    });

    return rows.join('\n');
}

/**
 * Download data as compressed CSV file(s)
 * @param {string} csvContent - The main CSV content to download
 * @param {string} percentilesCsvContent - The percentiles CSV content (optional)
 * @param {string} filename - The filename for the download (optional)
 */
export function downloadCompressedCSV(csvContent, percentilesCsvContent = null, filename = null) {
    if (!csvContent) {
        throw new Error('No CSV content to download');
    }

    const timestamp = new Date().toISOString().split('T')[0];
    const defaultFilename = `analysis_${timestamp}`;
    const finalFilename = filename || defaultFilename;

    const zip = new JSZip();

    zip.file(`${finalFilename}_data.csv`, csvContent);

    if (percentilesCsvContent && percentilesCsvContent.trim()) {
        zip.file(`${finalFilename}_percentiles.csv`, percentilesCsvContent);
    }

    zip.generateAsync({ type: 'blob', compression: 'DEFLATE' })
        .then(function (blob) {
            const link = document.createElement('a');
            if (link.download !== undefined) {
                const url = URL.createObjectURL(blob);
                link.setAttribute('href', url);
                link.setAttribute('download', `${finalFilename}.zip`);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                URL.revokeObjectURL(url);
            } else {
                throw new Error('ZIP download not supported in this browser');
            }
        })
        .catch(function (error) {
            console.error('Error creating ZIP file:', error);
            throw new Error('Failed to create download');
        });
}

/**
 * Main function to export analysis data to compressed CSV and trigger download
 * @param {Array} data - The analysis data
 * @param {Array} excludedVmps - List of excluded VMP codes
 * @param {Array} selectedTrusts - List of selected trust names
 * @param {Array} percentilesData - The percentiles data
 * @param {string} filename - Optional filename for the download
 * @param {Map} predecessorMap - Map of successor -> predecessors for normalisation
 */
export function exportAnalysisDataToCSV(data, excludedVmps = [], selectedTrusts = null, percentilesData = [], filename = null, predecessorMap = null) {
    const csvContent = convertToCSV(data, excludedVmps, selectedTrusts, predecessorMap);
    const percentilesCsvContent = convertPercentilesToCSV(percentilesData);

    if (!csvContent) {
        throw new Error('No data available for export');
    }

    downloadCompressedCSV(csvContent, percentilesCsvContent, filename);
}
