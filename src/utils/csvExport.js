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
 * Convert analysis data to CSV format
 * @param {Array} data - The analysis data
 * @param {Array} excludedVmps - List of excluded VMP codes
 * @param {Array} selectedTrusts - List of selected trust names
 * @param {Array} months - Months array
 * @returns {string} CSV formatted data
 */
export function convertToCSV(data, excludedVmps = [], selectedTrusts = null, months = []) {
    if (!data || !Array.isArray(data) || data.length === 0) {
        return '';
    }

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
        const unit = item.unit || '';

        if (item.data && Array.isArray(item.data) && months.length > 0) {
            months.forEach((date, i) => {
                const value = i < item.data.length && item.data[i] != null ? item.data[i] : '';
                rows.push([
                    date,
                    vmpCode,
                    `"${(vmpName || '').replace(/"/g, '""')}"`,
                    `"${(vtmName || '').replace(/"/g, '""')}"`,
                    trustCode,
                    `"${(trustName || '').replace(/"/g, '""')}"`,
                    `"${(region || '').replace(/"/g, '""')}"`,
                    `"${(icb || '').replace(/"/g, '""')}"`,
                    value,
                    unit,
                    `"${(ingredients || '').replace(/"/g, '""')}"`
                ].join(','));
            });
        }
    });

    return rows.join('\n');
}

/**
 * Convert trust totals to CSV format - lists all trusts with total issuing quantity over the period
 * @param {Array} data - The normalised analysis data
 * @param {Array} allOrganisations - List of {name, code, region, icb} for all trusts to include
 * @param {Array} excludedVmps - List of excluded VMP codes
 * @param {Array} months - Months array
 * @returns {string} CSV formatted trust summary
 */
export function convertTrustsSummaryToCSV(data, allOrganisations = [], excludedVmps = [], months = []) {
    if (!allOrganisations || allOrganisations.length === 0) {
        return '';
    }

    const totalsByTrust = new Map();

    (data || []).forEach(item => {
        if (excludedVmps && excludedVmps.length > 0 && excludedVmps.includes(String(item.vmp__code))) {
            return;
        }
        const orgName = item.organisation__ods_name;
        if (!orgName) return;

        const values = item.data && Array.isArray(item.data) ? item.data : [];
        const total = values.reduce((sum, v) => sum + (parseFloat(v) || 0), 0);

        if (!totalsByTrust.has(orgName)) {
            totalsByTrust.set(orgName, { total: 0, units: new Set() });
        }
        const entry = totalsByTrust.get(orgName);
        entry.total += total;
        if (item.unit) entry.units.add(item.unit);
    });

    const headers = ['Trust Code', 'Trust Name', 'Region', 'ICB', 'Total Quantity', 'Unit'];
    const rows = [headers.join(',')];

    allOrganisations.forEach(org => {
        const entry = totalsByTrust.get(org.name);
        const total = entry ? entry.total : 0;
        const unitSet = entry?.units || new Set();
        const unit = unitSet.size === 0 ? '' : unitSet.size === 1 ? [...unitSet][0] : 'multiple';
        rows.push([
            org.code || '',
            `"${(org.name || '').replace(/"/g, '""')}"`,
            `"${(org.region || '').replace(/"/g, '""')}"`,
            `"${(org.icb || '').replace(/"/g, '""')}"`,
            total,
            `"${(unit || '').replace(/"/g, '""')}"`
        ].join(','));
    });

    return rows.join('\n');
}

/**
 * Download data as compressed CSV file(s)
 * @param {string} csvContent - The main CSV content to download
 * @param {string} percentilesCsvContent - The percentiles CSV content (optional)
 * @param {string} filename - The filename for the download (optional)
 * @param {string} trustsSummaryCsvContent - Trust summary CSV (optional)
 */
export function downloadCompressedCSV(csvContent, percentilesCsvContent = null, filename = null, trustsSummaryCsvContent = null) {
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

    if (trustsSummaryCsvContent && trustsSummaryCsvContent.trim()) {
        zip.file(`${finalFilename}_trusts_summary.csv`, trustsSummaryCsvContent);
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
 * @param {Array} months - Months array
 * @param {Array} allOrganisations - Optional list of {name, code, region, icb} for trusts to include in the summary file
 */
export function exportAnalysisDataToCSV(data, excludedVmps = [], selectedTrusts = null, percentilesData = [], filename = null, months = [], allOrganisations = null) {
    const csvContent = convertToCSV(data, excludedVmps, selectedTrusts, months);
    const percentilesCsvContent = convertPercentilesToCSV(percentilesData);
    const trustsSummaryCsvContent = convertTrustsSummaryToCSV(data, allOrganisations || [], excludedVmps, months);

    if (!csvContent) {
        throw new Error('No data available for export');
    }

    downloadCompressedCSV(csvContent, percentilesCsvContent, filename, trustsSummaryCsvContent);
}
