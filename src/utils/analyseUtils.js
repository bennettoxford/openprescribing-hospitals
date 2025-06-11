import { chartConfig } from './chartConfig.js';

/**
 * Process chart data based on the selected mode and apply filtering
 * @param {Array} data - Raw analysis data
 * @param {Array} filteredData - Data filtered by products
 * @param {string} selectedMode - Current view mode
 * @param {Array} selectedOrganisations - Selected organisations for display
 * @param {Map} predecessorMap - Map of predecessor-successor relationships
 * @returns {Object} Processed chart data with datasets and labels
 */
export function processChartData(data, filteredData, selectedMode, selectedOrganisations, predecessorMap) {
    if (!Array.isArray(data)) {
        return { labels: [], datasets: [], maxValue: 0};
    }

    // Default to 'organisation' mode if selectedMode is null or undefined
    const mode = selectedMode || 'organisation';

    const safeFilteredData = Array.isArray(filteredData) ? filteredData : [];
    const safeSelectedOrganisations = Array.isArray(selectedOrganisations) ? selectedOrganisations : [];
    const safePredecessorMap = predecessorMap instanceof Map ? predecessorMap : new Map();

    // Get all dates from the complete dataset
    const allDates = [...new Set((safeFilteredData.length > 0 ? safeFilteredData : data).flatMap(item => 
        Array.isArray(item.data) ? item.data.map(([date]) => date) : []
    ))].sort();

    // Determine which data to process based on mode
    const dataToProcess = getDataToProcess(data, safeFilteredData, mode, safeSelectedOrganisations);

    let datasets = [];
    let maxValue = 0;

    // Process data based on mode
    switch (mode) {
        case 'organisation':
            const orgResult = processOrganisationMode(dataToProcess, allDates);
            datasets = orgResult.datasets;
            maxValue = orgResult.maxValue;
            break;
        case 'region':
            const regionResult = processRegionMode(dataToProcess, allDates, safePredecessorMap);
            datasets = regionResult.datasets;
            maxValue = regionResult.maxValue;
            break;
        case 'icb':
            const icbResult = processICBMode(dataToProcess, allDates, safePredecessorMap);
            datasets = icbResult.datasets;
            maxValue = icbResult.maxValue;
            break;
        case 'total':
            const totalResult = processNationalTotalMode(safeFilteredData.length > 0 ? safeFilteredData : data, allDates);
            datasets = totalResult.datasets;
            maxValue = totalResult.maxValue;
            break;
        case 'product':
            const productResult = processProductMode(dataToProcess, allDates);
            datasets = productResult.datasets;
            maxValue = productResult.maxValue;
            break;
        case 'productGroup':
            const groupResult = processProductGroupMode(dataToProcess, allDates);
            datasets = groupResult.datasets;
            maxValue = groupResult.maxValue;
            break;
        case 'unit':
            const unitResult = processUnitMode(dataToProcess, allDates);
            datasets = unitResult.datasets;
            maxValue = unitResult.maxValue;
            break;
        default:
            const defaultResult = processOrganisationMode(dataToProcess, allDates);
            datasets = defaultResult.datasets;
            maxValue = defaultResult.maxValue;
    }

    return {
        labels: allDates,
        datasets,
        maxValue
    };
}

function getDataToProcess(data, filteredData, selectedMode, selectedOrganisations) {
    if (!Array.isArray(data)) {
        return [];
    }

    const safeFilteredData = Array.isArray(filteredData) ? filteredData : [];
    const safeSelectedOrganisations = Array.isArray(selectedOrganisations) ? selectedOrganisations : [];

    // For organisation mode, only return data for selected organisations
    if (selectedMode === 'organisation') {
        const dataToUse = safeFilteredData.length > 0 ? safeFilteredData : data;
        // Only filter by selected organisations if there are any selected
        if (safeSelectedOrganisations.length > 0) {
            return dataToUse.filter(item => 
                item && safeSelectedOrganisations.includes(item.organisation__ods_name)
            );
        }
        // If no organizations selected, return empty array
        return [];
    } else {
        return safeFilteredData.length > 0 ? safeFilteredData : data;
    }
}

function processOrganisationMode(dataToProcess, allDates) {
    let datasets = [];
    
    // Only process data if we have data and it's not empty
    if (Array.isArray(dataToProcess) && dataToProcess.length > 0) {
        const { aggregatedData } = preAggregateData(dataToProcess);
        const trustDatasets = createTrustDatasetsFromAggregated(aggregatedData, allDates);
        datasets.push(...trustDatasets);
    }

    const maxValue = calculateMaxValue(datasets);
    return { datasets, maxValue };
}

function processRegionMode(dataToProcess, allDates, predecessorMap) {
    if (!Array.isArray(dataToProcess) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const { aggregatedData } = preAggregateData(dataToProcess);
    const regionData = aggregateByRegion(aggregatedData, dataToProcess, predecessorMap);

    const datasets = Object.entries(regionData)
        .filter(([region, dateMap]) => dateMap.size > 0)
        .map(([region, dateMap], index) => ({
            label: region,
            data: allDates.map(date => dateMap.get(date) || 0),
            color: getConsistentColor(region, index),
            strokeOpacity: 1,
            isRegion: true
        }));

    const maxValue = Math.max(...datasets.flatMap(d => d.data).filter(v => !isNaN(v)), 0);
    return { datasets, maxValue };
}

function aggregateByRegion(aggregatedData, originalData, predecessorMap) {
    const regionData = {};
    const orgRegionMap = new Map();

    // Build org to region mapping
    originalData.forEach(item => {
        if (!item?.organisation__ods_name || !item?.organisation__region) return;
        orgRegionMap.set(item.organisation__ods_name, item.organisation__region);
    });

    // Handle predecessor mapping
    if (predecessorMap instanceof Map) {
        for (const [successor, predecessors] of predecessorMap.entries()) {
            const successorRegion = orgRegionMap.get(successor);
            if (successorRegion && Array.isArray(predecessors)) {
                predecessors.forEach(pred => orgRegionMap.set(pred, successorRegion));
            }
        }
    }

    // Aggregate by region
    Object.entries(aggregatedData).forEach(([orgId, dateMap]) => {
        const region = orgRegionMap.get(orgId);
        if (!region) return;

        if (!regionData[region]) {
            regionData[region] = new Map();
        }

        dateMap.forEach((value, date) => {
            const currentValue = regionData[region].get(date) || 0;
            regionData[region].set(date, currentValue + value);
        });
    });

    return regionData;
}


function processICBMode(dataToProcess, allDates, predecessorMap) {
    if (!Array.isArray(dataToProcess) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const icbData = {};
    
    dataToProcess.forEach(item => {
        if (!item) return;
        
        const orgName = item.organisation__ods_name;
        let targetICB = item.organisation__icb;

        // Handle predecessor mapping
        if (predecessorMap instanceof Map) {
            for (const [successor, predecessors] of predecessorMap.entries()) {
                if (Array.isArray(predecessors) && predecessors.includes(orgName)) {
                    const successorData = dataToProcess.find(d => d && d.organisation__ods_name === successor);
                    if (successorData) {
                        targetICB = successorData.organisation__icb;
                    }
                    break;
                }
            }
        }
        
        if (!targetICB) return;
        
        if (!icbData[targetICB]) {
            icbData[targetICB] = {
                data: new Array(allDates.length).fill(0)
            };
        }
        
        if (Array.isArray(item.data)) {
            item.data.forEach(([date, value]) => {
                const dateIndex = allDates.indexOf(date);
                if (dateIndex !== -1) {
                    const numValue = parseFloat(value);
                    if (!isNaN(numValue)) {
                        icbData[targetICB].data[dateIndex] += numValue;
                    }
                }
            });
        }
    });

    const datasets = Object.entries(icbData)
        .filter(([_, { data }]) => Array.isArray(data) && data.some(v => v > 0))
        .map(([icb, { data }], index) => ({
            label: icb,
            data: data,
            color: getConsistentColor(icb, index),
            strokeOpacity: 1,
            isICB: true
        }));

    const maxValue = Math.max(...Object.values(icbData)
        .flatMap(icb => Array.isArray(icb.data) ? icb.data : [])
        .filter(v => v !== null && !isNaN(v) && v > 0), 0);

    return { datasets, maxValue };
}

function processNationalTotalMode(allData, allDates) {
    if (!Array.isArray(allData) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const datasets = [{
        label: 'National Total',
        data: allDates.map(date => {
            const total = allData.reduce((sum, item) => {
                if (!item || !Array.isArray(item.data)) return sum;
                const dateData = item.data.find(([d]) => d === date);
                return sum + (dateData ? parseFloat(dateData[1]) || 0 : 0);
            }, 0);
            return total;
        }),
        color: '#1e40af',
        strokeOpacity: 1,
        alwaysVisible: true
    }];

    const maxValue = Math.max(...datasets[0].data.filter(v => !isNaN(v)), 0);
    return { datasets, maxValue };
}

function processProductMode(dataToProcess, allDates) {
    if (!Array.isArray(dataToProcess) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const productData = {};
    
    dataToProcess.forEach(item => {
        if (!item) return;
        
        const productKey = item.vmp__code;
        const productName = item.vmp__name;
        
        if (!productKey) return;
        
        if (!productData[productKey]) {
            productData[productKey] = {
                name: productName,
                data: new Array(allDates.length).fill(0)
            };
        }
        
        if (Array.isArray(item.data)) {
            item.data.forEach(([date, value]) => {
                const dateIndex = allDates.indexOf(date);
                if (dateIndex !== -1) {
                    const numValue = parseFloat(value);
                    if (!isNaN(numValue)) {
                        productData[productKey].data[dateIndex] += numValue;
                    }
                }
            });
        }
    });

    const datasets = Object.entries(productData)
        .filter(([_, { data }]) => Array.isArray(data) && data.some(v => v > 0))
        .map(([code, { name, data }], index) => ({
            label: name,
            data: data,
            color: getConsistentColor(code, index),
            strokeOpacity: 1,
            isProduct: true
        }));

    const maxValue = Math.max(...Object.values(productData)
        .flatMap(product => Array.isArray(product.data) ? product.data : [])
        .filter(v => v !== null && !isNaN(v) && v > 0), 0);

    return { datasets, maxValue };
}

function processProductGroupMode(dataToProcess, allDates) {
    if (!Array.isArray(dataToProcess) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const vtmGroups = {};
    
    dataToProcess.forEach(item => {
        if (!item) return;
        
        const vtm = item.vmp__vtm__name || item.vtm__name || 'Unknown';

        if (!vtmGroups[vtm]) {
            vtmGroups[vtm] = {
                data: new Array(allDates.length).fill(0),
                color: getConsistentColor(vtm, Object.keys(vtmGroups).length)
            };
        }
        
        if (Array.isArray(item.data)) {
            item.data.forEach(([date, value]) => {
                const dateIndex = allDates.indexOf(date);
                if (dateIndex !== -1) {
                    vtmGroups[vtm].data[dateIndex] += parseFloat(value) || 0;
                }
            });
        }
    });

    const datasets = Object.entries(vtmGroups)
        .filter(([_, data]) => Array.isArray(data.data) && data.data.some(v => v > 0))
        .map(([vtm, data]) => ({
            label: vtm,
            data: data.data,
            color: data.color,
            strokeOpacity: 1,
            isProductGroup: true
        }));

    const maxValue = Math.max(...datasets.flatMap(d => Array.isArray(d.data) ? d.data : []).filter(v => !isNaN(v) && v > 0), 0);
    return { datasets, maxValue };
}


function processUnitMode(dataToProcess, allDates) {
    if (!Array.isArray(dataToProcess) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const unitData = {};
    
    dataToProcess.forEach(item => {
        if (!item || !Array.isArray(item.data)) return;
        
        item.data.forEach(([date, value, unit]) => {
            if (!unit) return;
            
            if (!unitData[unit]) {
                unitData[unit] = {
                    data: new Array(allDates.length).fill(0)
                };
            }
            
            const dateIndex = allDates.indexOf(date);
            if (dateIndex !== -1) {
                const numValue = parseFloat(value);
                if (!isNaN(numValue)) {
                    unitData[unit].data[dateIndex] += numValue;
                }
            }
        });
    });

    const datasets = Object.entries(unitData)
        .filter(([_, { data }]) => Array.isArray(data) && data.some(v => v > 0))
        .map(([unit, { data }], index) => ({
            label: unit,
            data: data,
            color: getConsistentColor(unit, index),
            strokeOpacity: 1,
            isUnit: true
        }));

    const maxValue = Math.max(...Object.values(unitData)
        .flatMap(unit => Array.isArray(unit.data) ? unit.data : [])
        .filter(v => v !== null && !isNaN(v) && v > 0), 0);

    return { datasets, maxValue };
}

function calculateMaxValue(datasets) {
    if (!Array.isArray(datasets) || datasets.length === 0) {
        return 0;
    }

    return Math.max(
        ...datasets.flatMap(d => {
            if (!d || !Array.isArray(d.data)) return [0];
            
            if (d.isRange) {
                return d.data.filter(v => v !== null && v !== undefined).flatMap(v => [v.lower, v.upper]).filter(v => !isNaN(v));
            } else {
                return d.data.filter(v => v !== null && !isNaN(v));
            }
        }),
        0 // Fallback to 0 if no valid values
    );
}

export function calculateUnits(data, filteredData) {
    if (!Array.isArray(data)) {
        return 'units';
    }

    const uniqueUnits = new Set();
    const allData = Array.isArray(filteredData) && filteredData.length > 0 ? filteredData : data;
    
    allData.forEach(item => {
        if (item && Array.isArray(item.data)) {
            item.data.forEach(([_, __, unit]) => {
                if (unit && unit !== 'nan') uniqueUnits.add(unit);
            });
        }
    });
    
    const formattedUnits = Array.from(uniqueUnits).map(unit => {
        const count = allData.reduce((acc, item) => {
            if (!item || !Array.isArray(item.data)) return acc;
            return acc + item.data.filter(([_, __, u]) => u === unit).length;
        }, 0);
        
        if (count > 1) {
            if (unit.endsWith('y')) {
                return unit.slice(0, -1) + 'ies';
            } else if (!unit.endsWith('s')) {
                return unit + 's';
            }
        }
        return unit;
    });
    
    return formattedUnits.join('/') || 'units';
}

let globalColorMappings = new Map();

function getOrganisationColor(index) {
    return chartConfig.allColours[index % chartConfig.allColours.length];
}

function getConsistentColor(key, index) {
    if (!globalColorMappings.has(key)) {
        globalColorMappings.set(key, getOrganisationColor(globalColorMappings.size));
    }
    return globalColorMappings.get(key);
}

function getTrustColor(org, index) {
    return getConsistentColor(org, index);
}

/**
 * Pre-aggregate data by organisation and date
 * @param {Array} data - Raw analysis data
 * @returns {Object} Aggregated data organized by org and date
 */
function preAggregateData(data) {
    if (!Array.isArray(data)) {
        return { aggregatedData: {}, allDates: new Set() };
    }

    const aggregatedByOrgAndDate = {};
    const allDates = new Set();

    data.forEach(item => {
        if (!item || !item.organisation__ods_name || !Array.isArray(item.data)) return;

        const orgId = item.organisation__ods_name;
        if (!aggregatedByOrgAndDate[orgId]) {
            aggregatedByOrgAndDate[orgId] = new Map();
        }

        item.data.forEach(([date, value, unit]) => {
            if (!date || !value || unit === 'nan') return;

            const numValue = parseFloat(value);
            if (isNaN(numValue)) return;

            allDates.add(date);
            const currentValue = aggregatedByOrgAndDate[orgId].get(date) || 0;
            aggregatedByOrgAndDate[orgId].set(date, currentValue + numValue);
        });
    });

    return { aggregatedData: aggregatedByOrgAndDate, allDates };
}

/**
 * Create trust datasets from pre-aggregated data
 */
function createTrustDatasetsFromAggregated(aggregatedData, allDates) {
    if (!aggregatedData || !Array.isArray(allDates)) {
        return [];
    }

    return Object.entries(aggregatedData)
        .filter(([_, dateMap]) => dateMap.size > 0)
        .map(([org, dateMap], index) => ({
            label: org,
            data: allDates.map(date => dateMap.get(date) || 0),
            color: getTrustColor(org, index),
            strokeOpacity: 1,
            strokeWidth: 2,
            spanGaps: true,
            isOrganisation: true,
            isTrust: true
        }));
} 