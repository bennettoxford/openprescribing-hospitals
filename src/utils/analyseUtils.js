import { chartConfig } from './chartConfig.js';

const modeProcessors = {
    organisation: processOrganisationMode,
    region: processRegionMode,
    icb: processICBMode,
    total: processNationalTotalMode,
    product: processProductMode,
    productGroup: processProductGroupMode,
    unit: processUnitMode
};

let globalColorMappings = new Map();

/**
 * Create and normalise analysis data structure
 * @param {Object} data - Raw analysis data
 * @returns {Object} Normalised analysis data
 */
export function createAnalysisData(data = {}) {
    return {
        data: Array.isArray(data.data) ? data.data : [],
        percentiles: Array.isArray(data.percentiles) ? data.percentiles : [],
        searchType: typeof data.searchType === 'string' ? data.searchType : 'vmp',
        quantityType: typeof data.quantityType === 'string' ? data.quantityType : '--',
        selectedOrganisations: Array.isArray(data.selectedOrganisations) ? data.selectedOrganisations : []
    };
}

/**
 * Process chart data based on the selected mode and apply filtering
 * @param {Array} data - Raw analysis data
 * @param {Array} filteredData - Data filtered by products
 * @param {string} selectedMode - Current view mode
 * @param {Array} selectedOrganisations - Selected organisations for display
 * @param {Map} predecessorMap - Map of predecessor-successor relationships
 * @param {Array} allTrusts - All trusts in the system
 * @param {boolean} showPercentiles - Whether to show percentile data
 * @param {Array} currentPercentiles - Calculated percentiles
 * @returns {Object} Processed chart data with datasets and labels
 */
export function processChartData(data, filteredData, selectedMode, selectedOrganisations, predecessorMap, allTrusts, showPercentiles, currentPercentiles) {
    if (!Array.isArray(data)) {
        console.warn('processChartData: Invalid data array provided', { data });
        return { labels: [], datasets: [], maxValue: 0, trustsWithNoData: [] };
    }
    
    if (data.length === 0) {
        console.info('processChartData: Empty data array provided');
        return { labels: [], datasets: [], maxValue: 0, trustsWithNoData: [] };
    }

    const mode = selectedMode || 'organisation';
    const safeFilteredData = Array.isArray(filteredData) ? filteredData : [];
    const safeSelectedOrganisations = Array.isArray(selectedOrganisations) ? selectedOrganisations : [];
    const safePredecessorMap = predecessorMap instanceof Map ? predecessorMap : new Map();
    const safeAllTrusts = Array.isArray(allTrusts) ? allTrusts : [];
    const safeShowPercentiles = Boolean(showPercentiles);
    const safeCurrentPercentiles = Array.isArray(currentPercentiles) ? currentPercentiles : [];

    // Get all dates from the complete dataset
    const allDates = [...new Set((safeFilteredData.length > 0 ? safeFilteredData : data).flatMap(item => 
        Array.isArray(item.data) ? item.data.map(([date]) => date) : []
    ))].sort();

    const trustsWithNoData = calculateTrustsWithNoData(
        safeFilteredData.length > 0 ? safeFilteredData : data, 
        safeAllTrusts, 
        safePredecessorMap
    );

    // Determine which data to process based on mode
    const dataToProcess = getDataToProcess(data, safeFilteredData, mode, safeSelectedOrganisations);

    // Process data using appropriate processor
    const processor = modeProcessors[mode] || modeProcessors.organisation;
    let result;

    if (mode === 'organisation') {
        result = processor(dataToProcess, allDates, safeShowPercentiles, safeCurrentPercentiles);
    } else if (mode === 'region' || mode === 'icb') {
        result = processor(dataToProcess, allDates, safePredecessorMap);
    } else if (mode === 'total') {
        result = processor(safeFilteredData.length > 0 ? safeFilteredData : data, allDates);
    } else {
        result = processor(dataToProcess, allDates);
    }

    return {
        labels: allDates,
        datasets: result.datasets,
        maxValue: result.maxValue,
        trustsWithNoData
    };
}

/**
 * Calculate units label from data
 * @param {Array} data - Analysis data
 * @param {Array} filteredData - Filtered data
 * @returns {string} Units label
 */
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

/**
 * Update trust count breakdown with predecessor information
 * @param {Object} percentilesResult - Percentiles calculation result
 * @param {Map} predecessorMap - Predecessor mapping
 * @returns {Object} Trust count breakdown
 */
export function updateTrustCountBreakdown(percentilesResult, predecessorMap) {
    if (!percentilesResult || typeof percentilesResult.trustCount !== 'number') {
        return { current: 0, predecessors: 0, total: 0 };
    }


    return {
        current: percentilesResult.trustCount,
        total: percentilesResult.trustCount
    };
}


function calculateTrustsWithNoData(data, allTrusts, predecessorMap) {
    if (!Array.isArray(data) || !Array.isArray(allTrusts)) {
        return [];
    }

    const trustsWithData = new Set();
    
    data.forEach(item => {
        if (item && item.organisation__ods_name) {
            trustsWithData.add(item.organisation__ods_name);
        }
    });
    
    // Mark predecessors as "having data" if their successor has data
    if (predecessorMap instanceof Map) {
        for (const [successor, predecessors] of predecessorMap.entries()) {
            if (trustsWithData.has(successor) && Array.isArray(predecessors)) {
                predecessors.forEach(predecessor => {
                    trustsWithData.add(predecessor);
                });
            }
        }
    }
    
    return allTrusts
        .filter(trust => !trustsWithData.has(trust))
        .sort()
        .map(trust => ({ name: trust, code: 'Unknown' }));
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

function processOrganisationMode(dataToProcess, allDates, showPercentiles, currentPercentiles) {
    let datasets = [];

    // Add percentile datasets if enabled
    if (Array.isArray(currentPercentiles) && currentPercentiles.length > 0 && showPercentiles) {
        const percentileDatasets = createPercentileDatasets(currentPercentiles, allDates);
        datasets.push(...percentileDatasets);
    }

    // Add organisation datasets if we have data
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

function processICBMode(dataToProcess, allDates, predecessorMap) {
    if (!Array.isArray(dataToProcess) || !Array.isArray(allDates)) {
        return { datasets: [], maxValue: 0 };
    }

    const icbData = {};
    
    dataToProcess.forEach(item => {
        if (!item) return;
        
        const orgName = item.organisation__ods_name;
        let targetICB = item.organisation__icb;

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

function createPercentileDatasets(percentiles, allDates) {
    if (!Array.isArray(percentiles) || !Array.isArray(allDates)) {
        return [];
    }

    const datasets = [];

    const percentilesByValue = {};
    percentiles.forEach(p => {
        if (p && typeof p.percentile !== 'undefined') {
            if (!percentilesByValue[p.percentile]) {
                percentilesByValue[p.percentile] = [];
            }
            percentilesByValue[p.percentile].push({
                month: p.month,
                quantity: p.quantity
            });
        }
    });

    const percentileRanges = [
        { range: [45, 55], opacity: 0.8 },
        { range: [35, 65], opacity: 0.6 },
        { range: [25, 75], opacity: 0.4 },
        { range: [15, 85], opacity: 0.2 },
        { range: [5, 95], opacity: 0.1 }
    ];

    percentileRanges.forEach(({ range: [lower, upper], opacity }) => {
        if (percentilesByValue[lower] && percentilesByValue[upper]) {
            const lowerData = percentilesByValue[lower].sort((a, b) => new Date(a.month) - new Date(b.month));
            const upperData = percentilesByValue[upper].sort((a, b) => new Date(b.month) - new Date(a.month));
            
            const rangeData = allDates.map(date => {
                const lowerValue = lowerData.find(v => v.month === date);
                const upperValue = upperData.find(v => v.month === date);
                return (lowerValue && upperValue) ? { 
                    lower: lowerValue.quantity, 
                    upper: upperValue.quantity 
                } : null;
            });

            datasets.push({
                label: `${lower}th-${upper}th percentiles`,
                data: rangeData,
                color: '#005AB5',
                fillOpacity: opacity,
                strokeWidth: 0,
                fill: true,
                isRange: true,
                isPercentile: true,
                hidden: false,
                alwaysVisible: false
            });
        }
    });

    if (percentilesByValue['50']) {
        const medianData = percentilesByValue['50'].sort((a, b) => new Date(a.month) - new Date(b.month));
        
        const medianValues = allDates.map(date => {
            const value = medianData.find(v => v.month === date);
            return value ? value.quantity : null;
        });

        datasets.push({
            label: 'Median (50th percentile)',
            data: medianValues,
            color: '#DC3220',
            strokeWidth: 2,
            strokeOpacity: 1,
            fill: false,
            isPercentile: true,
            hidden: false,
            alwaysVisible: false
        });
    }

    return datasets;
}

function aggregateByRegion(aggregatedData, originalData, predecessorMap) {
    const regionData = {};
    const orgRegionMap = new Map();

    originalData.forEach(item => {
        if (!item?.organisation__ods_name || !item?.organisation__region) return;
        orgRegionMap.set(item.organisation__ods_name, item.organisation__region);
    });

    if (predecessorMap instanceof Map) {
        for (const [successor, predecessors] of predecessorMap.entries()) {
            const successorRegion = orgRegionMap.get(successor);
            if (successorRegion && Array.isArray(predecessors)) {
                predecessors.forEach(pred => orgRegionMap.set(pred, successorRegion));
            }
        }
    }

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