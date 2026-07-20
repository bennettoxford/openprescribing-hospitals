import { chartConfig } from '../../../utils/chartConfig.js';
import { normaliseDDDUnit } from '../../../utils/utils.js';
import pluralize from 'pluralize';
import { ANALYSIS_SCOPE, getNationalModeLabel } from './analysisScope.js';


export class ChartDataProcessor {
    constructor(data, aggregatedData, options = {}) {
        this.rawData = data;
        this.aggregatedData = aggregatedData;
        this.options = options;
        this.months = options.months || [];
        this.colorMappings = new Map();
        this.allDates = this.extractAllDates();
        this.uniqueUnits = this.extractUniqueUnits();
    }

    extractAllDates() {
        if (this.months?.length > 0) {
            return [...this.months];
        }
        const allDatesSet = new Set();
        Object.values(this.aggregatedData.national?.National?.products || {}).forEach(product => {
            if (product?.data) {
                Object.keys(product.data).forEach(date => allDatesSet.add(date));
            }
        });
        return [...allDatesSet].sort();
    }

    extractUniqueUnits() {
        const units = new Set();
        if (this.rawData) {
            this.rawData.forEach(item => {
                if (item.unit) units.add(item.unit);
            });
        } else {
            Object.values(this.aggregatedData.national?.National?.products || {}).forEach(product => {
                if (product?.unit) units.add(product.unit);
            });
        }
        return Array.from(units);
    }

    getCombinedUnits() {
        const formattedUnits = this.uniqueUnits.map(unit => {
            const count = this.rawData ? this.rawData.filter(item => item.unit === unit).length : 1;
            if (count > 1) {
                return pluralize(unit);
            }
            return unit;
        });
        return formattedUnits.join('/');
    }

    processMode(mode) {
        const processors = {
            trust: () => this.processOrganisationMode(),
            region: () => this.processAggregatedMode('regions'),
            icb: () => this.processAggregatedMode('icbs'),
            national: () => this.processNationalMode(),
            product: () => this.processProductMode(),
            productGroup: () => this.processProductGroupMode(),
            unit: () => this.processUnitMode(),
            ingredient: () => this.processIngredientMode()
        };

        return processors[mode]?.() || { datasets: [], maxValue: 0 };
    }

    processOrganisationMode() {
        const orgData = {};
        const selectedOrganisations = this.options.selectedOrganisations || [];
        const dataToUse = selectedOrganisations.length > 0 ?
            this.rawData.filter(item => selectedOrganisations.includes(item.organisation__ods_name)) :
            [];

        if (selectedOrganisations.length > 0) {
            selectedOrganisations.forEach(org => {
                if (org && !orgData[org]) {
                    orgData[org] = {
                        name: org,
                        data: new Array(this.allDates.length).fill(0)
                    };
                }
            });
        }

        dataToUse.forEach(item => {
            const org = item.organisation__ods_name || 'Unknown';
            if (!orgData[org]) {
                orgData[org] = {
                    name: org,
                    data: new Array(this.allDates.length).fill(0)
                };
            }
            
            this.aggregateItemData(item, orgData[org].data);
        });

        const includeEmptyTrusts = selectedOrganisations.length > 0;
        const chartResult = this.createDatasets(orgData, 'trust', false, includeEmptyTrusts);

        if (this.options.showPercentiles && this.rawData && this.rawData.length > 0) {
            chartResult.needsPercentiles = true;
            chartResult.percentilesData = this.rawData;
        }

        return chartResult;
    }

    createPercentileDatasets(percentiles) {
        if (!Array.isArray(percentiles) || percentiles.length === 0) {
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
                
                const rangeData = this.allDates.map(date => {
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
            
            const medianValues = this.allDates.map(date => {
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

    processAggregatedMode(category) {
        const categoryData = {};
        const sourceData = this.aggregatedData[category] || {};
        
        const selectedProductCodes = this.rawData && this.rawData.length > 0 
            ? new Set(this.rawData.map(item => item.vmp__code).filter(Boolean))
            : null;

        Object.entries(sourceData).forEach(([name, info]) => {
            if (info?.products) {
                categoryData[name] = {
                    data: new Array(this.allDates.length).fill(0)
                };
                
                Object.entries(info.products).forEach(([productKey, product]) => {
                    if (selectedProductCodes && !selectedProductCodes.has(productKey)) {
                        return;
                    }
                    
                    if (product?.data) {
                        Object.entries(product.data).forEach(([date, quantity]) => {
                            const dateIndex = this.allDates.indexOf(date);
                            if (dateIndex !== -1) {
                                categoryData[name].data[dateIndex] += quantity;
                            }
                        });
                    }
                });
            }
        });

        const categoryTypeMap = {
            'regions': 'region',
            'icbs': 'icb',
            'organisations': 'trust'
        };

        return this.createDatasets(categoryData, categoryTypeMap[category] || category);
    }

    processNationalMode() {
        const nationalData = new Array(this.allDates.length).fill(0);
        
        const selectedProductCodes = this.rawData && this.rawData.length > 0 
            ? new Set(this.rawData.map(item => item.vmp__code).filter(Boolean))
            : null;
        
        const nationalProducts = this.aggregatedData.national?.National?.products || {};
        Object.entries(nationalProducts).forEach(([productKey, product]) => {
            if (selectedProductCodes && !selectedProductCodes.has(productKey)) {
                return;
            }
            
            if (product?.data) {
                Object.entries(product.data).forEach(([date, quantity]) => {
                    const dateIndex = this.allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        nationalData[dateIndex] += quantity;
                    }
                });
            }
        });

        const scope = this.options.scope || ANALYSIS_SCOPE.ALL;

        return {
            datasets: [{
                label: getNationalModeLabel(scope, { longForm: true }),
                data: nationalData,
                color: '#1e40af',
                strokeOpacity: 1,
                isNational: true,
                alwaysVisible: true
            }],
            maxValue: Math.max(...nationalData) * 1.1
        };
    }

    processProductMode() {
        const productData = {};
        const selectedOrganisations = this.options.selectedOrganisations || [];
        
        const dataToUse = selectedOrganisations.length > 0 ? 
            this.rawData.filter(item => selectedOrganisations.includes(item.organisation__ods_name)) : 
            this.rawData;
        
        dataToUse.forEach(item => {
            if (item.vmp__code && item.data) {
                const productKey = item.vmp__code;
                const productName = item.vmp__name;
                
                if (!productData[productKey]) {
                    productData[productKey] = {
                        name: productName,
                        data: new Array(this.allDates.length).fill(0)
                    };
                }
                
                this.aggregateItemData(item, productData[productKey].data);
            }
        });

        return this.createDatasets(productData, 'product');
    }

    processProductGroupMode() {
        const vtmGroups = {};
        const selectedOrganisations = this.options.selectedOrganisations || [];
        
        const dataToUse = selectedOrganisations.length > 0 ? 
            this.rawData.filter(item => selectedOrganisations.includes(item.organisation__ods_name)) : 
            this.rawData;
        
        dataToUse.forEach(item => {
            if (item.vmp__vtm__name && item.data) {
                const vtm = item.vmp__vtm__name || item.vtm__name || 'Unknown';
                
                if (!vtmGroups[vtm]) {
                    vtmGroups[vtm] = {
                        data: new Array(this.allDates.length).fill(0),
                        color: this.getConsistentColor(vtm, Object.keys(vtmGroups).length)
                    };
                }
                
                this.aggregateItemData(item, vtmGroups[vtm].data);
            }
        });

        return this.createDatasets(vtmGroups, 'productGroup', true);
    }

    processUnitMode() {
        const unitData = {};
        const selectedOrganisations = this.options.selectedOrganisations || [];
        
        const dataToUse = selectedOrganisations.length > 0 ? 
            this.rawData.filter(item => selectedOrganisations.includes(item.organisation__ods_name)) : 
            this.rawData;
        
        dataToUse.forEach(item => {
            const unit = item.unit;
            if (!unit) return;
            if (!unitData[unit]) {
                unitData[unit] = {
                    data: new Array(this.allDates.length).fill(0)
                };
            }
            this.aggregateItemData(item, unitData[unit].data);
        });

        return this.createDatasets(unitData, 'unit');
    }

    processIngredientMode() {
        const ingredientData = {};
        const selectedOrganisations = this.options.selectedOrganisations || [];

        const dataToUse = selectedOrganisations.length > 0 ? 
            this.rawData.filter(item => selectedOrganisations.includes(item.organisation__ods_name)) : 
            this.rawData;
        
        dataToUse.forEach(item => {
            if (item.ingredient_names && item.data) {
                const ingredients = item.ingredient_names || ['Unknown'];
                ingredients.forEach(ingredient => {
                    if (!ingredientData[ingredient]) {
                        ingredientData[ingredient] = {
                            data: new Array(this.allDates.length).fill(0)
                        };
                    }
                    
                    this.aggregateItemData(item, ingredientData[ingredient].data, ingredients.length);
                });
            }
        });

        return this.createDatasets(ingredientData, 'ingredient');
    }

    aggregateItemData(item, targetArray, divisor = 1) {
        const data = item.data;
        if (!Array.isArray(data)) return;
        for (let i = 0; i < data.length && i < this.allDates.length; i++) {
            const numValue = parseFloat(data[i]);
            if (!isNaN(numValue)) {
                targetArray[i] = (targetArray[i] || 0) + numValue / divisor;
            }
        }
    }

    createDatasets(data, type, useCustomColor = false, includeEmptySeries = false) {
        const datasets = Object.entries(data)
            .filter(([_, { data }]) => {
                if (!data) return false;
                if (includeEmptySeries) return true;
                return data.some(v => v > 0);
            })
            .map(([key, { name, data, color }], index) => {
                return {
                    label: name || key,
                    data: data,
                    color: useCustomColor ? color : this.getConsistentColor(key, index),
                    strokeOpacity: 1,
                    [`is${type.charAt(0).toUpperCase() + type.slice(1)}`]: true
                };
            })
            .filter(Boolean);

        const maxValue = Math.max(...Object.values(data)
            .flatMap(item => item.data || [])
            .filter(v => v !== null && !isNaN(v)));

        return {
            datasets,
            maxValue: (maxValue || 0) * 1.1
        };
    }

    getConsistentColor(key, index) {
        if (!this.colorMappings.has(key)) {
            this.colorMappings.set(key, chartConfig.allColours[this.colorMappings.size % chartConfig.allColours.length]);
        }
        return this.colorMappings.get(key);
    }
}


export function shouldIncludeDate(date, period, latestDate) {
    if (period === 'all') return true;

    const dataDate = new Date(date);
    const dataDateStr = dataDate.toISOString().split('T')[0];
    const latestDateStr = latestDate ? latestDate.toISOString().split('T')[0] : null;

    switch (period) {
        case 'latest_month':
            if (!latestDate) return false;
            return dataDate.getMonth() === latestDate.getMonth() &&
                   dataDate.getFullYear() === latestDate.getFullYear();
        case 'latest_year':
            if (!latestDate) return false;
            return dataDate.getFullYear() === latestDate.getFullYear();
        case 'current_fy':
            if (!latestDate) return false;
            const fyStart = new Date(latestDate.getFullYear(), 3, 1); // April 1st
            if (latestDate < fyStart) {
                fyStart.setFullYear(fyStart.getFullYear() - 1);
            }

            const fyStartStr = fyStart.toISOString().split('T')[0];

            return dataDateStr >= fyStartStr && dataDateStr <= latestDateStr;
        case 'last_12_months':
            if (!latestDate) return false;
            const twelveMonthsAgo = new Date(latestDate);
            twelveMonthsAgo.setMonth(twelveMonthsAgo.getMonth() - 11);

            const startDateStr = twelveMonthsAgo.toISOString().split('T')[0];

            return dataDateStr >= startDateStr && dataDateStr <= latestDateStr;
        default:
            return true;
    }
}

function getDefaultUnitsForZeroTotals(data) {
    const units = new Set();
    const addUnit = (u) => {
        const n = normaliseDDDUnit(u || '');
        if (n) units.add(n);
    };

    data?.forEach(item => addUnit(item.unit));

    const arr = [...units];
    return arr.length ? arr.map(unit => ({ unit, quantity: 0 })) : null;
}

export function processTableDataByMode(data, mode, period, aggregatedData, latestDate, selectedOrganisations = [], allOrganisations = [], months = [], regionsHierarchy = [], scope = ANALYSIS_SCOPE.ALL) {
    if (!data?.length && !aggregatedData) return [];

    const defaultUnits = getDefaultUnitsForZeroTotals(data);
    const selectedProductCodes = data?.length
        ? new Set(data.map(item => item.vmp__code).filter(Boolean))
        : null;

    if (['region', 'icb', 'national'].includes(mode) && aggregatedData) {
        return processAggregatedMode(aggregatedData, mode, period, latestDate, selectedProductCodes, regionsHierarchy, defaultUnits);
    }

    if (mode === 'trust') {
        return processOrganisationModeWithAggregation(data, period, latestDate, selectedOrganisations, allOrganisations, months, defaultUnits, scope);
    }

    return processRawDataMode(data, mode, period, latestDate, selectedOrganisations, months);
}

function processOrganisationModeWithAggregation(data, period, latestDate, selectedOrganisations, allOrganisations, months = [], defaultUnits = null, scope = ANALYSIS_SCOPE.ALL) {

    const filteredData = data || [];

    const selectedSet = new Set(selectedOrganisations);
    const selectedTrusts = {};
    const otherTrusts = {};
    const selectedTotals = { total: 0, units: {} };
    const otherTotals = { total: 0, units: {} };

    allOrganisations.forEach(orgName => {
        const isSelected = selectedSet.has(orgName);
        const targetGroup = isSelected ? selectedTrusts : otherTrusts;
        targetGroup[orgName] = { total: 0, units: {} };
    });

    filteredData.forEach(item => {
        const orgName = item.organisation__ods_name || 'Unknown Organisation';
        const isSelected = selectedSet.has(orgName);
        const targetGroup = isSelected ? selectedTrusts : otherTrusts;
        const targetTotals = isSelected ? selectedTotals : otherTotals;

        if (targetGroup[orgName] !== undefined) {
            item.data?.forEach((value, i) => {
                if (i >= months.length || !shouldIncludeDate(months[i], period, latestDate)) return;
                const parsedQuantity = parseFloat(value) || 0;
                const normalisedUnit = normaliseDDDUnit(item.unit || '');

                targetGroup[orgName].total += parsedQuantity;
                if (!targetGroup[orgName].units[normalisedUnit]) {
                    targetGroup[orgName].units[normalisedUnit] = 0;
                }
                targetGroup[orgName].units[normalisedUnit] += parsedQuantity;

                targetTotals.total += parsedQuantity;
                if (!targetTotals.units[normalisedUnit]) {
                    targetTotals.units[normalisedUnit] = 0;
                }
                targetTotals.units[normalisedUnit] += parsedQuantity;
            });
        }
    });

    const results = [];

    function countTotalOrgs(trustsGroup) {
        return Object.keys(trustsGroup).length;
    }

    const toUnitsArray = (unitsObj, fallback) => {
        const entries = Object.entries(unitsObj).sort(([, a], [, b]) => b - a);
        if (entries.length === 0 && fallback?.length) {
            return fallback;
        }
        if (entries.length === 0) {
            return [{ unit: '--', quantity: 0 }];
        }
        return entries.map(([unit, quantity]) => ({ unit, quantity }));
    };

    if (Object.keys(selectedTrusts).length > 0) {
        const totalSelectedCount = countTotalOrgs(selectedTrusts);
        
        results.push({
            key: totalSelectedCount === 1
                ? 'Selected trust (1)'
                : `Selected trusts (${totalSelectedCount})`,
            total: selectedTotals.total,
            units: toUnitsArray(selectedTotals.units, defaultUnits),
            isSubtotal: true
        });

        Object.entries(selectedTrusts)
            .sort(([, a], [, b]) => b.total - a.total)
            .forEach(([trustName, trustData]) => {
                let units = Object.entries(trustData.units);
                if (units.length === 0 && defaultUnits?.length) {
                    units = defaultUnits.map(({ unit, quantity }) => [unit, quantity]);
                } else if (units.length === 0) {
                    units = [['--', 0]];
                }
                results.push({
                    key: trustName,
                    total: trustData.total,
                    units: units
                        .sort(([, a], [, b]) => b - a)
                        .map(([unit, quantity]) => ({ unit, quantity })),
                    isIndividual: true
                });
            });
    }

    if (Object.keys(otherTrusts).length > 0) {
        const totalOtherCount = countTotalOrgs(otherTrusts);
        
        const isAllTrusts = selectedOrganisations.length === 0;
        const isFilteredScope = scope === ANALYSIS_SCOPE.GROUP;
        let keyText;
        if (isAllTrusts) {
            keyText = isFilteredScope
                ? `All in-scope trusts (${totalOtherCount})`
                : `All trusts (${totalOtherCount})`;
        } else {
            keyText = isFilteredScope
                ? `All other in-scope trusts (${totalOtherCount})`
                : `All other trusts (${totalOtherCount})`;
        }
        
        results.push({
            key: keyText,
            total: otherTotals.total,
            units: toUnitsArray(otherTotals.units, defaultUnits),
            isSubtotal: true
        });

        Object.entries(otherTrusts)
            .sort(([, a], [, b]) => b.total - a.total)
            .forEach(([trustName, trustData]) => {
                let units = Object.entries(trustData.units);
                if (units.length === 0 && defaultUnits?.length) {
                    units = defaultUnits.map(({ unit, quantity }) => [unit, quantity]);
                } else if (units.length === 0) {
                    units = [['--', 0]];
                }
                results.push({
                    key: trustName,
                    total: trustData.total,
                    units: units
                        .sort(([, a], [, b]) => b - a)
                        .map(([unit, quantity]) => ({ unit, quantity })),
                    isIndividual: true
                });
            });
    }

    return results;
}

function processAggregatedMode(aggregatedData, mode, period, latestDate, selectedProductCodes = null, regionsHierarchy = [], defaultUnits = null) {
    const categoryMap = {
        'region': 'regions',
        'icb': 'icbs',
        'national': 'national'
    };
    const categoryData = aggregatedData[categoryMap[mode]] || {};
    const resultsMap = new Map();

    Object.entries(categoryData).forEach(([name, info]) => {
        if (!info?.products) return;

        const totals = { total: 0, units: {} };

        Object.entries(info.products).forEach(([productKey, product]) => {
            if (selectedProductCodes && !selectedProductCodes.has(productKey)) {
                return;
            }

            if (!product?.data) return;

            const unit = product.unit || '';
            const normalisedUnit = normaliseDDDUnit(unit);

            Object.entries(product.data).forEach(([date, quantity]) => {
                if (!shouldIncludeDate(date, period, latestDate)) return;

                const parsedQuantity = parseFloat(quantity) || 0;
                totals.total += parsedQuantity;

                if (!totals.units[normalisedUnit]) {
                    totals.units[normalisedUnit] = 0;
                }
                totals.units[normalisedUnit] += parsedQuantity;
            });
        });

        let units = Object.entries(totals.units)
            .sort(([, a], [, b]) => b - a)
            .map(([unit, quantity]) => ({ unit, quantity }));
        if (units.length === 0 && defaultUnits?.length) {
            units = defaultUnits;
        } else if (units.length === 0) {
            units = [{ unit: '--', quantity: 0 }];
        }
        resultsMap.set(name, { key: name, total: totals.total, units });
    });

    const zeroUnits = defaultUnits?.length ? defaultUnits : [{ unit: '--', quantity: 0 }];
    let allCategoryNames = [];
    if (mode === 'region' && regionsHierarchy.length > 0) {
        allCategoryNames = regionsHierarchy.map(r => r.region).filter(Boolean);
    } else if (mode === 'icb' && regionsHierarchy.length > 0) {
        allCategoryNames = regionsHierarchy.flatMap(r => (r.icbs || []).map(i => i.name)).filter(Boolean);
    } else if (mode === 'national') {
        allCategoryNames = ['National'];
    }
    allCategoryNames.forEach(name => {
        if (!resultsMap.has(name)) {
            resultsMap.set(name, { key: name, total: 0, units: zeroUnits });
        }
    });

    return Array.from(resultsMap.values()).sort((a, b) => b.total - a.total);
}

function processRawDataMode(data, mode, period, latestDate, selectedOrganisations = [], months = []) {
    if (!data?.length) return [];

    const dataToProcess = selectedOrganisations.length > 0 
        ? data.filter(item => selectedOrganisations.includes(item.organisation__ods_name))
        : data;

    const groupedData = {};

    const singleUnitPerGroup = mode === 'product' || mode === 'unit';

    dataToProcess.forEach(item => {
        const groupKey = getGroupKey(item, mode);
        
        if (Array.isArray(groupKey)) {
            groupKey.forEach(key => processItemForGroup(groupedData, key, item, months, period, latestDate, singleUnitPerGroup));
        } else {
            processItemForGroup(groupedData, groupKey, item, months, period, latestDate, singleUnitPerGroup);
        }
    });

    return Object.entries(groupedData)
        .map(([key, value]) => {
            const units = value.unit != null
                ? [{ unit: value.unit, quantity: value.total }]
                : Object.entries(value.units || {})
                    .sort(([, a], [, b]) => b - a)
                    .map(([unit, quantity]) => ({ unit, quantity }));
            return { key, total: value.total, units };
        })
        .sort((a, b) => b.total - a.total);
}

function getGroupKey(item, mode) {
    switch (mode) {
        case 'trust':
            return item.organisation__ods_name || 'Unknown Organisation';
        case 'product':
            return item.vmp__name || 'Unknown Product';
        case 'productGroup':
            return item.vmp__vtm__name || 'Unknown Product Group';
        case 'ingredient':
            return item.ingredient_names?.length > 0 ? item.ingredient_names : ['Unknown Ingredient'];
        case 'unit':
            return item.unit ? [item.unit] : [];
        default:
            return item.vmp__name || 'Unknown';
    }
}

function processItemForGroup(groupedData, groupKey, item, months = [], period, latestDate, singleUnitPerGroup = false) {
    if (!groupedData[groupKey]) {
        groupedData[groupKey] = singleUnitPerGroup
            ? { total: 0, unit: '' }
            : { total: 0, units: {} };
    }

    const g = groupedData[groupKey];
    item.data?.forEach((value, i) => {
        if (i >= months.length || !shouldIncludeDate(months[i], period, latestDate)) return;
        const parsedQuantity = parseFloat(value) || 0;
        g.total += parsedQuantity;

        if (singleUnitPerGroup) {
            if (!g.unit && item.unit) g.unit = normaliseDDDUnit(item.unit) || item.unit;
        } else {
            const normalisedUnit = normaliseDDDUnit(item.unit || '');
            g.units[normalisedUnit] = (g.units[normalisedUnit] || 0) + parsedQuantity;
        }
    });
}

export function processAnalysisData(months, items, selectedOrganisations = []) {
    if (!Array.isArray(items)) {
        console.error('Invalid data format:', items);
        return {
            productData: {},
            organisationData: {},
            aggregatedData: { regions: {}, icbs: {}, national: {} }
        };
    }

    const organisationItems = items.filter(item => item.organisation__ods_code);
    const baseVmpItems = items.filter(item => !item.organisation__ods_code);
    const nationalOnlyItems = organisationItems.length === 0
        ? baseVmpItems.filter(item => Array.isArray(item.data) && item.data.length > 0)
        : [];

    const groupedData = {
        organisationItems,
        baseVmpItems,
        nationalOnlyItems
    };

    const results = {
        productData: {},
        organisationData: {},
        aggregatedData: { regions: {}, icbs: {}, national: {} }
    };

    groupedData.baseVmpItems.forEach(item => {
        if (item.vmp__code && !results.productData[item.vmp__code]) {
            results.productData[item.vmp__code] = {
                code: item.vmp__code,
                name: item.vmp__name,
                vtm: item.vmp__vtm__name,
                ingredients: item.ingredient_names || [],
                ingredient_codes: item.ingredient_codes || [],
                organisations: {}
            };
        }
    });

    function denseToTimeSeries(item) {
        const data = item.data;
        const unit = item.unit || '';
        if (!Array.isArray(months) || !Array.isArray(data)) return [];
        return months.map((date, i) => ({
            date,
            quantity: (i < data.length && data[i] != null) ? parseFloat(data[i]) || 0 : 0,
            unit
        }));
    }

    groupedData.organisationItems.forEach(item => {
        if (!item.vmp__code) return;

        const productKey = item.vmp__code;
        const timeSeriesData = denseToTimeSeries(item);

        const targetRegion = item.organisation__region;
        const targetICB = item.organisation__icb;

        if (targetRegion) {
            aggregateByCategory(results.aggregatedData.regions, targetRegion, productKey, timeSeriesData);
        }

        if (targetICB) {
            aggregateByCategory(results.aggregatedData.icbs, targetICB, productKey, timeSeriesData);
        }

        aggregateByCategory(results.aggregatedData.national, 'National', productKey, timeSeriesData);

        const isSelectedTrust = selectedOrganisations.length === 0 || 
                              selectedOrganisations.includes(item.organisation__ods_name);

        if (isSelectedTrust) {
            if (!results.productData[productKey]) {
                results.productData[productKey] = {
                    code: item.vmp__code,
                    name: item.vmp__name,
                    vtm: item.vmp__vtm__name,
                    ingredients: item.ingredient_names || [],
                    ingredient_codes: item.ingredient_codes || [],
                    organisations: {}
                };
            }

            const orgKey = item.organisation__ods_code;
            results.productData[productKey].organisations[orgKey] = {
                ods_code: item.organisation__ods_code,
                ods_name: item.organisation__ods_name,
                region: item.organisation__region,
                icb: item.organisation__icb,
                data: timeSeriesData
            };

            if (!results.organisationData[orgKey]) {
                results.organisationData[orgKey] = {
                    ods_code: item.organisation__ods_code,
                    ods_name: item.organisation__ods_name,
                    region: item.organisation__region,
                    icb: item.organisation__icb,
                    products: {}
                };
            }
            results.organisationData[orgKey].products[productKey] = {
                code: item.vmp__code,
                name: item.vmp__name,
                vtm: item.vmp__vtm__name,
                ingredients: item.ingredient_names || [],
                ingredient_codes: item.ingredient_codes || [],
                data: timeSeriesData
            };
        }
    });

    groupedData.nationalOnlyItems.forEach(item => {
        if (!item.vmp__code) return;

        const productKey = item.vmp__code;
        const timeSeriesData = denseToTimeSeries(item);
        aggregateByCategory(results.aggregatedData.national, 'National', productKey, timeSeriesData);
    });

    return results;
}

function aggregateByCategory(categoryData, categoryName, productKey, timeSeriesData) {
    if (!categoryData[categoryName]) {
        categoryData[categoryName] = {
            name: categoryName,
            products: {}
        };
    }

    if (!categoryData[categoryName].products[productKey]) {
        const firstUnit = timeSeriesData[0]?.unit || '';
        categoryData[categoryName].products[productKey] = {
            data: {},
            unit: firstUnit
        };
    }

    const product = categoryData[categoryName].products[productKey];
    timeSeriesData.forEach(({ date, quantity }) => {
        product.data[date] = (product.data[date] || 0) + quantity;
    });
}

export function calculatePercentiles(data, allTrusts = [], months = []) {
    if (!Array.isArray(data) || data.length === 0) {
        return { percentiles: [], trustCount: 0, excludedTrusts: [] };
    }

    const orgTotalUsage = {};
    const orgNames = {};
    const allMonths = new Set();
    const excludedTrusts = [];
    const seenOrgNames = new Set();

    data.forEach(item => {
        const orgId = item.organisation__ods_code;
        const orgName = item.organisation__ods_name;
        
        if (!orgId) {
            return;
        }

        if (orgName) {
            seenOrgNames.add(orgName);
            orgNames[orgId] = orgName;
        }
        
        if (!orgTotalUsage[orgId]) {
            orgTotalUsage[orgId] = 0;
        }

        let hasValidData = false;
        const dataArr = item.data;
        if (Array.isArray(dataArr)) {
            dataArr.forEach((qty, i) => {
                const month = months[i];
                const quantity = parseFloat(qty);
                if (month && !isNaN(quantity)) {
                    orgTotalUsage[orgId] += quantity;
                    allMonths.add(month);
                    hasValidData = true;
                }
            });
        }

        if (!hasValidData && orgName) {
            const existingExcluded = excludedTrusts.find(t => t.name === orgName);
            if (!existingExcluded) {
                excludedTrusts.push({
                    name: orgName,
                    code: orgId,
                    reason: 'No valid data for selected products'
                });
            }
        }
    });

    Object.keys(orgTotalUsage).forEach(orgId => {
        if (orgTotalUsage[orgId] === 0) {
            delete orgTotalUsage[orgId];
        }
    });

    if (Array.isArray(allTrusts) && allTrusts.length > 0) {
        allTrusts.forEach(trustName => {
            if (!seenOrgNames.has(trustName)) {
                excludedTrusts.push({
                    name: trustName,
                    code: 'Unknown',
                    reason: 'Not in analysis data (no matching products or quantity type)'
                });
            }
        });
    }

    const includedOrgs = Object.keys(orgTotalUsage);

    if (includedOrgs.length === 0) {
        return { 
            percentiles: [], 
            trustCount: 0, 
            excludedTrusts: []
        };
    }

    const orgMonthlyValues = {};
    const sortedMonths = Array.from(allMonths).sort();

    includedOrgs.forEach(orgId => {
        orgMonthlyValues[orgId] = {};
        sortedMonths.forEach(month => {
            orgMonthlyValues[orgId][month] = 0; // Set missing months to 0 - this is in line with the percentiles calculation in measures
        });
    });

    data.forEach(item => {
        const orgId = item.organisation__ods_code;
        
        if (orgId && includedOrgs.includes(orgId)) {
            const dataArr = item.data;
            if (Array.isArray(dataArr)) {
                dataArr.forEach((qty, i) => {
                    const month = months[i];
                    const quantity = parseFloat(qty);
                    if (month && orgMonthlyValues[orgId][month] !== undefined && !isNaN(quantity)) {
                        orgMonthlyValues[orgId][month] += quantity;
                    }
                });
            }
        }
    });

    const orgsForRanking = Object.keys(orgMonthlyValues);
    const trustCount = orgsForRanking.length;

    const percentilesData = [];
    
    sortedMonths.forEach(month => {
        const monthValues = orgsForRanking.map(orgId => orgMonthlyValues[orgId][month]).sort((a, b) => a - b);
        const n = monthValues.length;

        if (n > 0) {
            [5, 15, 25, 35, 45, 50, 55, 65, 75, 85, 95].forEach(percentile => {
                const k = (n - 1) * (percentile / 100);
                const f = Math.floor(k);
                const c = Math.ceil(k);

                let value;
                if (f === c) {
                    value = monthValues[f];
                } else {
                    const d0 = monthValues[f] * (c - k);
                    const d1 = monthValues[c] * (k - f);
                    value = d0 + d1;
                }

                percentilesData.push({
                    month,
                    percentile,
                    quantity: value
                });
            });
        }
    });

    return {
        percentiles: percentilesData,
        trustCount: trustCount,
        excludedTrusts: excludedTrusts
    };
}

export function getTrustCount(percentilesResult) {
    if (!percentilesResult || typeof percentilesResult.trustCount !== 'number') {
        return 0;
    }
    return percentilesResult.trustCount;
}


export class ViewModeCalculator {
    constructor(resultsStore, analyseOptions, organisationSearchStore, vmps, scope = ANALYSIS_SCOPE.ALL) {
        this.resultsStore = resultsStore;
        this.analyseOptions = analyseOptions;
        this.organisationSearchStore = organisationSearchStore;
        this.vmps = vmps;
        this.scope = scope;
    }

    calculateAvailableModes() {
        const modes = [];
        
        // Only add modes if there are VMPs with valid data
        if (this.vmps && this.vmps.length > 0) {
            if (this.scope === ANALYSIS_SCOPE.NATIONAL) {
                modes.push({ value: 'national', label: getNationalModeLabel(this.scope) });
                modes.push(...this.getProductModes());
                return modes;
            }

            modes.push({ value: 'trust', label: 'NHS Trust' });
            
            modes.push(...this.getAggregationModes());
            
            modes.push(...this.getProductModes());
        }
        
        return modes;
    }

    hasSelectedOrganisations() {
        return this.analyseOptions.selectedOrganisations && 
               this.analyseOptions.selectedOrganisations.length > 0;
    }

    hasSelectedOrganisationsWithData() {
        if (!this.hasSelectedOrganisations()) return false;

        const selectedOrgNames = new Set(this.analyseOptions.selectedOrganisations);

        const selectedOrganisationsWithData = this.resultsStore.analysisData
            ?.filter(item => {
                const orgName = item.organisation__ods_name;
                const isSelected = selectedOrgNames.has(orgName);
                const hasData = item.data && Array.isArray(item.data) &&
                    item.data.some(v => v > 0 && !isNaN(parseFloat(v)));
                return isSelected && hasData;
            }) || [];

        return selectedOrganisationsWithData.length >= 1;
    }

    getAggregationModes() {
        const modes = [];

        if (!this.resultsStore.aggregatedData) return modes;
        
        const { regions = {}, icbs = {}, national = {} } = this.resultsStore.aggregatedData;
        
        if (icbs && Object.keys(icbs).length > 1) {
            modes.push({ value: 'icb', label: 'ICB' });
        }
        
        if (regions && Object.keys(regions).length > 1) {
            modes.push({ value: 'region', label: 'Region' });
        }
        
        if (
            this.scope !== ANALYSIS_SCOPE.TRUST &&
            national &&
            Object.keys(national).length > 0
        ) {
            modes.push({ value: 'national', label: getNationalModeLabel(this.scope) });
        }

        if (this.resultsStore.analysisData) {
            const mappedICBs = new Set(
                this.resultsStore.analysisData.map(item => item.organisation__icb).filter(Boolean)
            );

            if (mappedICBs.size > 1 && !modes.some(m => m.value === 'icb')) {
                modes.push({ value: 'icb', label: 'ICB' });
            }

            const uniqueRegions = new Set(this.resultsStore.analysisData.map(item => item.organisation__region).filter(Boolean));
            if (uniqueRegions.size > 1 && !modes.some(m => m.value === 'region')) {
                modes.push({ value: 'region', label: 'Region' });
            }
        }
        
        return modes;
    }

    getProductModes() {
        const modes = [];
        
        if (!this.vmps || this.vmps.length === 0) return modes;

        if (this.vmps.length > 1) {
            modes.push({ value: 'product', label: 'Product' });
        }

        const uniqueVtms = new Set(this.vmps.map(vmp => vmp.vtm).filter(vtm => vtm && vtm !== '-' && vtm !== 'nan'));
        if (uniqueVtms.size > 1) {
            modes.push({ value: 'productGroup', label: 'Product Group' });
        }

        const uniqueIngredients = new Set(
            this.vmps.flatMap(vmp => (vmp.ingredients || []))
                .filter(ing => ing && ing !== '-' && ing !== 'nan')
        );
        if (uniqueIngredients.size > 1) {
            modes.push({ value: 'ingredient', label: 'Ingredient' });
        }

        const uniqueUnits = new Set(
            this.vmps.flatMap(vmp => Array.from(vmp.units || []))
                .filter(unit => unit && unit !== '-' && unit !== 'nan')
                .map(unit => normaliseDDDUnit(unit))
        );
        if (uniqueUnits.size > 1) {
            modes.push({ value: 'unit', label: 'Unit' });
        }

        return modes;
    }
}
