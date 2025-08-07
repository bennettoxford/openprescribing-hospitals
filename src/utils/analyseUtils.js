import { chartConfig } from './chartConfig.js';
import pluralize from 'pluralize';

export class ChartDataProcessor {
    constructor(data, aggregatedData, options = {}) {
        this.rawData = data;
        this.aggregatedData = aggregatedData;
        this.options = options;
        this.colorMappings = new Map();
        this.allDates = this.extractAllDates();
        this.uniqueUnits = this.extractUniqueUnits();
    }

    extractAllDates() {
        if (this.rawData) {
            return [...new Set(this.rawData.flatMap(item => 
                item.data ? item.data.map(([date]) => date) : []
            ))].sort();
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
                if (item.data) {
                    item.data.forEach(([_, __, unit]) => {
                        if (unit) units.add(unit);
                    });
                }
            });
        } else {
            Object.values(this.aggregatedData.national?.National?.products || {}).forEach(product => {
                if (product?.data) {
                    Object.values(product.data).forEach(({ unit }) => {
                        if (unit) units.add(unit);
                    });
                }
            });
        }
        return Array.from(units);
    }

    getCombinedUnits() {
        const formattedUnits = this.uniqueUnits.map(unit => {
            const count = this.rawData ? this.rawData.reduce((acc, item) => {
                return acc + (item.data ? item.data.filter(([_, __, u]) => u === unit).length : 0);
            }, 0) : 1;
            
            if (count > 1) {
                return pluralize(unit);
            }
            return unit;
        });
        
        return formattedUnits.join('/');
    }

    processMode(mode) {
        const processors = {
            organisation: () => this.processOrganisationMode(),
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

        dataToUse.forEach(item => {
            const org = item.organisation__ods_name || 'Unknown';
            if (!orgData[org]) {
                orgData[org] = {
                    name: org,
                    data: new Array(this.allDates.length).fill(0),
                    isPredecessor: false
                };
            }
            
            this.aggregateItemData(item, orgData[org].data);
        });

        this.handlePredecessors(orgData);

        const chartResult = this.createDatasets(orgData, 'organisation');

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

        Object.entries(sourceData).forEach(([name, info]) => {
            if (info?.products) {
                categoryData[name] = {
                    data: new Array(this.allDates.length).fill(0)
                };
                
                Object.values(info.products).forEach(product => {
                    if (product?.data) {
                        Object.entries(product.data).forEach(([date, { quantity }]) => {
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
            'organisations': 'organisation'
        };

        return this.createDatasets(categoryData, categoryTypeMap[category] || category);
    }

    processNationalMode() {
        const nationalData = new Array(this.allDates.length).fill(0);
        
        const nationalProducts = this.aggregatedData.national?.National?.products || {};
        Object.values(nationalProducts).forEach(product => {
            if (product?.data) {
                Object.entries(product.data).forEach(([date, { quantity }]) => {
                    const dateIndex = this.allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        nationalData[dateIndex] += quantity;
                    }
                });
            }
        });

        return {
            datasets: [{
                label: 'National Total',
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
        
        this.rawData.forEach(item => {
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
        
        this.rawData.forEach(item => {
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
        
        this.rawData.forEach(item => {
            if (item.data) {
                item.data.forEach(([date, value, unit]) => {
                    if (!unit) return;
                    
                    if (!unitData[unit]) {
                        unitData[unit] = {
                            data: new Array(this.allDates.length).fill(0)
                        };
                    }
                    
                    const dateIndex = this.allDates.indexOf(date);
                    if (dateIndex !== -1) {
                        const numValue = parseFloat(value);
                        if (!isNaN(numValue)) {
                            unitData[unit].data[dateIndex] += numValue;
                        }
                    }
                });
            }
        });

        return this.createDatasets(unitData, 'unit');
    }

    processIngredientMode() {
        const ingredientData = {};
        
        this.rawData.forEach(item => {
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
        item.data.forEach(([date, value]) => {
            const dateIndex = this.allDates.indexOf(date);
            if (dateIndex !== -1) {
                const numValue = parseFloat(value);
                if (!isNaN(numValue)) {
                    targetArray[dateIndex] += numValue / divisor;
                }
            }
        });
    }

    handlePredecessors(orgData) {
        if (this.options.predecessorMap) {
            for (const [org, orgInfo] of Object.entries(orgData)) {
                for (const [successor, predecessors] of this.options.predecessorMap.entries()) {
                    if (predecessors.includes(org)) {
                        orgData[org].isPredecessor = true;
            
                        if (orgData[successor]) {
                            orgData[successor].data = orgData[successor].data.map(
                                (value, index) => value + (orgData[org].data[index] || 0)
                            );
                        }
                    }
                }
            }
        }
    }

    createDatasets(data, type, useCustomColor = false) {
        const datasets = Object.entries(data)
            .filter(([_, { data }]) => data && data.some(v => v > 0))
            .map(([key, { name, data, color, isPredecessor }], index) => {
                if (isPredecessor) return null;
                
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
            .filter(item => !item.isPredecessor)
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

export class ViewModeCalculator {
    constructor(resultsStore, analyseOptions, organisationSearchStore, vmps) {
        this.resultsStore = resultsStore;
        this.analyseOptions = analyseOptions;
        this.organisationSearchStore = organisationSearchStore;
        this.vmps = vmps;
    }

    calculateAvailableModes() {
        const modes = [];
        
        modes.push({ value: 'organisation', label: 'NHS Trust' });
        
        modes.push(...this.getAggregationModes());
        
        modes.push(...this.getProductModes());
        
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
                const isPredecessor = Array.from(this.organisationSearchStore.predecessorMap.values())
                    .some(predecessors => predecessors.includes(orgName));
                
                return isSelected && 
                       !isPredecessor && 
                       item.data && 
                       item.data.some(([_, value]) => value && !isNaN(parseFloat(value)));
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
        
        if (national && Object.keys(national).length > 0) {
            modes.push({ value: 'national', label: 'National' });
        }

        if (this.resultsStore.isAdvancedMode && this.resultsStore.analysisData) {
            const mappedICBs = new Set(
                this.resultsStore.analysisData.map(item => {
                    const orgName = item.organisation__ods_name;
                    let targetICB = item.organisation__icb;
                    
                    for (const [successor, predecessors] of this.organisationSearchStore.predecessorMap.entries()) {
                        if (predecessors.includes(orgName)) {
                            const successorData = this.resultsStore.analysisData.find(d => d.organisation__ods_name === successor);
                            if (successorData) {
                                targetICB = successorData.organisation__icb;
                            }
                            break;
                        }
                    }
                    return targetICB;
                }).filter(Boolean)
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
        );
        if (uniqueUnits.size > 1) {
            modes.push({ value: 'unit', label: 'Unit' });
        }

        return modes;
    }
}

export function selectDefaultMode(availableModes, hasSelectedOrganisations = false) {
    const organisationMode = availableModes.find(m => m.value === 'organisation');
    if (organisationMode) return organisationMode.value;
    
    const nationalMode = availableModes.find(m => m.value === 'national');
    if (nationalMode) return nationalMode.value;
    
    return availableModes[0]?.value || 'organisation';
}

export function processAnalysisData(data, selectedOrganisations = [], predecessorMap = new Map()) {
    if (!Array.isArray(data)) {
        console.error('Invalid data format:', data);
        return {
            productData: {},
            organisationData: {},
            aggregatedData: { regions: {}, icbs: {}, national: {} }
        };
    }

    const groupedData = {
        organisationItems: data.filter(item => item.organisation__ods_code),
        baseVmpItems: data.filter(item => !item.organisation__ods_code)
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

    groupedData.organisationItems.forEach(item => {
        if (!item.vmp__code) return;

        const productKey = item.vmp__code;
        const timeSeriesData = Array.isArray(item.data) 
            ? item.data
                .filter(([date, quantity]) => date && quantity)
                .map(([date, quantity, unit]) => ({
                    date,
                    quantity: parseFloat(quantity) || 0,
                    unit
                }))
            : [];

        // Determine target region and ICB for aggregation
        const orgName = item.organisation__ods_name;
        let targetRegion = item.organisation__region;
        let targetICB = item.organisation__icb;

        for (const [successor, predecessors] of predecessorMap.entries()) {
            if (predecessors.includes(orgName)) {
                const successorData = data.find(d => d.organisation__ods_name === successor);
                if (successorData) {
                    targetRegion = successorData.organisation__region;
                    targetICB = successorData.organisation__icb;
                }
                break;
            }
        }

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
        categoryData[categoryName].products[productKey] = {
            data: {}
        };
    }

    timeSeriesData.forEach(({ date, quantity, unit }) => {
        if (!categoryData[categoryName].products[productKey].data[date]) {
            categoryData[categoryName].products[productKey].data[date] = {
                quantity: 0,
                unit: unit
            };
        }
        categoryData[categoryName].products[productKey].data[date].quantity += quantity;
    });
}

export function shouldIncludeDate(date, period, latestDate) {
    if (period === 'all') return true;

    const dataDate = new Date(date);
    
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
            return dataDate >= fyStart && dataDate <= latestDate;
        default:
            return true;
    }
}

export function processTableDataByMode(data, mode, period, aggregatedData, latestDate, selectedOrganisations = [], allOrganisations = [], predecessorMap = new Map(), expandedTrusts = new Set()) {
    if (!data?.length && !aggregatedData) return [];

    if (['region', 'icb', 'national'].includes(mode) && aggregatedData) {
        return processAggregatedMode(aggregatedData, mode, period, latestDate);
    }

    if (mode === 'organisation') {
        return processOrganisationModeWithAggregation(data, period, latestDate, selectedOrganisations, allOrganisations, predecessorMap, expandedTrusts);
    }

    return processRawDataMode(data, mode, period, latestDate);
}

function processOrganisationModeWithAggregation(data, period, latestDate, selectedOrganisations, allOrganisations, predecessorMap, expandedTrusts) {

    const filteredData = data ? data.map(item => ({
        ...item,
        data: item.data ? item.data.filter(([date]) => 
            shouldIncludeDate(date, period, latestDate)) : []
    })) : [];

    const selectedSet = new Set(selectedOrganisations);
    const selectedTrusts = {};
    const otherTrusts = {};
    const selectedTotals = { total: 0, units: {} };
    const otherTotals = { total: 0, units: {} };

    const predecessorToSuccessor = new Map();
    for (const [successor, predecessors] of predecessorMap.entries()) {
        predecessors.forEach(pred => {
            predecessorToSuccessor.set(pred, successor);
        });
    }

    const allPredecessors = new Set(Array.from(predecessorMap.values()).flat());

    const mainOrganisations = allOrganisations.filter(org => !allPredecessors.has(org));

    mainOrganisations.forEach(orgName => {
        const isSelected = selectedSet.has(orgName);
        const targetGroup = isSelected ? selectedTrusts : otherTrusts;
        
        targetGroup[orgName] = { 
            total: 0, 
            units: {},
            predecessors: {}
        };

        const predecessors = predecessorMap.get(orgName) || [];
        predecessors.forEach(predName => {
            targetGroup[orgName].predecessors[predName] = {
                total: 0,
                units: {}
            };
        });
    });

    filteredData.forEach(item => {
        const orgName = item.organisation__ods_name || 'Unknown Organisation';

        let targetOrgName = orgName;
        let isPredecessor = false;
        
        if (predecessorToSuccessor.has(orgName)) {
            targetOrgName = predecessorToSuccessor.get(orgName);
            isPredecessor = true;
        }

        const isSelected = selectedSet.has(targetOrgName);
        const targetGroup = isSelected ? selectedTrusts : otherTrusts;
        const targetTotals = isSelected ? selectedTotals : otherTotals;

        if (targetGroup[targetOrgName] !== undefined) {
            item.data?.forEach(([date, quantity, unit]) => {
                const parsedQuantity = parseFloat(quantity) || 0;
                
                if (isPredecessor) {
                    targetGroup[targetOrgName].predecessors[orgName].total += parsedQuantity;
                    if (!targetGroup[targetOrgName].predecessors[orgName].units[unit]) {
                        targetGroup[targetOrgName].predecessors[orgName].units[unit] = 0;
                    }
                    targetGroup[targetOrgName].predecessors[orgName].units[unit] += parsedQuantity;
                }
                
                targetGroup[targetOrgName].total += parsedQuantity;
                if (!targetGroup[targetOrgName].units[unit]) {
                    targetGroup[targetOrgName].units[unit] = 0;
                }
                targetGroup[targetOrgName].units[unit] += parsedQuantity;

                targetTotals.total += parsedQuantity;
                if (!targetTotals.units[unit]) {
                    targetTotals.units[unit] = 0;
                }
                targetTotals.units[unit] += parsedQuantity;
            });
        }
    });

    const results = [];

    function countTotalOrgs(trustsGroup) {
        let count = Object.keys(trustsGroup).length;
        Object.values(trustsGroup).forEach(trust => {
            count += Object.keys(trust.predecessors || {}).length;
        });
        return count;
    }

    if (Object.keys(selectedTrusts).length > 0) {
        const totalSelectedCount = countTotalOrgs(selectedTrusts);
        
        results.push({
            key: `Selected trusts (${totalSelectedCount})`,
            total: selectedTotals.total,
            units: Object.entries(selectedTotals.units)
                .sort(([, a], [, b]) => b - a)
                .map(([unit, quantity]) => ({ unit, quantity })),
            isSubtotal: true
        });

        Object.entries(selectedTrusts)
            .sort(([, a], [, b]) => b.total - a.total)
            .forEach(([trustName, trustData]) => {
                const hasPredecessors = Object.keys(trustData.predecessors).length > 0;
                const isExpanded = expandedTrusts.has(trustName);
                
                let units = Object.entries(trustData.units);
                if (units.length === 0) {
                    units = [['--', 0]];
                }
                
                results.push({
                    key: trustName,
                    total: trustData.total,
                    units: units
                        .sort(([, a], [, b]) => b - a)
                        .map(([unit, quantity]) => ({ unit, quantity })),
                    isIndividual: true,
                    hasPredecessors,
                    isExpanded
                });

                if (hasPredecessors && isExpanded) {
                    Object.entries(trustData.predecessors)
                        .sort(([, a], [, b]) => b.total - a.total)
                        .forEach(([predName, predData]) => {
                            let predUnits = Object.entries(predData.units);
                            if (predUnits.length === 0) {
                                predUnits = [['--', 0]];
                            }
                            
                            results.push({
                                key: predName,
                                total: predData.total,
                                units: predUnits
                                    .sort(([, a], [, b]) => b - a)
                                    .map(([unit, quantity]) => ({ unit, quantity })),
                                isPredecessor: true
                            });
                        });
                }
            });
    }

    if (Object.keys(otherTrusts).length > 0) {
        const totalOtherCount = countTotalOrgs(otherTrusts);
        
        const isAllTrusts = selectedOrganisations.length === 0;
        const keyText = isAllTrusts ? `All trusts (${totalOtherCount})` : `All other trusts (${totalOtherCount})`;
        
        results.push({
            key: keyText,
            total: otherTotals.total,
            units: Object.entries(otherTotals.units)
                .sort(([, a], [, b]) => b - a)
                .map(([unit, quantity]) => ({ unit, quantity })),
            isSubtotal: true
        });

        Object.entries(otherTrusts)
            .sort(([, a], [, b]) => b.total - a.total)
            .forEach(([trustName, trustData]) => {
                const hasPredecessors = Object.keys(trustData.predecessors).length > 0;
                const isExpanded = expandedTrusts.has(trustName);
                
                let units = Object.entries(trustData.units);
                if (units.length === 0) {
                    units = [['--', 0]];
                }
                
                results.push({
                    key: trustName,
                    total: trustData.total,
                    units: units
                        .sort(([, a], [, b]) => b - a)
                        .map(([unit, quantity]) => ({ unit, quantity })),
                    isIndividual: true,
                    hasPredecessors,
                    isExpanded
                });

                if (hasPredecessors && isExpanded) {
                    Object.entries(trustData.predecessors)
                        .sort(([, a], [, b]) => b.total - a.total)
                        .forEach(([predName, predData]) => {
                            let predUnits = Object.entries(predData.units);
                            if (predUnits.length === 0) {
                                predUnits = [['--', 0]];
                            }
                            
                            results.push({
                                key: predName,
                                total: predData.total,
                                units: predUnits
                                    .sort(([, a], [, b]) => b - a)
                                    .map(([unit, quantity]) => ({ unit, quantity })),
                                isPredecessor: true
                            });
                        });
                }
            });
    }

    return results;
}

function processAggregatedMode(aggregatedData, mode, period, latestDate) {
    const categoryMap = {
        'region': 'regions',
        'icb': 'icbs', 
        'national': 'national'
    };

    const categoryData = aggregatedData[categoryMap[mode]] || {};
    const results = [];

    Object.entries(categoryData).forEach(([name, info]) => {
        if (!info?.products) return;

        const totals = { total: 0, units: {} };

        Object.values(info.products).forEach(product => {
            if (!product?.data) return;

            Object.entries(product.data).forEach(([date, { quantity, unit }]) => {
                if (!shouldIncludeDate(date, period, latestDate)) return;

                const parsedQuantity = parseFloat(quantity) || 0;
                totals.total += parsedQuantity;

                if (!totals.units[unit]) {
                    totals.units[unit] = 0;
                }
                totals.units[unit] += parsedQuantity;
            });
        });

        if (totals.total > 0) {
            results.push({
                key: name,
                total: totals.total,
                units: Object.entries(totals.units)
                    .sort(([, a], [, b]) => b - a)
                    .map(([unit, quantity]) => ({ unit, quantity }))
            });
        }
    });

    return results.sort((a, b) => b.total - a.total);
}

function processRawDataMode(data, mode, period, latestDate) {
    if (!data?.length) return [];

    const filteredData = data.map(item => ({
        ...item,
        data: item.data ? item.data.filter(([date]) => 
            shouldIncludeDate(date, period, latestDate)) : []
    }));

    const groupedData = {};

    filteredData.forEach(item => {
        const groupKey = getGroupKey(item, mode);
        
        if (Array.isArray(groupKey)) {
            groupKey.forEach(key => processItemForGroup(groupedData, key, item));
        } else {
            processItemForGroup(groupedData, groupKey, item);
        }
    });

    return Object.entries(groupedData)
        .map(([key, value]) => ({
            key,
            total: value.total,
            units: Object.entries(value.units)
                .sort(([, a], [, b]) => b - a)
                .map(([unit, quantity]) => ({ unit, quantity }))
        }))
        .sort((a, b) => b.total - a.total);
}

function getGroupKey(item, mode) {
    switch (mode) {
        case 'organisation':
            return item.organisation__ods_name || 'Unknown Organisation';
        case 'product':
            return item.vmp__name || 'Unknown Product';
        case 'productGroup':
            return item.vmp__vtm__name || 'Unknown Product Group';
        case 'ingredient':
            return item.ingredient_names?.length > 0 ? item.ingredient_names : ['Unknown Ingredient'];
        case 'unit':
            return item.data?.map(([, , unit]) => unit).filter(Boolean) || [];
        default:
            return item.vmp__name || 'Unknown';
    }
}

function processItemForGroup(groupedData, groupKey, item) {
    if (!groupedData[groupKey]) {
        groupedData[groupKey] = {
            total: 0,
            units: {}
        };
    }

    item.data?.forEach(([date, quantity, unit]) => {
        const parsedQuantity = parseFloat(quantity) || 0;
        groupedData[groupKey].total += parsedQuantity;

        if (!groupedData[groupKey].units[unit]) {
            groupedData[groupKey].units[unit] = 0;
        }
        groupedData[groupKey].units[unit] += parsedQuantity;
    });
}

export function getModeDisplayName(mode) {
    const modeNames = {
        'organisation': 'NHS Trust',
        'region': 'Region',
        'icb': 'ICB', 
        'national': 'National total',
        'product': 'Product',
        'productGroup': 'Product group',
        'ingredient': 'Ingredient',
        'unit': 'Unit'
    };
    return modeNames[mode] || mode;
}

export function getChartExplainerText(mode, options = {}) {
    const { 
        hasSelectedOrganisations = false, 
        currentModeHasData = true,
        vmpsCount = 0
    } = options;

    const baseExplainers = {
        'organisation': () => {
            if (hasSelectedOrganisations) {
                if (currentModeHasData) {
                    return "This chart shows the quantity of the selected products issued over time for the selected NHS Trusts. Each colored line represents one trust's issuing pattern for the selected products.";
                } else {
                    return "The selected NHS Trusts have no data for these products and quantity type. Try selecting different trusts or changing the quantity type.";
                }
            } else {
                return "This chart would show individual NHS Trust quantities over time. Select specific trusts from the search panel to see their usage patterns.";
            }
        },

        'region': () => {
            return "This chart shows total quantities aggregated by NHS region over time. Each line represents the combined usage across all trusts within that region.";
        },

        'icb': () => {
            return "This chart shows total quantities aggregated by Integrated Care Board (ICB) over time. Each line represents the combined usage across all trusts within that ICB.";
        },

        'national': () => {
            return "This chart shows the total national quantities over time, representing the combined usage across all NHS Trusts in England.";
        },

        'product': () => {
            if (vmpsCount > 1) {
                return "This chart compares quantities between different products over time. Each line represents one product, showing its total usage across all NHS Trusts.";
            } else {
                return "This chart shows quantities for the selected product over time across all NHS Trusts.";
            }
        },

        'productGroup': () => {
            return "This chart shows quantities grouped by product category (VTM - Virtual Therapeutic Moiety) over time. Each line represents a therapeutic group, combining all related products.";
        },

        'ingredient': () => {
            return "This chart shows quantities grouped by active ingredient over time. Each line represents one ingredient, combining all products containing that ingredient.";
        },

        'unit': () => {
            return "This chart shows quantities grouped by unit of measurement over time. Each line represents one unit type (e.g., tablets, bottles, ampoules).";
        }
    };

    const explainerFunc = baseExplainers[mode];
    if (explainerFunc) {
        return explainerFunc();
    }

    return "This chart shows the selected data over time.";
}

export function calculatePercentiles(data, predecessorMap = new Map(), allTrusts = []) {
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
        if (item.data && Array.isArray(item.data)) {
            item.data.forEach(entry => {
                if (entry && entry.length >= 2 && entry[1] !== null && entry[1] !== undefined) {
                    const month = entry[0];
                    const quantity = parseFloat(entry[1]);
                    
                    if (!isNaN(quantity)) {
                        orgTotalUsage[orgId] += quantity;
                        allMonths.add(month);
                        hasValidData = true;
                    }
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
        const allPredecessorNames = new Set();
        if (predecessorMap instanceof Map) {
            for (const predecessors of predecessorMap.values()) {
                if (Array.isArray(predecessors)) {
                    predecessors.forEach(pred => allPredecessorNames.add(pred));
                }
            }
        }

        allTrusts.forEach(trustName => {
            if (!seenOrgNames.has(trustName)) {
                const isPredecessor = allPredecessorNames.has(trustName);
                excludedTrusts.push({
                    name: trustName,
                    code: 'Unknown',
                    reason: isPredecessor 
                        ? 'Predecessor trust (merged into successor)' 
                        : 'Not in analysis data (no matching products or quantity type)'
                });
            }
        });
    }

    const includedOrgs = Object.keys(orgTotalUsage);
    
    const trustCount = includedOrgs.length;

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
            if (item.data && Array.isArray(item.data)) {
                item.data.forEach(entry => {
                    if (entry && entry.length >= 2 && entry[1] !== null && entry[1] !== undefined) {
                        const month = entry[0];
                        const quantity = parseFloat(entry[1]);
                        
                        if (!isNaN(quantity)) {
                            orgMonthlyValues[orgId][month] = quantity;
                        }
                    }
                });
            }
        }
    });


    if (predecessorMap instanceof Map) {
        const orgNameToId = {};
        Object.entries(orgNames).forEach(([orgId, orgName]) => {
            orgNameToId[orgName] = orgId;
        });


        for (const [successorName, predecessorNames] of predecessorMap.entries()) {
            const successorId = orgNameToId[successorName];
            
            if (successorId && orgMonthlyValues[successorId]) {

                predecessorNames.forEach(predecessorName => {
                    const predecessorId = orgNameToId[predecessorName];
                    
                    if (predecessorId && orgMonthlyValues[predecessorId]) {

                        sortedMonths.forEach(month => {
                            orgMonthlyValues[successorId][month] += orgMonthlyValues[predecessorId][month];
                        });
                        
                        // Remove predecessor from ranking (but keep in trust count)
                        delete orgMonthlyValues[predecessorId];
                    }
                });
            }
        }
    }

    const orgsForRanking = Object.keys(orgMonthlyValues);

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
