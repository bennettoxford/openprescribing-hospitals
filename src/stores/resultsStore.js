import { writable } from 'svelte/store';
import { processAnalysisData } from '../utils/analyseUtils.js';

export const resultsStore = writable({
    isAnalysisRunning: false,
    showResults: false,
    analysisData: null,
    analysisMonths: null,
    filteredData: null,
    quantityType: null,
    searchType: null,
    dateRange: null,
    productData: {},
    organisationData: {},
    aggregatedData: {
        regions: {},
        icbs: {},
        national: {}
    },
    visibleItems: new Set(),
    showPercentiles: true,
    percentiles: [],
    trustCount: 0,
    excludedTrusts: [],
    excludedVmps: []
});

export function updateResults(data, options = {}) {
    const months = data?.months ?? [];
    const items = data?.items ?? [];

    resultsStore.update(store => ({
        ...store,
        analysisData: null,
        analysisMonths: null,
        filteredData: null,
        productData: {},
        organisationData: {},
        aggregatedData: { regions: {}, icbs: {}, national: {} }
    }));

    const { productData, organisationData, aggregatedData } = processAnalysisData(
        months,
        items,
        options.selectedOrganisations || [],
        options.predecessorMap || new Map()
    );

    const selectedOrganisations = options.selectedOrganisations || [];
    const hasShowPercentilesOverride = typeof options.showPercentiles === 'boolean';
    const excludedVmpsOverride = Array.isArray(options.excludedVmps)
        ? Array.from(new Set(options.excludedVmps.map(String))).sort()
        : null;
    
    const defaultShowPercentiles = selectedOrganisations.length === 0;
    const showPercentilesValue = hasShowPercentilesOverride
        ? options.showPercentiles
        : defaultShowPercentiles;
    
    const filteredData = selectedOrganisations.length === 0 ? 
        [] :
        items.filter(item => {
            const isIncluded = selectedOrganisations.includes(item.organisation__ods_name);
            return isIncluded;
        });

    resultsStore.update(store => ({
        ...store,
        isAnalysisRunning: false,
        showResults: true,
        analysisData: items,
        analysisMonths: months,
        filteredData: filteredData,
        productData,
        organisationData,
        aggregatedData,
        quantityType: options.quantityType || store.quantityType,
        searchType: options.searchType || store.searchType,
        dateRange: calculateDateRange(months),
        selectedOrganisations: options.selectedOrganisations || [],
        showPercentiles: showPercentilesValue,
        excludedTrusts: [],
        excludedVmps: excludedVmpsOverride ?? []
    }));
}

function calculateDateRange(months) {
    if (!Array.isArray(months) || months.length === 0) {
        return { minDate: null, maxDate: null };
    }
    const dates = months.map(m => new Date(m));
    return {
        minDate: new Date(Math.min(...dates)),
        maxDate: new Date(Math.max(...dates))
    };
}

