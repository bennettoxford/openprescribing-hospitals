import { writable } from 'svelte/store';
import { processAnalysisData } from '../utils/analyseUtils.js';

export const resultsStore = writable({
    isAnalysisRunning: false,
    showResults: false,
    analysisData: null,
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
    isAdvancedMode: false
});

export function updateResults(data, options = {}) {
    resultsStore.update(store => ({
        ...store,
        analysisData: null,
        filteredData: null,
        productData: {},
        organisationData: {},
        aggregatedData: { regions: {}, icbs: {}, national: {} }
    }));

    const { productData, organisationData, aggregatedData } = processAnalysisData(
        data, 
        options.selectedOrganisations || [],
        options.predecessorMap || new Map()
    );

    const selectedOrganisations = options.selectedOrganisations || [];
    
    const filteredData = selectedOrganisations.length === 0 ? 
        [] :
        data.filter(item => {
            const isIncluded = selectedOrganisations.includes(item.organisation__ods_name);
            return isIncluded;
        });

    resultsStore.update(store => ({
        ...store,
        isAnalysisRunning: false,
        showResults: true,
        analysisData: data,
        filteredData: filteredData,
        productData,
        organisationData,
        aggregatedData,
        quantityType: options.quantityType || store.quantityType,
        searchType: options.searchType || store.searchType,
        dateRange: calculateDateRange(data),
        isAdvancedMode: options.isAdvancedMode,
        selectedOrganisations: options.selectedOrganisations || []
    }));
}

function calculateDateRange(data) {
    let minDate = null;
    let maxDate = null;

    data.forEach(item => {
        if (item.data) {
            item.data.forEach(([date]) => {
                const currentDate = new Date(date);
                if (!minDate || currentDate < minDate) minDate = currentDate;
                if (!maxDate || currentDate > maxDate) maxDate = currentDate;
            });
        }
    });

    return { minDate, maxDate };
}

