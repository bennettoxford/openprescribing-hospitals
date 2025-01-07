import { writable } from 'svelte/store';
import { legendStore } from './legendStore';

export const resultsStore = writable({
    isAnalysisRunning: false,
    showResults: false,
    analysisData: null,
    filteredData: null,
    quantityType: null,
    searchType: null,
    dateRange: null,
    productData: {},
    organisationData: {}
});

function processAnalysisData(data) {
    const productData = {};
    const organisationData = {};

    if (!Array.isArray(data)) {
        console.error('Invalid data format:', data);
        return { productData, organisationData };
    }

    data.forEach(item => {
        if (item.vmp__code) {
            const productKey = item.vmp__code;
            
            if (!productData[productKey]) {
                productData[productKey] = {
                    code: item.vmp__code,
                    name: item.vmp__name,
                    vtm: item.vmp__vtm__name,
                    routes: item.routes || [],
                    ingredients: item.ingredient_names || [],
                    organisations: {}
                };
            }

            if (item.organisation__ods_code) {
                const orgKey = item.organisation__ods_code;
                const timeSeriesData = Array.isArray(item.data) 
                    ? item.data
                        .filter(([date, quantity]) => date && quantity)
                        .map(([date, quantity, unit]) => ({
                            date,
                            quantity: parseFloat(quantity) || 0,
                            unit
                        }))
                    : [];

                productData[productKey].organisations[orgKey] = {
                    ods_code: item.organisation__ods_code,
                    ods_name: item.organisation__ods_name,
                    data: timeSeriesData
                };

                if (!organisationData[orgKey]) {
                    organisationData[orgKey] = {
                        ods_code: item.organisation__ods_code,
                        ods_name: item.organisation__ods_name,
                        products: {}
                    };
                }
                organisationData[orgKey].products[productKey] = {
                    code: item.vmp__code,
                    name: item.vmp__name,
                    vtm: item.vmp__vtm__name,
                    routes: item.routes || [],
                    ingredients: item.ingredient_names || [],
                    data: timeSeriesData
                };
            }
        } else {
            console.warn('Invalid item format:', item);
        }
    });

    return { productData, organisationData };
}

export function updateResults(data, options = {}) {
    const { productData, organisationData } = processAnalysisData(data);

    resultsStore.update(store => ({
        ...store,
        isAnalysisRunning: false,
        showResults: true,
        analysisData: data,
        productData,
        organisationData,
        quantityType: options.quantityType || store.quantityType,
        searchType: options.searchType || store.searchType,
        dateRange: calculateDateRange(data)
    }));
}

function calculateDateRange(data) {
    let minDate = null;
    let maxDate = null;

    data.forEach(item => {
        item.data.forEach(([date]) => {
            const currentDate = new Date(date);
            if (!minDate || currentDate < minDate) minDate = currentDate;
            if (!maxDate || currentDate > maxDate) maxDate = currentDate;
        });
    });

    return { minDate, maxDate };
}

export function clearResults() {
    legendStore.reset();
    resultsStore.set({
        isAnalysisRunning: false,
        showResults: false,
        analysisData: null,
        filteredData: null,
        productData: {},
        organisationData: {},
        quantityType: null,
        searchType: null,
        dateRange: null
    });
}

