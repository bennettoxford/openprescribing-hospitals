import { writable } from 'svelte/store';

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

    data.forEach(item => {
        const productKey = `${item.vmp__code}`;
        const orgKey = item.organisation__ods_code;
        
        const timeSeriesData = item.data.map(([date, quantity, unit]) => ({
            date,
            quantity: parseFloat(quantity) || 0,
            unit
        }));

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
    });

    return { productData, organisationData };
}

export function updateResults(data, options = {}) {
    console.log('Results Store - Updating with data:', data);
    console.log('Results Store - Update options:', options);

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

