import { writable } from 'svelte/store';

export const resultsStore = writable({
    isAnalysisRunning: false,
    showResults: false,
    analysisData: null,
    filteredData: null,
    quantityType: null,
    searchType: null,
    dateRange: null
});

export function clearResults() {
    resultsStore.set({
        isAnalysisRunning: false,
        showResults: false,
        analysisData: null,
        filteredData: null,
        quantityType: null,
        searchType: null,
        dateRange: null
    });
}

