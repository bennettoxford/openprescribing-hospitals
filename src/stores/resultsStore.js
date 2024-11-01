import { writable } from 'svelte/store';

const initialState = {
    isAnalysisRunning: false,
    analysisData: null,
    showResults: false,
    quantityType: 'Dose',
    searchType: 'vmp',
    filteredData: []
};

export const resultsStore = writable(initialState);

export function clearResults() {
    resultsStore.set(initialState);
}

