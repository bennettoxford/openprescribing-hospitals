import { writable } from 'svelte/store';

const initialState = {
    isAnalysisRunning: false,
    showResults: false,
    analysisData: null,
    filteredData: null,
    productData: {},
    organisationData: {},
    quantityType: null,
    searchType: null,
    dateRange: null,
    visibleItems: new Set(),
    isAdvancedMode: false,
    showPercentiles: true,
    percentiles: [],
    trustCount: 0,
    excludedTrusts: [],
    trustCountBreakdown: { current: 0, predecessors: 0, total: 0 }
};

export const resultsStore = writable({ ...initialState });

export function clearResults() {
    resultsStore.set({ ...initialState });
}
