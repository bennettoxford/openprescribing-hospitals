import { writable } from 'svelte/store';

export const resultsStore = writable({
  isAnalysisRunning: false,
  analysisData: null,
  showResults: false,
  quantityType: 'Dose',
  searchType: 'vmp',
  filteredData: []
});

