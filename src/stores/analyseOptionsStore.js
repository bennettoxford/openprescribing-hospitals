import { writable } from 'svelte/store';
import { clearResults } from './resultsStore';

export const analyseOptions = writable({
  selectedVMPs: [],
  selectedODS: [],
  quantityType: '--',
  searchType: 'vmp',
  usedOrganisationSelection: false,
  vmpNames: [],
  odsNames: [],
  vtmNames: [],
  atcNames: [],
  ingredientNames: [],
  dateRange: {
    startDate: null,
    endDate: null
  },
  minDate: null,
  maxDate: null
});

export function clearAnalysisOptions() {
    analyseOptions.update(store => ({
        ...store,
        selectedVMPs: [],
        selectedODS: [],
        quantityType: '--',
        searchType: 'vmp',
        usedOrganisationSelection: false,
        dateRange: {
          startDate: store.minDate,
          endDate: store.maxDate
      }
    }));
    clearResults();
}

