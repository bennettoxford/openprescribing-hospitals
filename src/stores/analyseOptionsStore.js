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
  ingredientNames: []
});

export function clearAnalysisOptions() {
    analyseOptions.update(store => ({
        ...store,
        selectedVMPs: [],
        selectedODS: [],
        quantityType: '--',
        searchType: 'vmp',
        usedOrganisationSelection: false,
    }));
    clearResults();
}

