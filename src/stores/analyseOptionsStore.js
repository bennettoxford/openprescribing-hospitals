import { writable } from 'svelte/store';

export const analyseOptions = writable({
  selectedVMPs: [],
  selectedODS: [],
  quantityType: '--',
  searchType: 'vmp',
  usedOrganisationSelection: false
});

