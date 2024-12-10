import { writable } from 'svelte/store';
import { clearResults } from './resultsStore';
import { organisationSearchStore } from './organisationSearchStore';

const createAnalyseOptionsStore = () => {
    const { subscribe, set, update } = writable({
        selectedVMPs: [],
        quantityType: '--',
        searchType: 'vmp',
        vmpNames: [],
        vtmNames: [],
        ingredientNames: [],
        dateRange: {
            startDate: null,
            endDate: null
        },
        minDate: null,
        maxDate: null
    });

    return {
        subscribe,
        set,
        update,
        // Update organisations in the organisationSearchStore
        updateOrganisations: (organisations) => {
            organisationSearchStore.setItems(organisations);
        }
    };
};

export const analyseOptions = createAnalyseOptionsStore();

export function clearAnalysisOptions() {
    analyseOptions.update(store => ({
        ...store,
        selectedVMPs: [],
        quantityType: '--',
        searchType: 'vmp',
        dateRange: {
            startDate: store.minDate,
            endDate: store.maxDate
        }
    }));
    organisationSearchStore.reset();
    clearResults();
}

