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
        ingredientNames: []
    });

    const runAnalysis = (options) => {
        update(store => ({
            ...store,
            ...options
        }));
    };

    return {
        subscribe,
        set,
        update,
        runAnalysis,
        updateOrganisations: (organisations) => {
            console.log('Analysis Options Store - Updating organisations:', organisations);
            organisationSearchStore.setItems(organisations);
            organisationSearchStore.setAvailableItems(organisations);
            organisationSearchStore.setFilterType('trust');
        }
    };
};

export const analyseOptions = createAnalyseOptionsStore();

export function clearAnalysisOptions() {
    analyseOptions.update(store => ({
        ...store,
        selectedVMPs: [],
        quantityType: '--',
        searchType: 'vmp'
    }));
    organisationSearchStore.reset();
    clearResults();
}

