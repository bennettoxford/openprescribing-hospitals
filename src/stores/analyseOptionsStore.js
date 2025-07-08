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
        isAdvancedMode: false,
        selectedOrganisations: []
    });

    const runAnalysis = (options) => {
        update(store => ({
            ...store,
            ...options,
            selectedOrganisations: options.organisations || []
        }));
    };

    return {
        subscribe,
        set,
        update,
        runAnalysis,
        updateOrganisations: (organisations) => {
            organisationSearchStore.setItems(organisations);
            organisationSearchStore.setAvailableItems(organisations);
            organisationSearchStore.setFilterType('trust');
        },
        setAdvancedMode: (isAdvanced) => {
            update(store => ({
                ...store,
                isAdvancedMode: isAdvanced
            }));
        },
        setAuthentication: (isAuthenticated) => {
            update(store => ({
                ...store,
                isAuthenticated: Boolean(isAuthenticated)
            }));
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
        selectedOrganisations: []
    }));
    organisationSearchStore.reset();
    clearResults();
}

