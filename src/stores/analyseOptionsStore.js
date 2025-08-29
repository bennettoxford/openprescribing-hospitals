import { writable } from 'svelte/store';
import { organisationSearchStore } from './organisationSearchStore';

const createAnalyseOptionsStore = () => {
    const { subscribe, set, update } = writable({
        selectedVMPs: [],
        quantityType: '--',
        searchType: 'vmp',
        vmpNames: [],
        vtmNames: [],
        ingredientNames: [],
        selectedOrganisations: []
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
            organisationSearchStore.setItems(organisations);
            organisationSearchStore.setAvailableItems(organisations);
            organisationSearchStore.setFilterType('trust');
        },
        setSelectedOrganisations: (organisations) => {
            update(store => ({
                ...store,
                selectedOrganisations: organisations
            }));
        }
    };
};

export const analyseOptions = createAnalyseOptionsStore();


