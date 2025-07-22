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
        isAdvancedMode: false,
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
            console.log('Analysis Options Store - Updating organisations:', organisations);
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
        setSelectedOrganisations: (organisations) => {
            update(store => ({
                ...store,
                selectedOrganisations: organisations
            }));
        }
    };
};

export const analyseOptions = createAnalyseOptionsStore();


