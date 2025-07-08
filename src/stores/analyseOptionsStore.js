import { writable } from 'svelte/store';
import { clearResults } from './resultsStore';
import { organisationSearchStore } from './organisationSearchStore';

export const VIEW_MODES = [
    { value: 'organisation', label: 'NHS Trust' },
    { value: 'icb', label: 'ICB' },
    { value: 'region', label: 'Region' },
    { value: 'total', label: 'National Total' }
];

export const ANALYSIS_DEFAULTS = {
    selectedVMPs: [],
    quantityType: '--',
    searchType: 'vmp',
    isAdvancedMode: false,
    selectedOrganisations: [],
    isAuthenticated: false
};

export const CHART_DEFAULTS = {
    yAxisBehavior: {
        forceZero: true,
        padTop: 1.1,
        resetToInitial: true
    }
};



const createAnalyseOptionsStore = () => {
    const { subscribe, set, update } = writable({
        ...ANALYSIS_DEFAULTS,
        vmpNames: [],
        vtmNames: [],
        ingredientNames: []
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
        ...ANALYSIS_DEFAULTS
    }));
    organisationSearchStore.reset();
    clearResults();
}

