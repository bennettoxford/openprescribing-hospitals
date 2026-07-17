import { writable } from 'svelte/store';
import {
    allOverlaySelection,
    normaliseOverlaySelection,
    overlayTrustsForModeChange,
} from '../components/analyse/lib/chartOverlay.js';

const createAnalyseOptionsStore = () => {
    const { subscribe, set, update } = writable({
        selectedVMPs: [],
        quantityType: null,
        searchType: 'vmp',
        vmpNames: [],
        vtmNames: [],
        ingredientNames: [],
        selectedOrganisations: [],
        rememberedOverlayOrganisations: [],
        selectedChartRegions: allOverlaySelection(),
        selectedChartIcbs: allOverlaySelection(),
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
        setSelectedOrganisations: (organisations) => {
            update(store => ({
                ...store,
                selectedOrganisations: Array.isArray(organisations) ? organisations : []
            }));
        },
        setRememberedOverlayOrganisations: (organisations) => {
            update(store => ({
                ...store,
                rememberedOverlayOrganisations: Array.isArray(organisations) ? organisations : []
            }));
        },
        applyOverlayModeChange: (fromMode, toMode) => {
            update(store => {
                const { patch } = overlayTrustsForModeChange(fromMode, toMode, {
                    selectedOrganisations: store.selectedOrganisations,
                    rememberedOverlayOrganisations: store.rememberedOverlayOrganisations,
                });
                return patch ? { ...store, ...patch } : store;
            });
        },
        setSelectedChartRegions: (regions) => {
            update(store => ({
                ...store,
                selectedChartRegions: normaliseOverlaySelection(regions),
            }));
        },
        setSelectedChartIcbs: (icbs) => {
            update(store => ({
                ...store,
                selectedChartIcbs: normaliseOverlaySelection(icbs),
            }));
        },
        setQuantityType: (quantityType) => {
            update(store => ({
                ...store,
                quantityType: quantityType
            }));
        }
    };
};

export const analyseOptions = createAnalyseOptionsStore();
