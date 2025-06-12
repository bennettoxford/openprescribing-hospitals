import { writable } from 'svelte/store';
import { DEFAULT_ANALYSIS_MODE } from './analyseOptionsStore.js';

function createModeSelectorStore() {
    const { subscribe, set, update } = writable({
        selectedMode: DEFAULT_ANALYSIS_MODE,
        options: []
    });

    return {
        subscribe,
        setOptions: (options) => {
            update(state => ({ ...state, options }));
        },
        setSelectedMode: (mode) => {
            update(state => ({ ...state, selectedMode: mode }));
        },
        reset: () => {
            set({ selectedMode: DEFAULT_ANALYSIS_MODE, options: [] });
        },
        resetToDefault: (defaultMode) => {
            update(state => ({ ...state, selectedMode: defaultMode }));
        }
    };
}

export const modeSelectorStore = createModeSelectorStore();
