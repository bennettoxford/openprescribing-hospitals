import { writable } from 'svelte/store';

export const DEFAULT_MODE = 'total';

function createModeSelectorStore() {
    const { subscribe, set, update } = writable({
        selectedMode: DEFAULT_MODE,
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
            set({ selectedMode: DEFAULT_MODE, options: [] });
        },
        resetToDefault: (defaultMode) => {
            update(state => ({ ...state, selectedMode: defaultMode }));
        }
    };
}

export const modeSelectorStore = createModeSelectorStore();
