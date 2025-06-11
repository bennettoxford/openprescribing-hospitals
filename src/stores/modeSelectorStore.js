import { writable } from 'svelte/store';

function createModeSelectorStore() {
    const { subscribe, set, update } = writable({
        selectedMode: 'total',
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
            set({ selectedMode: 'total', options: [] });
        },
        resetToDefault: (defaultMode) => {
            update(state => ({ ...state, selectedMode: defaultMode }));
        }
    };
}

export const modeSelectorStore = createModeSelectorStore();
