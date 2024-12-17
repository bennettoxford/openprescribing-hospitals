import { writable } from 'svelte/store';

function createModeSelectorStore() {
    const { subscribe, set, update } = writable({
        selectedMode: null,
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
            set({ selectedMode: null, options: [] });
        }
    };
}

export const modeSelectorStore = createModeSelectorStore();
