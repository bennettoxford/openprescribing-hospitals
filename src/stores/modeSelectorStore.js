import { writable } from 'svelte/store';

function createModeSelectorStore() {
    const { subscribe, set, update } = writable({
        selectedMode: null,
        options: []
    });

    return {
        subscribe,
        setOptions: (options) => {
            update(state => ({ 
                ...state, 
                options,
                selectedMode: options.some(opt => opt.value === state.selectedMode) 
                    ? state.selectedMode 
                    : (options.length > 0 ? options[0].value : null)
            }));
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
