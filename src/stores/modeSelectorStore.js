import { writable } from 'svelte/store';
import { ANALYSIS_SCOPE } from '../components/analyse/lib/analysisScope.js';

export function reconcileSelectedMode(selectedMode, availableModes = []) {
    if (selectedMode == null) {
        return null;
    }

    const values = availableModes.map((mode) =>
        typeof mode === 'string' ? mode : mode?.value
    );

    return values.includes(selectedMode) ? selectedMode : null;
}

export function selectDefaultMode(availableModes, scope = ANALYSIS_SCOPE.ALL) {
    if (scope === ANALYSIS_SCOPE.NATIONAL) {
        const nationalModeForScope = availableModes.find((m) => m.value === 'national');
        if (nationalModeForScope) return nationalModeForScope.value;
    }

    const trustMode = availableModes.find((m) => m.value === 'trust');
    if (trustMode) return trustMode.value;

    const nationalMode = availableModes.find((m) => m.value === 'national');
    if (nationalMode) return nationalMode.value;

    return availableModes[0]?.value || 'trust';
}

function createModeSelectorStore() {
    const { subscribe, set, update } = writable({
        selectedMode: null,
    });

    return {
        subscribe,
        setSelectedMode: (mode) => {
            update((state) => ({ ...state, selectedMode: mode }));
        },
        reset: () => {
            set({ selectedMode: null });
        },
    };
}

export const modeSelectorStore = createModeSelectorStore();
