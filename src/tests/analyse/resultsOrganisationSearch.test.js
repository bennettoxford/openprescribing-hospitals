import { describe, it, expect } from 'vitest';
import { get } from 'svelte/store';
import {
    applyResultsModeSearchSync,
    buildResultsModeSearchSync,
} from '../../components/analyse/lib/chartOverlay.js';
import { createResultsModeSearchStore } from '../../stores/resultsModeSearchStore.js';

describe('analysis results organisation search', () => {
    it('lists all in-scope trusts and only lets trusts with data be selected', () => {
        const resultsModeSearchStore = createResultsModeSearchStore();
        const sync = buildResultsModeSearchSync({
            selectedMode: 'trust',
            availableTrusts: ['Trust A', 'Trust B', 'Trust C'],
            selectedOrganisations: ['Trust A'],
            selectedData: [
                {
                    organisation__ods_code: 'A',
                    organisation__ods_name: 'Trust A',
                    data: [10, 5],
                },
                {
                    organisation__ods_code: 'B',
                    organisation__ods_name: 'Trust B',
                    data: [0, 0],
                },
            ],
            months: ['2024-01', '2024-02'],
        });

        applyResultsModeSearchSync(sync, {
            analyseOptions: { setSelectedOrganisations() {} },
            resultsModeSearchStore,
        });

        const state = get(resultsModeSearchStore);

        expect(state.items).toEqual(['Trust A', 'Trust B', 'Trust C']);
        expect(state.availableItems).toEqual(new Set(['Trust A']));
        expect(resultsModeSearchStore.isAvailable('Trust A')).toBe(true);
        expect(resultsModeSearchStore.isAvailable('Trust B')).toBe(false);
        expect(resultsModeSearchStore.isAvailable('Trust C')).toBe(false);
    });

    it('drops overlay trusts that have no quantity data', () => {
        let overlaySelection = ['Trust A', 'Trust B'];
        const resultsModeSearchStore = createResultsModeSearchStore();
        const sync = buildResultsModeSearchSync({
            selectedMode: 'trust',
            availableTrusts: ['Trust A', 'Trust B', 'Trust C'],
            selectedOrganisations: overlaySelection,
            selectedData: [
                {
                    organisation__ods_code: 'A',
                    organisation__ods_name: 'Trust A',
                    data: [10, 5],
                },
                {
                    organisation__ods_code: 'B',
                    organisation__ods_name: 'Trust B',
                    data: [0, 0],
                },
            ],
            months: ['2024-01', '2024-02'],
        });

        expect(sync.selectedItems).toEqual(['Trust A']);
        expect(sync.selectedOrganisations).toEqual(['Trust A']);

        applyResultsModeSearchSync(sync, {
            analyseOptions: {
                setSelectedOrganisations(next) {
                    overlaySelection = next;
                },
            },
            resultsModeSearchStore,
        });

        expect(overlaySelection).toEqual(['Trust A']);
        expect(get(resultsModeSearchStore).selectedItems).toEqual(['Trust A']);
    });

    it('keeps trusts with offsetting quantities and rejects missing or zero-only data', () => {
        const sync = buildResultsModeSearchSync({
            selectedMode: 'trust',
            availableTrusts: ['Trust A', 'Trust B', 'Trust C', 'Trust D'],
            selectedData: [
                {
                    organisation__ods_code: 'A',
                    organisation__ods_name: 'Trust A',
                    data: [10, -10],
                },
                {
                    organisation__ods_code: 'B',
                    organisation__ods_name: 'Trust B',
                    data: [0, 0],
                },
                {
                    organisation__ods_code: 'C',
                    organisation__ods_name: 'Trust C',
                    data: null,
                },
                {
                    organisation__ods_code: 'D',
                    organisation__ods_name: 'Trust D',
                    data: [],
                },
            ],
            months: ['2024-01', '2024-02'],
        });

        expect(sync.availableItems).toEqual(['Trust A']);
    });
});
