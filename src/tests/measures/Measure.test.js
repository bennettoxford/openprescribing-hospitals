import { describe, it, expect, beforeEach } from 'vitest';
import { get } from 'svelte/store';
import { organisationSearchStore } from '../../stores/organisationSearchStore.js';
import { syncOrganisationSearchForMode } from '../../components/measures/lib/measure.js';

const SAMPLE_ORG_DATA = {
    orgs: {
        R1: 'Trust A',
        R2: 'Trust B',
        R3: 'Trust C',
    },
    org_codes: {
        'Trust A': 'R1',
        'Trust B': 'R2',
        'Trust C': 'R3',
    },
    trust_types: {
        'Trust A': 'Acute - Teaching',
        'Trust B': 'Community',
        'Trust C': 'Acute - Teaching',
    },
};

const TRUSTS = ['Trust A', 'Trust B', 'Trust C'];

const SEARCH_CONTEXT = {
    organisationSearchStore,
    parsedOrgData: SAMPLE_ORG_DATA,
    icbs: ['ICB 1'],
    regions: ['Region 1'],
    trusts: TRUSTS,
    availableTrusts: TRUSTS,
};

describe('Measure organisation search rebuild', () => {
    beforeEach(() => {
        organisationSearchStore.setOrganisationData({ orgs: {} });
        organisationSearchStore.updateSelection([]);
        organisationSearchStore.setFilterType('trust');
        organisationSearchStore.setAvailableItems([]);
        organisationSearchStore.setFiltersApplied(false);
    });

    it('keeps applied filters when selection changes in the same mode', () => {
        let lastSearchMode = syncOrganisationSearchForMode({
            ...SEARCH_CONTEXT,
            currentMode: 'trust',
            lastSearchMode: undefined,
            selectedItems: TRUSTS,
        });

        organisationSearchStore.applyScopeFilters({
            trustTypes: ['Community'],
        });
        organisationSearchStore.updateSelection(['Trust B']);

        lastSearchMode = syncOrganisationSearchForMode({
            ...SEARCH_CONTEXT,
            currentMode: 'trust',
            lastSearchMode,
            selectedItems: ['Trust B'],
        });

        const state = get(organisationSearchStore);

        expect(lastSearchMode).toBe('trust');
        expect(state.filtersApplied).toBe(true);
        expect(state.availableItems).toEqual(new Set(['Trust B']));
        expect(state.selectedItems).toEqual(['Trust B']);
    });

    it('rebuilds the search when the mode changes', () => {
        syncOrganisationSearchForMode({
            ...SEARCH_CONTEXT,
            currentMode: 'trust',
            lastSearchMode: undefined,
            selectedItems: TRUSTS,
        });

        organisationSearchStore.applyScopeFilters({
            trustTypes: ['Community'],
        });
        organisationSearchStore.updateSelection(['Trust B']);

        const lastSearchMode = syncOrganisationSearchForMode({
            ...SEARCH_CONTEXT,
            currentMode: 'icb',
            lastSearchMode: 'trust',
            selectedItems: ['ICB 1'],
        });

        const state = get(organisationSearchStore);

        expect(lastSearchMode).toBe('icb');
        expect(state.filtersApplied).toBe(false);
        expect(state.filterType).toBe('icb');
        expect(state.availableItems).toEqual(new Set(['ICB 1']));
        expect(state.selectedItems).toEqual(['ICB 1']);
    });
});
