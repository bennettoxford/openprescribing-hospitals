import { describe, it, expect, beforeEach } from 'vitest';
import { get } from 'svelte/store';
import { organisationSearchStore } from '../../stores/organisationSearchStore.js';
import {
    buildMeasureAnalyseHref,
    sortMeasureProducts,
    syncOrganisationSearchForMode,
} from '../../components/measures/lib/measure.js';
import { encodeQuantityType } from '../../components/analyse/lib/analyseUrlParams.js';

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

describe('sortMeasureProducts', () => {
    it('puts numerators first and drops denominator duplicates', () => {
        const denominatorItems = [
            { code: '1', name: 'Zopiclone 7.5mg tablets' },
            { code: '2', name: 'Amoxicillin 500mg capsules' },
            { code: '3', name: 'Metformin 500mg tablets' },
        ];
        const numeratorItems = [
            { code: '1', name: 'Zopiclone 7.5mg tablets' },
            { code: '2', name: 'Amoxicillin 500mg capsules' },
        ];

        const sorted = sortMeasureProducts(denominatorItems, numeratorItems);

        expect(sorted.map((item) => item.name)).toEqual([
            'Amoxicillin 500mg capsules',
            'Zopiclone 7.5mg tablets',
            'Metformin 500mg tablets',
        ]);
        expect(sorted.map((item) => item.code)).toEqual(['2', '1', '3']);
        expect(sorted).toHaveLength(3);
    });
});

describe('buildMeasureAnalyseHref', () => {
    it('builds an Analyse URL with vmps and quantity type', () => {
        const href = buildMeasureAnalyseHref({
            products: [
                { code: '111', name: 'Amoxicillin 500mg capsules' },
                { code: '222', name: 'Metformin 500mg tablets' },
            ],
            quantityType: 'ddd',
            maxVmpCount: 250,
        });

        expect(href).toBe('/analyse/?vmps=111,222&quantity=ddd');
    });

    it('does not duplicate vmp codes when numerator is also in denominator', () => {
        const products = sortMeasureProducts(
            [
                { code: '111', name: 'Amoxicillin 500mg capsules' },
                { code: '222', name: 'Metformin 500mg tablets' },
            ],
            [{ code: '111', name: 'Amoxicillin 500mg capsules' }]
        );

        const href = buildMeasureAnalyseHref({
            products,
            quantityType: 'dose',
            maxVmpCount: 250,
        });

        expect(href).toBe('/analyse/?vmps=111,222&quantity=dose');
        expect(href.match(/111/g)).toHaveLength(1);
    });

    it('returns null when the product count is over the limit', () => {
        const products = [
            { code: '1', name: 'Amoxicillin 500mg capsules' },
            { code: '2', name: 'Fluconazole 50mg capsules' },
            { code: '3', name: 'Metformin 500mg tablets' },
        ];

        expect(
            buildMeasureAnalyseHref({
                products,
                quantityType: 'scmd',
                maxVmpCount: 2,
            })
        ).toBeNull();
    });

    it('returns null when there are no products', () => {
        expect(
            buildMeasureAnalyseHref({
                products: [],
                quantityType: 'ddd',
                maxVmpCount: 250,
            })
        ).toBeNull();
    });

    it('returns null when maxVmpCount is missing', () => {
        expect(
            buildMeasureAnalyseHref({
                products: [{ code: '111', name: 'Amoxicillin 500mg capsules' }],
                quantityType: 'ddd',
                maxVmpCount: null,
            })
        ).toBeNull();
    });

    it('uses SCMD quantity for indicative cost measures', () => {
        const href = buildMeasureAnalyseHref({
            products: [{ code: '111', name: 'Amoxicillin 500mg capsules' }],
            quantityType: 'indicative_cost',
            maxVmpCount: 250,
        });

        expect(href).toBe('/analyse/?vmps=111&quantity=scmd');
    });
});

describe('encodeQuantityType for measure short codes', () => {
    it('encodes measure short codes and Analyse display names', () => {
        expect(encodeQuantityType('ddd')).toBe('ddd');
        expect(encodeQuantityType('dose')).toBe('dose');
        expect(encodeQuantityType('Defined Daily Dose Quantity')).toBe('ddd');
        expect(encodeQuantityType('Unit Dose Quantity')).toBe('dose');
        expect(encodeQuantityType('indicative_cost')).toBeNull();
    });
});
