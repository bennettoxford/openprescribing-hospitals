import { describe, it, expect } from 'vitest';
import {
    buildMeasureCards,
    countMeasuresInCards,
    filterMeasuresByGroups,
    measureGroupOptions,
    measureListTitle,
    sortMeasureCards,
} from '../../components/measures/lib/measureList.js';

function measure(overrides) {
    return {
        slug: 'measure',
        status: 'published',
        name: 'Measure name',
        short_name: 'Measure',
        description: 'Description',
        measure_group: '',
        measure_group_slug: '',
        tags: [],
        first_published: '2024-01-01',
        ...overrides,
    };
}

describe('measure list groups', () => {
    it('keeps ungrouped measures as individual cards', () => {
        const cards = buildMeasureCards([
            measure({ slug: 'a', short_name: 'Alpha' }),
            measure({ slug: 'b', short_name: 'Beta' }),
        ]);
        expect(cards.map((card) => card.type)).toEqual(['measure', 'measure']);
    });

    it('collapses two measures that share a group', () => {
        const cards = buildMeasureCards([
            measure({
                slug: 'atropine',
                short_name: 'Atropine',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
            measure({
                slug: 'adrenaline',
                short_name: 'Adrenaline',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
            measure({ slug: 'other', short_name: 'Other' }),
        ]);
        expect(cards).toHaveLength(2);
        expect(cards[0].type).toBe('group');
        expect(cards[0].group.measures.map((item) => item.slug)).toEqual(['adrenaline', 'atropine']);
        expect(cards[1].measure.slug).toBe('other');
    });

    it('does not collapse a group that has one measure', () => {
        const cards = buildMeasureCards([
            measure({
                slug: 'atropine',
                short_name: 'Atropine',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
        ]);
        expect(cards).toEqual([
            expect.objectContaining({ type: 'measure', key: 'atropine' }),
        ]);
    });

    it('shows matching measures individually when the user searches', () => {
        const cards = buildMeasureCards([
            measure({
                slug: 'atropine',
                short_name: 'Atropine PFS',
                name: 'Atropine pre-filled syringe',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
            measure({
                slug: 'adrenaline',
                short_name: 'Adrenaline PFS',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
        ], { query: 'atropine' });
        expect(cards).toHaveLength(1);
        expect(cards[0].type).toBe('measure');
        expect(cards[0].measure.slug).toBe('atropine');
    });

    it('finds grouped measures from the group name', () => {
        const atropine = measure({
            slug: 'atropine',
            short_name: 'Atropine PFS',
            measure_group: 'Pre-filled syringes',
            measure_group_slug: 'pre-filled-syringes',
        });
        const cards = buildMeasureCards([
            atropine,
            measure({
                slug: 'adrenaline',
                short_name: 'Adrenaline PFS',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
        ], { query: 'syringes' });
        expect(cards.map((card) => card.measure.slug)).toEqual(['atropine', 'adrenaline']);
    });

    it('sorts a group card by the group name', () => {
        const cards = sortMeasureCards(buildMeasureCards([
            measure({ slug: 'zebra', short_name: 'Zebra' }),
            measure({
                slug: 'atropine',
                short_name: 'Atropine',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
            measure({
                slug: 'adrenaline',
                short_name: 'Adrenaline',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
        ]), 'name');
        expect(cards.map((card) => card.type === 'group' ? card.group.name : card.measure.short_name))
            .toEqual(['Pre-filled syringes', 'Zebra']);
    });
});

describe('measure count', () => {
    it('counts each measure inside a group card', () => {
        const cards = buildMeasureCards([
            measure({
                slug: 'atropine',
                short_name: 'Atropine',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
            measure({
                slug: 'adrenaline',
                short_name: 'Adrenaline',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
            measure({ slug: 'other', short_name: 'Other' }),
        ]);
        expect(countMeasuresInCards(cards)).toBe(3);
    });

    it('names the list from the active filters', () => {
        expect(measureListTitle({})).toBe('All measures');
        expect(measureListTitle({ previewMode: true })).toBe('All preview measures');
        expect(measureListTitle({ previewMode: true, includeInDevelopment: true }))
            .toBe('All preview/in development measures');
        expect(measureListTitle({ previewMode: true, groupName: 'Low value prescribing' }))
            .toBe('Low value prescribing');
        expect(measureListTitle({ showArchived: 'include' })).toBe('All measures, including archived');
        expect(measureListTitle({ showArchived: 'only' })).toBe('Archived measures');
        expect(measureListTitle({ query: ' omeprazole ' })).toBe('Results for "omeprazole"');
        expect(measureListTitle({ tagNames: ['Safety', 'Cost'] })).toBe('Measures tagged Safety, Cost');
        expect(measureListTitle({ query: 'omeprazole', showArchived: 'only' }))
            .toBe('Archived results for "omeprazole"');
        expect(measureListTitle({ tagNames: ['Safety'], showArchived: 'include' }))
            .toBe('Measures tagged Safety, including archived');
        expect(measureListTitle({ groupName: 'Low value prescribing', showArchived: 'include' }))
            .toBe('Low value prescribing, including archived');
        expect(measureListTitle({ groupName: 'Low value prescribing', query: 'aliskiren' }))
            .toBe('Results for "aliskiren"');
        expect(measureListTitle({ groupNames: ['Low value prescribing', 'Pre-filled syringes'] }))
            .toBe('Measures in Low value prescribing, Pre-filled syringes');
        expect(measureListTitle({ query: 'aliskiren', tagNames: ['Safety'], groupNames: ['Low value prescribing'] }))
            .toBe('Results for "aliskiren" tagged Safety in Low value prescribing');
    });
});

describe('measure group filter', () => {
    const measures = [
        measure({
            slug: 'aliskiren',
            short_name: 'Aliskiren',
            measure_group: 'Low value prescribing',
            measure_group_slug: 'low-value-prescribing',
        }),
        measure({
            slug: 'atropine',
            short_name: 'Atropine',
            measure_group: 'Pre-filled syringes',
            measure_group_slug: 'pre-filled-syringes',
        }),
        measure({ slug: 'other', short_name: 'Other' }),
    ];

    it('lists each group once, in name order', () => {
        expect(measureGroupOptions([measures, [
            measure({
                slug: 'adrenaline',
                measure_group: 'Pre-filled syringes',
                measure_group_slug: 'pre-filled-syringes',
            }),
        ]])).toEqual([
            { slug: 'low-value-prescribing', name: 'Low value prescribing' },
            { slug: 'pre-filled-syringes', name: 'Pre-filled syringes' },
        ]);
    });

    it('keeps measures from the selected groups', () => {
        expect(filterMeasuresByGroups(measures, ['low-value-prescribing']).map((item) => item.slug))
            .toEqual(['aliskiren']);
        expect(filterMeasuresByGroups(
            measures,
            ['low-value-prescribing', 'pre-filled-syringes'],
        ).map((item) => item.slug)).toEqual(['aliskiren', 'atropine']);
    });
});
