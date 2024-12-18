import { writable } from 'svelte/store';

function createLegendStore() {
    const { subscribe, set, update } = writable({
        items: [],
        isPercentileMode: false,
        visibleItems: new Set(),
        percentileRanges: [
            { range: [5, 95], opacity: 0.1 },
            { range: [15, 85], opacity: 0.2 },
            { range: [25, 75], opacity: 0.4 },
            { range: [35, 65], opacity: 0.6 },
            { range: [45, 55], opacity: 0.8 }
        ]
    });

    return {
        subscribe,
        setItems: (items) => {
            update(state => {
                return { ...state, items };
            });
        },
        setPercentileMode: (mode) => {
            update(state => ({ ...state, isPercentileMode: mode }));
        },
        toggleItem: (itemLabel) => {
            update(state => {
                const newVisible = new Set(state.visibleItems);
                if (newVisible.has(itemLabel)) {
                    newVisible.delete(itemLabel);
                } else {
                    newVisible.add(itemLabel);
                }
                return { ...state, visibleItems: newVisible };
            });
        },
        setVisibleItems: (items) => {
            update(state => {
                const newVisible = new Set(items);
                if (state.isPercentileMode) {
                    newVisible.add('Median (50th Percentile)');
                    newVisible.add('Percentile Range');
                }
                return { ...state, visibleItems: newVisible };
            });
        },
        clearVisibleItems: () => {
            update(state => ({ ...state, visibleItems: new Set() }));
        },
        reset: () => {
            set({
                items: [],
                isPercentileMode: false,
                visibleItems: new Set(),
                percentileRanges: [
                    { range: [5, 95], opacity: 0.1 },
                    { range: [15, 85], opacity: 0.2 },
                    { range: [25, 75], opacity: 0.4 },
                    { range: [35, 65], opacity: 0.6 },
                    { range: [45, 55], opacity: 0.8 }
                ]
            });
        }
    };
}

export const legendStore = createLegendStore();