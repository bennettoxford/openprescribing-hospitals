import { writable, get } from 'svelte/store';

function createOrganisationSearchStore() {
    const { subscribe, set, update } = writable({
        items: [],
        selectedItems: [],
        filterType: 'trust',
        availableItems: new Set(),
        predecessorMap: new Map(),
        selectedRegion: 'all',
        selectedICB: 'all'
    });

    return {
        subscribe,
        setItems: (items) => {
            update(store => ({ ...store, items }));
        },
        setPredecessorMap: (predecessorMap) => {
            update(store => ({ ...store, predecessorMap }));
        },
        updateSelection: (selectedItems) => {
            update(store => ({ ...store, selectedItems }));
        },
        setFilterType: (filterType) => {
            update(store => ({ ...store, filterType }));
        },
        setAvailableItems: (availableItems) => {
            update(store => ({ ...store, availableItems: new Set(availableItems) }));
        },
        isAvailable(item) {
            const currentStore = get(this);
            return currentStore.availableItems.has(item);
        },
        getRelatedOrgs(org) {
            const store = get(this);
            const related = new Set([org]);
            
            const predecessors = store.predecessorMap.get(org) || [];
            predecessors.forEach(pred => related.add(pred));
            
            for (const [successor, preds] of store.predecessorMap.entries()) {
                if (preds.includes(org)) {
                    related.add(successor);
                    preds.forEach(pred => related.add(pred));
                }
            }
            
            return Array.from(related);
        },
        setRegion: (region) => {
            update(store => ({ ...store, selectedRegion: region, selectedICB: 'all' }));
        },
        setICB: (icb) => {
            update(store => ({ ...store, selectedICB: icb }));
        }
    };
}

export const organisationSearchStore = createOrganisationSearchStore();