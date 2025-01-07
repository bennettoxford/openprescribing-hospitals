import { writable, get } from 'svelte/store';

function createOrganisationSearchStore() {
    const { subscribe, set, update } = writable({
        items: [],
        selectedItems: [],
        filterType: 'trust',
        availableItems: new Set()
    });

    return {
        subscribe,
        setItems: (items) => {
            update(store => ({ ...store, items }));
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
        }
    };
}

export const organisationSearchStore = createOrganisationSearchStore();