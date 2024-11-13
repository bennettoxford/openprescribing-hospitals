import { writable } from 'svelte/store';

function createOrganisationSearchStore() {
    const { subscribe, set, update } = writable({
        items: [],
        selectedItems: [],
        filterType: 'trust'
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
        isFiltering() {
            return this.selectedItems.length > 0;
        },
        reset() {
            update(store => ({
                ...store,
                selectedItems: [],
                items: []
            }));
        }
    };
}

export const organisationSearchStore = createOrganisationSearchStore();