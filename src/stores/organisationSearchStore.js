import { writable } from 'svelte/store';

function createOrganisationSearchStore() {
    const { subscribe, set, update } = writable({
        items: [],
        selectedItems: [],
        usedOrganisationSelection: false,
        filterType: 'trust'
    });

    return {
        subscribe,
        setItems: (items) => {
            update(store => ({ ...store, items }));
        },
        updateSelection: (selectedItems, usedOrganisationSelection) => {
            update(store => ({ ...store, selectedItems, usedOrganisationSelection }));
        },
        setFilterType: (filterType) => {
            update(store => ({ ...store, filterType }));
        },
        toggleSelection: () => {
            update(store => ({
                ...store,
                usedOrganisationSelection: !store.usedOrganisationSelection,
                selectedItems: []
            }));
        },
        reset: () => {
            update(store => ({
                ...store,
                selectedItems: [],
                usedOrganisationSelection: false
            }));
        }
    };
}

export const organisationSearchStore = createOrganisationSearchStore();