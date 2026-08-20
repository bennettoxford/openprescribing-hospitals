import { get, writable } from 'svelte/store';

export function createResultsModeSearchStore() {
    const { subscribe, update } = writable({
        filterType: 'trust',
        items: [],
        selectedItems: [],
        availableItems: new Set()
    });

    function setItems(items = [], filterType = 'trust', availableItems = items) {
        const uniqueItems = Array.from(new Set((items || []).filter(Boolean)));
        const availableSet = new Set(availableItems);
        update(store => ({
            ...store,
            filterType,
            items: uniqueItems,
            selectedItems: (store.selectedItems || []).filter(item => availableSet.has(item)),
            availableItems: availableSet
        }));
    }

    function updateSelection(selectedItems = []) {
        const nextSelected = Array.isArray(selectedItems) ? selectedItems : [];
        update(store => ({
            ...store,
            selectedItems: nextSelected.filter(item => store.availableItems.has(item))
        }));
    }

    return {
        subscribe,
        setItems,
        updateSelection,
        isAvailable: (item) => get({ subscribe }).availableItems.has(item),
        getDisplayName: (item) => item || ''
    };
}
