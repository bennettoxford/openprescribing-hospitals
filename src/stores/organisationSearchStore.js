import { writable, get } from 'svelte/store';

function createOrganisationSearchStore() {
    const { subscribe, set, update } = writable({
        organisations: new Map(), // name -> { code, available, predecessors, successors }
        selectedItems: [],
        filterType: 'trust',
        selectedRegion: 'all',
        selectedICB: 'all',
        items: [],
        availableItems: new Set(),
        predecessorMap: new Map(),
        orgCodes: new Map()
    });

    return {
        subscribe,
        
        setOrganisationData: (orgData) => {
            update(store => {
                const organisations = new Map();

                const orgNames = Object.values(orgData.orgs || {});
                const orgCodes = orgData.org_codes || {};
                const predecessorMap = orgData.predecessor_map || orgData.predecessorMap || {};
                
                orgNames.forEach(name => {
                    organisations.set(name, {
                        name: name,
                        code: orgCodes[name] || null,
                        available: true,
                        predecessors: predecessorMap[name] || [],
                        successors: []
                    });
                });
                
                Object.entries(predecessorMap).forEach(([successor, predecessors]) => {
                    if (organisations.has(successor)) {
                        organisations.get(successor).predecessors = predecessors;
                    }
                    
                    predecessors.forEach(pred => {
                        if (!organisations.has(pred)) {
                            organisations.set(pred, {
                                name: pred,
                                code: orgCodes[pred] || null,
                                available: true,
                                predecessors: [],
                                successors: [successor]
                            });
                        } else {
                            const existing = organisations.get(pred);
                            if (!existing.successors.includes(successor)) {
                                existing.successors.push(successor);
                            }
                        }
                    });
                });
                
                const items = Array.from(organisations.keys());
                const availableItems = new Set(Array.from(organisations.entries())
                    .filter(([name, org]) => org.available)
                    .map(([name]) => name));
                const computedPredecessorMap = new Map();
                const computedOrgCodes = new Map();
                
                organisations.forEach((org, name) => {
                    if (org.predecessors.length > 0) {
                        computedPredecessorMap.set(name, org.predecessors);
                    }
                    if (org.code) {
                        computedOrgCodes.set(name, org.code);
                    }
                });
                
                return { 
                    ...store, 
                    organisations,
                    items,
                    availableItems,
                    predecessorMap: computedPredecessorMap,
                    orgCodes: computedOrgCodes
                };
            });
        },
        
        updateSelection: (selectedItems) => {
            update(store => ({ ...store, selectedItems }));
        },
        
        setFilterType: (filterType) => {
            update(store => ({ ...store, filterType }));
        },
        
        setAvailableItems: (availableItems) => {
            update(store => {
                const availableSet = new Set(availableItems);
                store.organisations.forEach((org) => {
                    org.available = availableSet.has(org.name);
                });
                
                const newAvailableItems = new Set(Array.from(store.organisations.entries())
                    .filter(([name, org]) => org.available)
                    .map(([name]) => name));
                
                return {
                    ...store,
                    availableItems: newAvailableItems
                };
            });
        },
        
        isAvailable(item) {
            const currentStore = get(this);
            const org = currentStore.organisations.get(item);
            return org ? org.available : false;
        },
        
        getOrgCode(orgName) {
            const currentStore = get(this);
            const org = currentStore.organisations.get(orgName);
            return org ? org.code : null;
        },
        
        getDisplayName(orgName) {
            const currentStore = get(this);
            const org = currentStore.organisations.get(orgName);
            if (!org) return orgName;
            return org.code ? `${orgName} (${org.code})` : orgName;
        },
        
        getRelatedOrgs(orgName) {
            const store = get(this);
            const related = new Set([orgName]);
            const org = store.organisations.get(orgName);
            
            if (org) {
                org.predecessors.forEach(pred => related.add(pred));
                
                org.successors.forEach(succ => related.add(succ));
                
                org.successors.forEach(succ => {
                    const succOrg = store.organisations.get(succ);
                    if (succOrg) {
                        succOrg.predecessors.forEach(pred => related.add(pred));
                    }
                });
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