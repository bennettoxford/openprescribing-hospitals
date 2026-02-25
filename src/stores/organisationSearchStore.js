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
        orgCodes: new Map(),
        trustTypes: new Map(),
        orgRegions: new Map(),
        orgIcbs: new Map(),
        regionsHierarchy: [],
        filtersApplied: false
    });

    return {
        subscribe,
        
        setOrganisationData: (orgData) => {
            update(store => {
                const organisations = new Map();

                const orgNames = Object.values(orgData.orgs || {});
                const orgCodes = orgData.org_codes || {};
                const predecessorMap = orgData.predecessor_map || orgData.predecessorMap || {};
                const trustTypesRaw = orgData.trust_types || {};
                const trustTypes = new Map(Object.entries(trustTypesRaw));
                const orgRegionsRaw = orgData.org_regions || {};
                const orgRegions = new Map(Object.entries(orgRegionsRaw));
                const orgIcbsRaw = orgData.org_icbs || {};
                const orgIcbs = new Map(Object.entries(orgIcbsRaw));
                const regionsHierarchy = Array.isArray(orgData.regions_hierarchy) ? orgData.regions_hierarchy : [];
                
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
                    orgCodes: computedOrgCodes,
                    trustTypes,
                    orgRegions,
                    orgIcbs,
                    regionsHierarchy,
                    filtersApplied: false
                };
            });
        },

        setFiltersApplied: (flag) => {
            update(store => ({ ...store, filtersApplied: !!flag }));
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
                const newOrganisations = new Map();
                store.organisations.forEach((org, name) => {
                    newOrganisations.set(name, { ...org, available: availableSet.has(name) });
                });
                const newAvailableItems = new Set(Array.from(newOrganisations.entries())
                    .filter(([, org]) => org.available)
                    .map(([name]) => name));
                return {
                    ...store,
                    organisations: newOrganisations,
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

        getTrustType(orgName) {
            const currentStore = get(this);
            return currentStore.trustTypes?.get(orgName) ?? null;
        },

        getTrustTypes() {
            const currentStore = get(this);
            const types = currentStore.trustTypes ? [...new Set(currentStore.trustTypes.values())] : [];
            return types.sort((a, b) => a.localeCompare(b));
        },

        getOrgsByTrustType(trustType) {
            const currentStore = get(this);
            if (!currentStore.trustTypes || !trustType) return [];
            const result = [];
            currentStore.trustTypes.forEach((type, orgName) => {
                if (type === trustType && currentStore.availableItems.has(orgName)) {
                    result.push(orgName);
                }
            });
            return result;
        },

        getRegionsHierarchy() {
            const currentStore = get(this);
            return currentStore.regionsHierarchy || [];
        },

        getOrgRegion(orgName) {
            const currentStore = get(this);
            return currentStore.orgRegions?.get(orgName) ?? null;
        },

        getOrgICB(orgName) {
            const currentStore = get(this);
            return currentStore.orgIcbs?.get(orgName) ?? null;
        },

        getOrgsByRegion(regionName) {
            const currentStore = get(this);
            if (!currentStore.orgRegions || !regionName) return [];
            const result = [];
            currentStore.organisations.forEach((org, name) => {
                if (currentStore.orgRegions.get(name) === regionName) {
                    result.push(name);
                }
            });
            return result;
        },

        getOrgsByICB(icbName) {
            const currentStore = get(this);
            if (!currentStore.orgIcbs || !icbName) return [];
            const result = [];
            currentStore.organisations.forEach((org, name) => {
                if (currentStore.orgIcbs.get(name) === icbName) {
                    result.push(name);
                }
            });
            return result;
        },

        getOrgsByRegionsOrICBs(selectedRegions, selectedICBs) {
            const result = new Set();
            if (selectedRegions && selectedRegions.size > 0) {
                selectedRegions.forEach((regionName) => {
                    this.getOrgsByRegion(regionName).forEach((name) => result.add(name));
                });
            }
            if (selectedICBs && selectedICBs.size > 0) {
                selectedICBs.forEach((icbName) => {
                    this.getOrgsByICB(icbName).forEach((name) => result.add(name));
                });
            }
            return Array.from(result);
        },

        getICBsByRegions(selectedRegions) {
            const currentStore = get(this);
            const hierarchy = currentStore.regionsHierarchy || [];
            const result = new Set();
            if (selectedRegions && selectedRegions.size > 0) {
                hierarchy.forEach((r) => {
                    if (selectedRegions.has(r.region)) {
                        (r.icbs || []).forEach((icb) => result.add(icb.name));
                    }
                });
            }
            return Array.from(result);
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