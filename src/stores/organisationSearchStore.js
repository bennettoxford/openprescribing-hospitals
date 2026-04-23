import { writable, get } from 'svelte/store';

function createOrganisationSearchStore() {
    const { subscribe, set, update } = writable({
        organisations: new Map(), // name -> { code, available }
        selectedItems: [],
        filterType: 'trust',
        items: [],
        availableItems: new Set(),
        orgCodes: new Map(),
        trustTypes: new Map(),
        orgRegions: new Map(),
        orgIcbs: new Map(),
        orgCancerAlliances: new Map(),
        orgShelfordGroup: new Map(),
        regionsHierarchy: [],
        cancerAlliances: [],
        filtersApplied: false
    });

    return {
        subscribe,
        
        setOrganisationData: (orgData) => {
            update(store => {
                const organisations = new Map();

                const orgNames = Object.values(orgData.orgs || {});
                const orgCodes = orgData.org_codes || {};
                const trustTypesRaw = orgData.trust_types || {};
                const trustTypes = new Map(Object.entries(trustTypesRaw));
                const orgRegionsRaw = orgData.org_regions || {};
                const orgRegions = new Map(Object.entries(orgRegionsRaw));
                const orgIcbsRaw = orgData.org_icbs || {};
                const orgIcbs = new Map(Object.entries(orgIcbsRaw));
                const orgCancerAlliancesRaw = orgData.org_cancer_alliances || {};
                const orgCancerAlliances = new Map(Object.entries(orgCancerAlliancesRaw));
                const orgShelfordGroupRaw = orgData.org_shelford_group || {};
                const orgShelfordGroup = new Map(
                    Object.entries(orgShelfordGroupRaw).map(([k, v]) => [k, !!v])
                );
                const regionsHierarchy = Array.isArray(orgData.regions_hierarchy) ? orgData.regions_hierarchy : [];
                const cancerAlliances = Array.isArray(orgData.cancer_alliances) ? orgData.cancer_alliances : [];
                
                orgNames.forEach(name => {
                    organisations.set(name, {
                        name: name,
                        code: orgCodes[name] || null,
                        available: true
                    });
                });

                const items = Array.from(orgNames);
                const availableItems = new Set(
                    items.filter((name) => organisations.get(name)?.available)
                );
                const computedOrgCodes = new Map();
                
                organisations.forEach((org, name) => {
                    if (org.code) {
                        computedOrgCodes.set(name, org.code);
                    }
                });
                
                return {
                    ...store,
                    organisations,
                    items,
                    availableItems,
                    orgCodes: computedOrgCodes,
                    trustTypes,
                    orgRegions,
                    orgIcbs,
                    orgCancerAlliances,
                    orgShelfordGroup,
                    regionsHierarchy,
                    cancerAlliances,
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
            return currentStore.availableItems?.has(item) ?? false;
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

        /** Returns orgs by trust type from the full items set */
        getOrgsByTrustTypeGlobal(trustType) {
            const currentStore = get(this);
            if (!currentStore.trustTypes || !trustType) return [];
            const itemsSet = new Set(currentStore.items || []);
            const result = [];
            currentStore.trustTypes.forEach((type, orgName) => {
                if (type === trustType && itemsSet.has(orgName)) {
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

        getCancerAlliance(orgName) {
            const currentStore = get(this);
            return currentStore.orgCancerAlliances?.get(orgName) ?? null;
        },

        getCancerAlliances() {
            const currentStore = get(this);
            return currentStore.cancerAlliances || [];
        },

        getOrgsByCancerAlliance(caName) {
            const currentStore = get(this);
            if (!currentStore.orgCancerAlliances || !caName) return [];
            const result = [];
            currentStore.orgCancerAlliances.forEach((ca, orgName) => {
                if (ca === caName && currentStore.items?.includes(orgName)) result.push(orgName);
            });
            return result;
        },

        getOrgsWithNoCancerAlliance() {
            const currentStore = get(this);
            if (!currentStore.items) return [];
            return currentStore.items.filter((orgName) => {
                const ca = currentStore.orgCancerAlliances?.get(orgName);
                return !ca || ca === '';
            });
        },

        getOrgsByCancerAlliances(selectedCancerAlliances) {
            const result = new Set();
            if (selectedCancerAlliances && selectedCancerAlliances.size > 0) {
                selectedCancerAlliances.forEach((caName) => {
                    if (caName === 'Not applicable') {
                        this.getOrgsWithNoCancerAlliance().forEach((name) => result.add(name));
                    } else {
                        this.getOrgsByCancerAlliance(caName).forEach((name) => result.add(name));
                    }
                });
            }
            return Array.from(result);
        },

        getOrgsByRegion(regionName) {
            const currentStore = get(this);
            if (!currentStore.orgRegions || !regionName) return [];
            return (currentStore.items || []).filter(
                (name) => currentStore.orgRegions.get(name) === regionName
            );
        },

        getOrgsByICB(icbName) {
            const currentStore = get(this);
            if (!currentStore.orgIcbs || !icbName) return [];
            return (currentStore.items || []).filter(
                (name) => currentStore.orgIcbs.get(name) === icbName
            );
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
        }
    };
}

export const organisationSearchStore = createOrganisationSearchStore();