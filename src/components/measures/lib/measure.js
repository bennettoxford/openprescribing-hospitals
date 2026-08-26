export function sortMeasureProducts(denominatorItems = [], numeratorItems = []) {
    const numeratorCodes = new Set(
        (numeratorItems || []).map((item) => item?.code).filter(Boolean)
    );
    const sortedNumeratorItems = [...(numeratorItems || [])].sort((a, b) =>
        a.name.localeCompare(b.name)
    );
    const sortedNonNumeratorItems = [...(denominatorItems || [])]
        .filter((item) => !numeratorCodes.has(item?.code))
        .sort((a, b) => a.name.localeCompare(b.name));

    return [...sortedNumeratorItems, ...sortedNonNumeratorItems];
}

function rebuildOrganisationSearchForMode({
    mode,
    organisationSearchStore,
    parsedOrgData = {},
    icbs = [],
    regions = [],
    trusts = [],
    availableTrusts = [],
    selectedItems = [],
}) {
    if (mode === 'icb') {
        organisationSearchStore.setOrganisationData({
            orgs: Object.fromEntries(icbs.map((name) => [name, name])),
            regions_hierarchy: parsedOrgData.regions_hierarchy || [],
        });
        organisationSearchStore.setFilterType('icb');
        organisationSearchStore.setAvailableItems(icbs);
    } else if (mode === 'region') {
        organisationSearchStore.setOrganisationData({
            orgs: Object.fromEntries(regions.map((name) => [name, name])),
        });
        organisationSearchStore.setFilterType('region');
        organisationSearchStore.setAvailableItems(regions);
    } else if (mode === 'trust') {
        organisationSearchStore.setOrganisationData({
            orgs: Object.fromEntries(
                trusts.map((name) => [parsedOrgData.org_codes?.[name] || name, name])
            ),
            org_codes: parsedOrgData.org_codes || {},
            trust_types: parsedOrgData.trust_types || {},
            org_regions: parsedOrgData.org_regions || {},
            org_icbs: parsedOrgData.org_icbs || {},
            org_cancer_alliances: parsedOrgData.org_cancer_alliances || {},
            org_shelford_group: parsedOrgData.org_shelford_group || {},
            regions_hierarchy: parsedOrgData.regions_hierarchy || [],
            cancer_alliances: parsedOrgData.cancer_alliances || [],
        });
        organisationSearchStore.setFilterType('trust');
        organisationSearchStore.setAvailableItems(availableTrusts);
    } else {
        return;
    }

    organisationSearchStore.updateSelection(Array.from(selectedItems || []));
}

export function syncOrganisationSearchForMode({
    currentMode,
    lastSearchMode,
    ...rebuildArgs
}) {
    if (!currentMode || currentMode === lastSearchMode) {
        return lastSearchMode;
    }

    rebuildOrganisationSearchForMode({
        mode: currentMode,
        ...rebuildArgs,
    });
    return currentMode;
}
