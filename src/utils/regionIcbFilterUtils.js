/**
 * Flattens nested organisations to array of { name, region, icb, data, available }.
 * @param {Array<{name: string, region?: string, icb?: string, data?: Array, available?: boolean, predecessors?: Array}>} orgs
 * @returns {Array<{name: string, region?: string, icb?: string, data: Array, available?: boolean}>}
 */
export function flattenOrganisationsWithMetadata(orgs) {
    const result = [];
    function collect(org) {
        result.push({
            name: org.name,
            region: org.region ?? null,
            icb: org.icb ?? null,
            data: org.data || [],
            available: org.available ?? true,
        });
        if (org.predecessors) {
            org.predecessors.forEach((pred) => collect(pred));
        }
    }
    (orgs || []).forEach((org) => collect(org));
    return result;
}

/**
 * Flattens nested organisations to flat data format: { name: { available, data } }.
 * available is true only when org has data for the measure (consistent with MeasureTrustsList).
 * @param {Array<{name: string, data: Array, available?: boolean, predecessors?: Array}>} orgs - Nested org objects
 * @returns {Object<string, {available: boolean, data: Array}>}
 */
export function flattenOrganisationsToData(orgs) {
    return Object.fromEntries(
        flattenOrganisationsWithMetadata(orgs).map((o) => {
            const hasData = (o.data?.length ?? 0) > 0 && o.available !== false;
            return [o.name, { available: hasData, data: o.data || [] }];
        })
    );
}

/**
 * Flattens nested organisations (with predecessors) to a list of org names for search.
 * @param {Array<{name: string, predecessors?: Array}>} orgs - Nested org objects
 * @returns {Array<string>} Unique org names
 */
export function prepareOrganisationsForSearch(orgs) {
    let allOrgs = [];
    function collectOrgs(org) {
        allOrgs.push(org.name);
        if (org.predecessors) {
            org.predecessors.forEach((pred) => collectOrgs(pred));
        }
    }
    (orgs || []).forEach((org) => collectOrgs(org));
    return [...new Set(allOrgs)];
}

/**
 * Returns updated selectedRegions/selectedICBs when a region is toggled.
 * @param {{ region: string, icbs?: Array<{name: string}> }} region
 * @param {Set<string>} selectedRegions
 * @param {Set<string>} selectedICBs
 * @returns {{ selectedRegions: Set<string>, selectedICBs: Set<string> }}
 */
export function updateRegionSelection(region, selectedRegions, selectedICBs) {
    if (selectedRegions.has(region.region)) {
        const next = new Set(selectedRegions);
        next.delete(region.region);
        return { selectedRegions: next, selectedICBs: new Set(selectedICBs) };
    }
    const nextRegions = new Set([...selectedRegions, region.region]);
    const icbNamesToRemove = new Set(region.icbs?.map((i) => i.name) ?? []);
    const nextICBs = new Set([...selectedICBs].filter((icb) => !icbNamesToRemove.has(icb)));
    return { selectedRegions: nextRegions, selectedICBs: nextICBs };
}

/**
 * Returns updated selectedICBs when an ICB is toggled, or null if no change (e.g. region already selected).
 * @param {{ region: string }} region
 * @param {{ name: string }} icb
 * @param {Set<string>} selectedRegions
 * @param {Set<string>} selectedICBs
 * @returns {{ selectedRegions: Set<string>, selectedICBs: Set<string> } | null}
 */
export function updateIcbSelection(region, icb, selectedRegions, selectedICBs) {
    if (selectedRegions.has(region.region)) return null;
    if (selectedICBs.has(icb.name)) {
        return {
            selectedRegions: new Set(selectedRegions),
            selectedICBs: new Set([...selectedICBs].filter((i) => i !== icb.name)),
        };
    }
    return {
        selectedRegions: new Set(selectedRegions),
        selectedICBs: new Set([...selectedICBs, icb.name]),
    };
}

/**
 * Returns org identifiers that match the selected regions and/or ICBs.
 * @param {Array} itemsToFilter - Items to filter (trust names, org objects, etc.)
 * @param {(item) => string|null} getRegion - Extracts region from an item
 * @param {(item) => string|null} getIcb - Extracts ICB from an item
 * @param {(item) => string} getId - Extracts identifier (name) from an item
 * @param {Set<string>} selectedRegions
 * @param {Set<string>} selectedICBs
 * @param {Array<string>} allItems - Returned when no filters are selected
 * @returns {Array<string>}
 */
export function getOrgsFromRegionIcbFilter(
    itemsToFilter,
    getRegion,
    getIcb,
    getId,
    selectedRegions,
    selectedICBs,
    allItems
) {
    if (selectedRegions.size === 0 && selectedICBs.size === 0) {
        return allItems;
    }
    const result = new Set();
    itemsToFilter.forEach((item) => {
        const region = getRegion(item);
        const icb = getIcb(item);
        if (selectedRegions.has(region) || selectedICBs.has(icb)) {
            result.add(getId(item));
        }
    });
    return Array.from(result);
}
