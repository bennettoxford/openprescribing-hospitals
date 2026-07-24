export const ACUTE_PREFIX = 'Acute - ';
export const ACUTE_PARENT = 'Acute';

/** Display name → short URL slug. */
export const TRUST_TYPE_URL_SLUGS = {
    [ACUTE_PARENT]: 'acute',
    'Acute - Large': 'acute-large',
    'Acute - Medium': 'acute-medium',
    'Acute - Multi-Service': 'acute-multi-service',
    'Acute - Small': 'acute-small',
    'Acute - Specialist': 'acute-specialist',
    'Acute - Teaching': 'acute-teaching',
    Ambulance: 'ambulance',
    'Care Trust': 'care',
    Community: 'community',
    'Mental Health And Learning Disability': 'mhld',
};

const TRUST_TYPE_FROM_URL_SLUG = new Map(
    Object.entries(TRUST_TYPE_URL_SLUGS).map(([name, slug]) => [slug, name])
);

export function isAcuteType(type) {
    return typeof type === 'string' && type.startsWith(ACUTE_PREFIX);
}

export function acuteSubtypeLabel(type) {
    return type.slice(ACUTE_PREFIX.length).trim();
}

function selectionHas(selected, value) {
    return selected instanceof Set ? selected.has(value) : selected.includes(value);
}

/** Parent `Acute` matches every `Acute - …` subtype. */
export function trustTypeMatchesSelection(trustType, selectedTrustTypes) {
    if (!trustType) return false;
    if (selectionHas(selectedTrustTypes, trustType)) return true;
    return selectionHas(selectedTrustTypes, ACUTE_PARENT) && isAcuteType(trustType);
}

export function normaliseAcuteSelection(types, acuteTypes, { asArray = false } = {}) {
    const next = new Set(Array.isArray(types) ? types : [...types]);
    if (
        next.has(ACUTE_PARENT) ||
        (acuteTypes.length > 0 && acuteTypes.every((type) => next.has(type)))
    ) {
        acuteTypes.forEach((type) => next.delete(type));
        next.add(ACUTE_PARENT);
    }
    return asArray ? Array.from(next).sort() : next;
}

/** `Care Trust` → `care`, `Acute - Teaching` → `acute-teaching`. */
export function encodeTrustTypeForUrl(type) {
    if (!type || typeof type !== 'string') return type;
    return TRUST_TYPE_URL_SLUGS[type] || type;
}

export function decodeTrustTypeFromUrl(value) {
    if (!value) return value;

    const trimmed = value.trim();
    const normalised = trimmed.toLowerCase();

    if (TRUST_TYPE_FROM_URL_SLUG.has(normalised)) {
        return TRUST_TYPE_FROM_URL_SLUG.get(normalised);
    }

    const knownName = Object.keys(TRUST_TYPE_URL_SLUGS).find(
        (name) => name.toLowerCase() === normalised
    );
    if (knownName) return knownName;

    return trimmed;
}

export const CANCER_ALLIANCE_NA = 'na';

/** Strip trailing "Cancer Alliance" for UI labels. */
export function cancerAllianceDisplayName(name) {
    if (typeof name !== 'string') return name;
    if (name === CANCER_ALLIANCE_NA) {
        return 'Not applicable';
    }
    const trimmed = name.replace(/\s+Cancer Alliance$/i, '').trim();
    return trimmed || name;
}

export function createEmptyScopeFilters() {
    return {
        trustTypes: [],
        regions: [],
        icbs: [],
        cancerAlliances: [],
        shelford: null
    };
}

function collectionSize(value) {
    if (!value) return 0;
    if (value instanceof Set) return value.size;
    return Array.isArray(value) ? value.length : 0;
}

export function normaliseScopeFilters(filters = {}) {
    const shelford =
        filters?.shelford === 'in' || filters?.shelford === 'not_in' ? filters.shelford : null;
    const cancerAlliances = Array.isArray(filters?.cancerAlliances)
        ? filters.cancerAlliances.map(String).filter(Boolean)
        : [];
    return {
        trustTypes: Array.isArray(filters?.trustTypes) ? filters.trustTypes.map(String).filter(Boolean) : [],
        regions: Array.isArray(filters?.regions) ? filters.regions.map(String).filter(Boolean) : [],
        icbs: Array.isArray(filters?.icbs) ? filters.icbs.map(String).filter(Boolean) : [],
        cancerAlliances,
        shelford
    };
}

export function hasAnyScopeFilters(filters) {
    if (!filters) return false;
    return (
        collectionSize(filters.trustTypes) > 0 ||
        collectionSize(filters.regions) > 0 ||
        collectionSize(filters.icbs) > 0 ||
        collectionSize(filters.cancerAlliances) > 0 ||
        filters.shelford === 'in' ||
        filters.shelford === 'not_in'
    );
}

export function applyScopeFiltersToSource({
    allItems,
    filters,
    getTrustType,
    getOrgsByRegionsOrICBs,
    getOrgsByCancerAlliances,
    orgShelfordGroup,
}) {
    const normalised = normaliseScopeFilters(filters);
    const orgList = filterTrustNamesByScope({
        allItems,
        getTrustType,
        getOrgsByRegionsOrICBs,
        getOrgsByCancerAlliances,
        orgShelfordGroup,
        selectedTrustTypes: normalised.trustTypes,
        selectedRegions: new Set(normalised.regions),
        selectedICBs: new Set(normalised.icbs),
        selectedCancerAlliances: new Set(normalised.cancerAlliances),
        shelfordFilter: normalised.shelford,
    });
    return {
        normalised,
        orgList,
        hasFilters: hasAnyScopeFilters(normalised),
    };
}

export function filterTrustNamesByScope({
    allItems,
    getTrustType,
    getOrgsByRegionsOrICBs,
    getOrgsByCancerAlliances,
    orgShelfordGroup,
    selectedTrustTypes,
    selectedRegions,
    selectedICBs,
    selectedCancerAlliances,
    shelfordFilter
}) {
    const trustTypeCount = collectionSize(selectedTrustTypes);
    let orgList = Array.from(allItems || []);

    if (trustTypeCount > 0 && getTrustType) {
        orgList = orgList.filter((name) =>
            trustTypeMatchesSelection(getTrustType(name), selectedTrustTypes)
        );
    }

    // AND across filter categories so each selected dimension narrows the cohort.
    if (collectionSize(selectedRegions) > 0 || collectionSize(selectedICBs) > 0) {
        const byRegionIcb = getOrgsByRegionsOrICBs
            ? getOrgsByRegionsOrICBs(selectedRegions, selectedICBs)
            : [];
        const regionSet = new Set(byRegionIcb);
        orgList = orgList.filter((name) => regionSet.has(name));
    }

    if (collectionSize(selectedCancerAlliances) > 0) {
        const byCancerAlliance = getOrgsByCancerAlliances
            ? getOrgsByCancerAlliances(selectedCancerAlliances)
            : [];
        const cancerSet = new Set(byCancerAlliance);
        orgList = orgList.filter((name) => cancerSet.has(name));
    }

    if (shelfordFilter !== null) {
        const map = orgShelfordGroup || new Map();
        orgList = orgList.filter((name) =>
            shelfordFilter === 'in' ? map.get(name) === true : !map.get(name)
        );
    }

    return orgList;
}
