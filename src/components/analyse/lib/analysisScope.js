export const ANALYSIS_SCOPE = {
    ALL: 'all',
    NATIONAL: 'national',
    SINGLE: 'single',
    FILTERED: 'filtered',
};

export const SCOPE_OPTIONS = Object.values(ANALYSIS_SCOPE);

export const TRUST_PERCENTILE_MIN_COUNT = 30;
export const TRUST_OVERLAY_MAX_COUNT = 30;

export function normaliseScope(scopeValue) {
    if (!scopeValue || typeof scopeValue !== 'string') return ANALYSIS_SCOPE.ALL;
    const candidate = scopeValue.trim().toLowerCase();
    return SCOPE_OPTIONS.includes(candidate) ? candidate : ANALYSIS_SCOPE.ALL;
}

export function resolveAnalysisCohort(scope, {
    selectedItems = [],
    availableItems = [],
    allItems = [],
} = {}) {
    const normalisedScope = normaliseScope(scope);

    if (normalisedScope === ANALYSIS_SCOPE.FILTERED) {
        return Array.from(availableItems || []);
    }
    if (normalisedScope === ANALYSIS_SCOPE.SINGLE) {
        return (selectedItems || []).slice(0, 1);
    }
    if (normalisedScope === ANALYSIS_SCOPE.NATIONAL) {
        return [];
    }
    return Array.from(allItems || []);
}

export function getScopedTrustCodes(scope, selectedTrustNames = [], {
    getOrgCode,
    availableItems = [],
} = {}) {
    const normalisedScope = normaliseScope(scope);
    const uniqueSelectedTrusts = Array.from(new Set((selectedTrustNames || []).filter(Boolean)));
    const selectedTrustCodes = uniqueSelectedTrusts
        .map(name => getOrgCode?.(name))
        .filter(Boolean);

    if (normalisedScope === ANALYSIS_SCOPE.SINGLE) {
        return selectedTrustCodes;
    }

    if (normalisedScope === ANALYSIS_SCOPE.FILTERED) {
        return Array.from(new Set(
            Array.from(availableItems || [])
                .map(name => getOrgCode?.(name))
                .filter(Boolean)
        ));
    }

    return [];
}

export function resolveNextOverlaySelection(scope, {
    selectedItems = [],
    overlayOrganisations,
} = {}) {
    const normalisedScope = normaliseScope(scope);
    if (normalisedScope === ANALYSIS_SCOPE.SINGLE) {
        return (selectedItems || []).slice(0, 1);
    }
    if (Array.isArray(overlayOrganisations)) {
        return overlayOrganisations;
    }
    return [];
}

export function isNarrowedAnalysisScope(scope) {
    return scope === ANALYSIS_SCOPE.SINGLE || scope === ANALYSIS_SCOPE.FILTERED;
}

export function getNationalModeLabel(scope, { longForm = false } = {}) {
    if (isNarrowedAnalysisScope(scope)) return 'Total';
    return longForm ? 'National Total' : 'National';
}

export function buildAnalysisRequestPayload({
    selectedProducts = [],
    quantityType = null,
    scope = ANALYSIS_SCOPE.ALL,
    odsCodes = [],
}) {
    return {
        names: selectedProducts,
        quantity_type: quantityType,
        scope,
        ods_codes: Array.isArray(odsCodes) ? odsCodes : [],
    };
}

export function arePercentilesDisabled(scope, trustCount = 0) {
    const normalisedScope = normaliseScope(scope);
    return normalisedScope === ANALYSIS_SCOPE.SINGLE
        || (normalisedScope === ANALYSIS_SCOPE.FILTERED && trustCount < TRUST_PERCENTILE_MIN_COUNT);
}

export function computePercentileScopeConstraints({
    scope = ANALYSIS_SCOPE.ALL,
    inScopeTrusts = [],
    selectedOrganisations = [],
    showPercentiles = false,
    mode = null,
} = {}) {
    const trusts = Array.isArray(inScopeTrusts) ? inScopeTrusts : [];
    const disabled = arePercentilesDisabled(scope, trusts.length);
    const patches = {
        resultsStore: null,
        selectedOrganisations: null,
        percentilesDisabled: disabled,
    };

    if (disabled && showPercentiles) {
        patches.resultsStore = { showPercentiles: false };
    }

    if (
        disabled
        && mode === 'trust'
        && trusts.length > 0
        && (selectedOrganisations || []).length === 0
    ) {
        patches.selectedOrganisations = trusts;
    }

    return patches;
}
