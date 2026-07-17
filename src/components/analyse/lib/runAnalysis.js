import {
    ANALYSIS_SCOPE,
    resolveAnalysisCohort,
    getScopedTrustCodes,
    resolveNextOverlaySelection,
    buildAnalysisRequestPayload,
} from './analysisScope.js';
import {
    createEmptyScopeFilters,
    hasAnyScopeFilters,
    normaliseScopeFilters,
} from '../../../utils/scopeFilters.js';

export function validateAnalysisRun({
    selectedVMPs = [],
    selectedScope = ANALYSIS_SCOPE.ALL,
    selectedTrusts = [],
    selectedScopeFilters = createEmptyScopeFilters(),
    availableItems = [],
} = {}) {
    if (!selectedVMPs || selectedVMPs.length === 0) {
        return 'Please select at least one product or ingredient.';
    }
    if (selectedScope === ANALYSIS_SCOPE.TRUST && selectedTrusts.length === 0) {
        return 'Please select a trust for your single-trust analysis.';
    }
    if (selectedScope === ANALYSIS_SCOPE.GROUP && !hasAnyScopeFilters(selectedScopeFilters)) {
        return 'Please select at least one filter to define your trust group.';
    }
    if (
        selectedScope === ANALYSIS_SCOPE.GROUP
        && Array.from(availableItems || []).length === 0
    ) {
        return 'No trusts match the selected filters. Adjust your filters and try again.';
    }
    return null;
}

export function buildAnalysisRunPlan({
    selectedScope = ANALYSIS_SCOPE.ALL,
    selectedItems = [],
    availableItems = [],
    allItems = [],
    selectedTrusts = [],
    getOrgCode,
    overlayOrganisations,
} = {}) {
    const cohortTrusts = resolveAnalysisCohort(selectedScope, {
        selectedItems,
        availableItems,
        allItems,
    });
    const odsCodes = getScopedTrustCodes(selectedScope, selectedTrusts, {
        getOrgCode,
        availableItems,
    });
    const hasExplicitOverlay = Array.isArray(overlayOrganisations);
    const nextOverlaySelection = resolveNextOverlaySelection(selectedScope, {
        selectedItems,
        overlayOrganisations: hasExplicitOverlay ? overlayOrganisations : undefined,
    });

    return {
        cohortTrusts,
        odsCodes,
        hasExplicitOverlay,
        nextOverlaySelection,
    };
}

export function buildAnalysisResultsUpdateOptions({
    searchType,
    quantityType,
    selectedScope,
    selectedScopeFilters,
    plan,
    showPercentilesFromUrl = null,
    excludedVmps = [],
} = {}) {
    const updateOptions = {
        searchType,
        quantityType,
        selectedOrganisations: plan.nextOverlaySelection,
        scope: selectedScope,
        scopeFilters: selectedScope === ANALYSIS_SCOPE.GROUP
            ? normaliseScopeFilters(selectedScopeFilters)
            : createEmptyScopeFilters(),
        inScopeTrusts: plan.cohortTrusts,
    };

    if (showPercentilesFromUrl !== null) {
        updateOptions.showPercentiles = showPercentilesFromUrl === 'true';
    }
    if (excludedVmps.length > 0) {
        updateOptions.excludedVmps = excludedVmps;
    }
    return updateOptions;
}

export async function executeAnalysisFetch({
    fetchImpl = fetch,
    endpoint = '/api/get-quantity-data/',
    csrftoken,
    resolvedProducts,
    quantityType,
    selectedScope,
    odsCodes,
} = {}) {
    const response = await fetchImpl(endpoint, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrftoken,
        },
        body: JSON.stringify(
            buildAnalysisRequestPayload({
                selectedProducts: resolvedProducts,
                quantityType,
                scope: selectedScope,
                odsCodes,
            })
        ),
    });

    if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || `HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return { months: data.months ?? [], items: data.items ?? [] };
}

export function completeAnalysisRun({
    plan,
    payload,
    selectedVMPs = [],
    searchType,
    quantityType,
    selectedScope = ANALYSIS_SCOPE.ALL,
    selectedScopeFilters = createEmptyScopeFilters(),
    showPercentilesFromUrl = null,
    excludedVmps = [],
    analyseOptions,
    updateResults,
} = {}) {
    analyseOptions.setSelectedOrganisations(plan.nextOverlaySelection);
    if (!plan.hasExplicitOverlay && selectedScope !== ANALYSIS_SCOPE.TRUST) {
        analyseOptions.setRememberedOverlayOrganisations([]);
    }
    analyseOptions.runAnalysis({
        selectedVMPs,
        searchType,
        selectedOrganisations: plan.nextOverlaySelection,
    });

    updateResults(
        payload,
        buildAnalysisResultsUpdateOptions({
            searchType,
            quantityType,
            selectedScope,
            selectedScopeFilters,
            plan,
            showPercentilesFromUrl,
            excludedVmps,
        })
    );

    return {
        data: payload,
        searchType,
        selectedOrganisations: plan.nextOverlaySelection,
    };
}
