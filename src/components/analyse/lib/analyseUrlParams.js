import {
    encodeTrustTypeForUrl,
    normaliseScopeFilters,
} from '../../../utils/scopeFilters.js';
import { formatArrayParam, parseArrayParam, setUrlParams } from '../../../utils/utils.js';
import {
    ANALYSIS_SCOPE,
    isAggregationChartMode,
    normaliseMode,
    normaliseScope,
} from './analysisScope.js';
import {
    allOverlaySelection,
    encodeChartOverlayParam,
    parseChartOverlayParam,
} from './chartOverlay.js';

export const PRODUCT_PARAM_BY_TYPE = {
    vmp: 'vmps',
    vtm: 'vtms',
    ingredient: 'ingredients',
    atc: 'atcs',
};

export const PRODUCT_TYPE_ORDER = Object.keys(PRODUCT_PARAM_BY_TYPE);

export const ANALYSIS_TRUSTS_PARAM = 'trusts';
export const ANALYSIS_SCOPE_PARAM = 'scope';
export const ANALYSIS_TRUST_TYPES_PARAM = 'trust_types';
export const ANALYSIS_REGIONS_PARAM = 'regions';
export const ANALYSIS_ICBS_PARAM = 'icbs';
export const ANALYSIS_CHART_REGIONS_PARAM = 'chart_regions';
export const ANALYSIS_CHART_ICBS_PARAM = 'chart_icbs';
export const ANALYSIS_CANCER_ALLIANCES_PARAM = 'cancer_alliances';
export const ANALYSIS_SHELFORD_PARAM = 'shelford';
export const ANALYSIS_QUANTITY_PARAM = 'quantity';
export const ANALYSIS_MODE_PARAM = 'mode';
export const ANALYSIS_PERCENTILES_PARAM = 'show_percentiles';
export const ANALYSIS_EXCLUDED_VMPS_PARAM = 'excluded_vmps';

export const QUANTITY_TYPE_CODES = {
    'SCMD Quantity': 'scmd',
    'Unit Dose Quantity': 'dose',
    'Ingredient Quantity': 'ingredient',
    'Defined Daily Dose Quantity': 'ddd',
};

export const SUPPORTED_ANALYSIS_PARAMS = [
    ...Object.values(PRODUCT_PARAM_BY_TYPE),
    ANALYSIS_TRUSTS_PARAM,
    ANALYSIS_SCOPE_PARAM,
    ANALYSIS_TRUST_TYPES_PARAM,
    ANALYSIS_REGIONS_PARAM,
    ANALYSIS_ICBS_PARAM,
    ANALYSIS_CHART_REGIONS_PARAM,
    ANALYSIS_CHART_ICBS_PARAM,
    ANALYSIS_CANCER_ALLIANCES_PARAM,
    ANALYSIS_SHELFORD_PARAM,
    ANALYSIS_QUANTITY_PARAM,
    ANALYSIS_MODE_PARAM,
    ANALYSIS_PERCENTILES_PARAM,
    ANALYSIS_EXCLUDED_VMPS_PARAM,
];

export function encodeQuantityType(quantityType) {
    if (!quantityType) return null;
    const trimmed = quantityType.trim();
    return QUANTITY_TYPE_CODES[trimmed] || null;
}

export function getShowPercentilesParam(mode, trusts, showPercentilesValue, scope = 'all') {
    if (scope === ANALYSIS_SCOPE.TRUST || mode !== 'trust') return null;
    const hasTrusts = Array.isArray(trusts) && trusts.length > 0;
    if (hasTrusts && showPercentilesValue === true) return 'true';
    return null;
}

export function getRegionCodeMaps(hierarchy = []) {
    const regions = hierarchy || [];
    const regionCodeByName = new Map(
        regions
            .filter(region => region?.region && region?.region_code)
            .map(region => [region.region, region.region_code])
    );
    const regionNameByCode = new Map(
        regions
            .filter(region => region?.region && region?.region_code)
            .map(region => [region.region_code, region.region])
    );
    const icbCodeByName = new Map(
        regions
            .flatMap(region => region?.icbs || [])
            .filter(icb => icb?.name && icb?.code)
            .map(icb => [icb.name, icb.code])
    );
    const icbNameByCode = new Map(
        regions
            .flatMap(region => region?.icbs || [])
            .filter(icb => icb?.name && icb?.code)
            .map(icb => [icb.code, icb.name])
    );
    return { regionCodeByName, regionNameByCode, icbCodeByName, icbNameByCode };
}

export function getCancerAllianceCodeMaps(cancerAlliances = []) {
    const codeByName = new Map();
    const nameByCode = new Map();
    (cancerAlliances || []).forEach((alliance) => {
        const name = alliance?.name;
        const code = alliance?.code;
        if (!name || !code) return;
        codeByName.set(name, code);
        nameByCode.set(code, name);
    });
    return { codeByName, nameByCode };
}

export function encodeCancerAllianceForUrl(name, codeByName = new Map()) {
    if (!name) return name;
    return codeByName.get(name) || name;
}

export function decodeCancerAllianceFromUrl(value, nameByCode = new Map()) {
    if (!value) return value;
    const trimmed = String(value).trim();
    return nameByCode.get(trimmed) || trimmed;
}

export function buildAnalysisUrlParams({
    products = [],
    trusts = [],
    quantityType = null,
    scope = 'all',
    scopeFilters = {},
    mode = null,
    showPercentiles = null,
    excludedVmps = [],
    chartRegions = null,
    chartIcbs = null,
    getOrgCode,
    regionCodeByName = new Map(),
    icbCodeByName = new Map(),
    cancerAllianceCodeByName = new Map(),
} = {}) {
    const params = {};

    const productGroups = {};
    products.forEach(item => {
        if (!item?.code || !item?.type) return;
        const typeKey = item.type.toLowerCase();
        const paramName = PRODUCT_PARAM_BY_TYPE[typeKey];
        if (!paramName) return;

        if (!productGroups[paramName]) productGroups[paramName] = [];
        if (!productGroups[paramName].includes(item.code)) {
            productGroups[paramName].push(item.code);
        }
    });

    Object.entries(productGroups).forEach(([paramName, codes]) => {
        if (codes.length > 0) {
            params[paramName] = formatArrayParam(codes);
        }
    });

    const trustCodes = (trusts || []).map(name => getOrgCode?.(name)).filter(Boolean);
    if (trustCodes.length > 0) {
        params[ANALYSIS_TRUSTS_PARAM] = formatArrayParam(trustCodes);
    }

    const normalisedScope = normaliseScope(scope);
    if (normalisedScope !== 'all') {
        params[ANALYSIS_SCOPE_PARAM] = normalisedScope;
    }

    if (normalisedScope === ANALYSIS_SCOPE.GROUP) {
        const setScopeFilterParam = (paramName, values, encode = value => value) => {
            const encoded = Array.isArray(values)
                ? Array.from(new Set(values.map(String).filter(Boolean).map(encode).filter(Boolean))).sort()
                : [];
            if (encoded.length > 0) {
                params[paramName] = formatArrayParam(encoded);
            }
        };

        setScopeFilterParam(ANALYSIS_TRUST_TYPES_PARAM, scopeFilters?.trustTypes, encodeTrustTypeForUrl);
        setScopeFilterParam(ANALYSIS_REGIONS_PARAM, scopeFilters?.regions, name => regionCodeByName.get(name) || name);
        setScopeFilterParam(ANALYSIS_ICBS_PARAM, scopeFilters?.icbs, name => icbCodeByName.get(name) || name);
        setScopeFilterParam(
            ANALYSIS_CANCER_ALLIANCES_PARAM,
            scopeFilters?.cancerAlliances,
            name => encodeCancerAllianceForUrl(name, cancerAllianceCodeByName)
        );

        if (scopeFilters?.shelford === 'in' || scopeFilters?.shelford === 'not_in') {
            params[ANALYSIS_SHELFORD_PARAM] = scopeFilters.shelford;
        }
    }

    if (quantityType) {
        const encodedQuantity = encodeQuantityType(quantityType);
        if (encodedQuantity) {
            params[ANALYSIS_QUANTITY_PARAM] = encodedQuantity;
        }
    }

    if (mode) {
        const normalisedMode = normaliseMode(mode);
        const shouldForceModeParam = showPercentiles === 'true';
        if (normalisedMode && (normalisedMode !== 'trust' || shouldForceModeParam)) {
            params[ANALYSIS_MODE_PARAM] = normalisedMode;
        }
    }

    if (showPercentiles !== null) {
        params[ANALYSIS_PERCENTILES_PARAM] = showPercentiles;
    }

    if ((excludedVmps || []).length > 0) {
        params[ANALYSIS_EXCLUDED_VMPS_PARAM] = formatArrayParam(excludedVmps);
    }

    const encodedChartRegions = encodeChartOverlayParam(chartRegions, regionCodeByName);
    if (encodedChartRegions) {
        params[ANALYSIS_CHART_REGIONS_PARAM] = encodedChartRegions;
    }

    const encodedChartIcbs = encodeChartOverlayParam(chartIcbs, icbCodeByName);
    if (encodedChartIcbs) {
        params[ANALYSIS_CHART_ICBS_PARAM] = encodedChartIcbs;
    }

    return params;
}

export function updateAnalysisUrl(urlState, setParams = setUrlParams) {
    if (typeof window === 'undefined') return;
    const params = buildAnalysisUrlParams(urlState);
    setParams(params, SUPPORTED_ANALYSIS_PARAMS);
}

export function buildValidationParams(urlParams, showPercentiles = null) {
    const queryParams = new URLSearchParams();

    PRODUCT_TYPE_ORDER.forEach(type => {
        const paramName = PRODUCT_PARAM_BY_TYPE[type];
        const paramValue = urlParams.get(paramName);
        if (paramValue?.trim()) {
            queryParams.append(paramName, paramValue);
        }
    });

    [ANALYSIS_TRUSTS_PARAM, ANALYSIS_QUANTITY_PARAM, ANALYSIS_MODE_PARAM,
        ANALYSIS_EXCLUDED_VMPS_PARAM].forEach(param => {
        const value = urlParams.get(param);
        if (value?.trim()) {
            queryParams.append(param, value.trim());
        }
    });

    if (showPercentiles?.trim()) {
        queryParams.append(ANALYSIS_PERCENTILES_PARAM, showPercentiles.trim());
    }

    return queryParams;
}

export function parseScopeFiltersFromUrl(urlParams, {
    regionNameByCode = new Map(),
    icbNameByCode = new Map(),
    cancerAllianceNameByCode = new Map(),
    decodeTrustTypeFromUrl = value => value,
} = {}) {
    const trustTypes = parseArrayParam(urlParams.get(ANALYSIS_TRUST_TYPES_PARAM)).map(decodeTrustTypeFromUrl);
    const regions = parseArrayParam(urlParams.get(ANALYSIS_REGIONS_PARAM)).map(value => regionNameByCode.get(value) || value);
    const icbs = parseArrayParam(urlParams.get(ANALYSIS_ICBS_PARAM)).map(value => icbNameByCode.get(value) || value);
    const cancerAlliances = parseArrayParam(urlParams.get(ANALYSIS_CANCER_ALLIANCES_PARAM)).map(
        value => decodeCancerAllianceFromUrl(value, cancerAllianceNameByCode)
    );
    const shelfordParam = (urlParams.get(ANALYSIS_SHELFORD_PARAM) || '').trim().toLowerCase();
    const shelford = shelfordParam === 'in' || shelfordParam === 'not_in' ? shelfordParam : null;

    return normaliseScopeFilters({
        trustTypes,
        regions,
        icbs,
        cancerAlliances,
        shelford,
    });
}

export function planAnalysisHydrate({
    urlParams,
    regionsHierarchy = [],
    cancerAlliances = [],
    decodeTrustTypeFromUrl = value => value,
} = {}) {
    const scope = normaliseScope(urlParams.get(ANALYSIS_SCOPE_PARAM));
    const mode = normaliseMode(urlParams.get(ANALYSIS_MODE_PARAM));
    const hasExplicitTrustSelection = parseArrayParam(urlParams.get(ANALYSIS_TRUSTS_PARAM)).length > 0;
    const { regionNameByCode, icbNameByCode } = getRegionCodeMaps(regionsHierarchy);
    const { nameByCode: cancerAllianceNameByCode } = getCancerAllianceCodeMaps(cancerAlliances);

    const scopeFilters = parseScopeFiltersFromUrl(urlParams, {
        regionNameByCode,
        icbNameByCode,
        cancerAllianceNameByCode,
        decodeTrustTypeFromUrl,
    });

    const rawShowPercentiles = urlParams.get(ANALYSIS_PERCENTILES_PARAM);
    const showPercentilesRaw =
        scope === ANALYSIS_SCOPE.TRUST ||
        (rawShowPercentiles?.toLowerCase() === 'true' && mode !== 'trust')
            ? null
            : rawShowPercentiles;

    return {
        scope,
        scopeFilters,
        mode,
        showPercentilesRaw,
        chartRegions: mode === 'region'
            ? parseChartOverlayParam(urlParams.get(ANALYSIS_CHART_REGIONS_PARAM), regionNameByCode)
            : allOverlaySelection(),
        chartIcbs: mode === 'icb'
            ? parseChartOverlayParam(urlParams.get(ANALYSIS_CHART_ICBS_PARAM), icbNameByCode)
            : allOverlaySelection(),
        hasExplicitTrustSelection,
        validationQuery: buildValidationParams(urlParams, showPercentilesRaw),
        shouldShowAdvancedOptions: scope !== ANALYSIS_SCOPE.ALL,
    };
}

export function planValidationStorePatch({
    data,
    scope = ANALYSIS_SCOPE.ALL,
    mode = null,
    showPercentilesRaw = null,
    hasExplicitTrustSelection = false,
} = {}) {
    const hydratedTrustNames = Array.isArray(data?.valid_trusts)
        ? data.valid_trusts.map(trust => trust.ods_name).filter(Boolean)
        : [];

    const stashTrustsForAggregation =
        scope !== ANALYSIS_SCOPE.TRUST &&
        hasExplicitTrustSelection &&
        isAggregationChartMode(mode);

    const selectedOrganisations = scope === ANALYSIS_SCOPE.TRUST
        ? hydratedTrustNames.slice(0, 1)
        : (hasExplicitTrustSelection && !stashTrustsForAggregation ? hydratedTrustNames : []);

    const rememberedOverlayOrganisations = stashTrustsForAggregation
        ? hydratedTrustNames
        : (scope !== ANALYSIS_SCOPE.TRUST && hasExplicitTrustSelection ? hydratedTrustNames : []);

    const organisationSelection = scope === ANALYSIS_SCOPE.TRUST ? selectedOrganisations : [];
    const hasValidTrusts = selectedOrganisations.length > 0;
    const excludedVmps = Array.isArray(data?.excluded_vmps)
        ? Array.from(new Set(data.excluded_vmps.map(String).filter(Boolean))).sort()
        : [];

    let showPercentiles;
    if (showPercentilesRaw === 'true') {
        showPercentiles = true;
    } else if (showPercentilesRaw === 'false') {
        showPercentiles = false;
    } else if (data?.mode === 'trust' && hasValidTrusts) {
        showPercentiles = data.show_percentiles ?? false;
    } else {
        showPercentiles = !hasValidTrusts;
    }

    return {
        selectedVMPs: Array.isArray(data?.valid_products) ? data.valid_products : null,
        quantityType: data?.quantity_type || null,
        selectedOrganisations,
        rememberedOverlayOrganisations,
        organisationSelection,
        showPercentiles,
        excludedVmps,
        mode: mode || null,
    };
}

export function applyHydrateStorePatch(patch, {
    analyseOptions,
    organisationSearchStore,
    resultsStore,
    modeSelectorStore,
} = {}) {
    if (!patch) return;

    if (patch.selectedVMPs?.length > 0) {
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: patch.selectedVMPs,
        }));
    }

    organisationSearchStore.updateSelection(patch.organisationSelection);
    analyseOptions.setSelectedOrganisations(patch.selectedOrganisations);
    analyseOptions.setRememberedOverlayOrganisations(patch.rememberedOverlayOrganisations);
    resultsStore.update(store => ({
        ...store,
        showPercentiles: patch.showPercentiles,
        excludedVmps: patch.excludedVmps,
    }));

    if (patch.mode) {
        modeSelectorStore.setSelectedMode(patch.mode);
    }
}

export async function finishValidatedHydrate({
    patch,
    selectedScope,
    selectedScopeFilters,
    analyseOptions,
    organisationSearchStore,
    resultsStore,
    modeSelectorStore,
    applyQuantityType,
    cleanupUrlParams,
    waitForUi,
} = {}) {
    applyHydrateStorePatch(patch, {
        analyseOptions,
        organisationSearchStore,
        resultsStore,
        modeSelectorStore,
    });

    if (patch.selectedVMPs?.length > 0) {
        await applyQuantityType?.(patch);
    }

    cleanupUrlParams?.();
    await waitForUi?.();

    if (selectedScope === ANALYSIS_SCOPE.GROUP) {
        organisationSearchStore.applyScopeFilters(selectedScopeFilters);
    }

    return {
        excludedVmps: patch.excludedVmps || [],
    };
}
