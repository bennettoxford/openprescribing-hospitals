import { cancerAllianceDisplayName } from '../../../utils/scopeFilters.js';

export const ANALYSIS_SCOPE = {
    ALL: 'all',
    NATIONAL: 'national',
    GROUP: 'group',
};

export const SCOPE_OPTIONS = Object.values(ANALYSIS_SCOPE);

export const MODE_MAPPINGS = {
    trust: 'trust',
    region: 'region',
    icb: 'icb',
    national: 'national',
    product: 'product',
    productgroup: 'productGroup',
    product_group: 'productGroup',
    unit: 'unit',
    ingredient: 'ingredient',
};

export const AGGREGATION_CHART_MODES = ['national', 'icb', 'region'];

export function isAggregationChartMode(mode) {
    return AGGREGATION_CHART_MODES.includes(mode);
}

export function normaliseMode(mode) {
    if (!mode || typeof mode !== 'string') return null;
    const trimmed = mode.trim();
    if (!trimmed) return null;
    const lower = trimmed.toLowerCase();
    const normalised = lower.replace(/[-\s]/g, '_');
    return MODE_MAPPINGS[normalised] || MODE_MAPPINGS[lower] || null;
}

export const TRUST_PERCENTILE_MIN_COUNT = 30;
export const TRUST_OVERLAY_MAX_COUNT = 30;

export function normaliseScope(scopeValue) {
    if (!scopeValue || typeof scopeValue !== 'string') return ANALYSIS_SCOPE.ALL;
    const candidate = scopeValue.trim().toLowerCase();
    return SCOPE_OPTIONS.includes(candidate) ? candidate : ANALYSIS_SCOPE.ALL;
}

export function resolveAnalysisCohort(scope, {
    availableItems = [],
    allItems = [],
} = {}) {
    const normalisedScope = normaliseScope(scope);

    if (normalisedScope === ANALYSIS_SCOPE.GROUP) {
        return Array.from(availableItems || []);
    }
    if (normalisedScope === ANALYSIS_SCOPE.NATIONAL) {
        return [];
    }
    return Array.from(allItems || []);
}

export function getScopedTrustCodes(scope, {
    getOrgCode,
    availableItems = [],
} = {}) {
    const normalisedScope = normaliseScope(scope);

    if (normalisedScope === ANALYSIS_SCOPE.GROUP) {
        return Array.from(new Set(
            Array.from(availableItems || [])
                .map(name => getOrgCode?.(name))
                .filter(Boolean)
        ));
    }

    return [];
}

export function resolveNextOverlaySelection({
    overlayOrganisations,
} = {}) {
    if (Array.isArray(overlayOrganisations)) {
        return overlayOrganisations;
    }
    return [];
}

export function isNarrowedAnalysisScope(scope) {
    return normaliseScope(scope) === ANALYSIS_SCOPE.GROUP;
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
    return normalisedScope === ANALYSIS_SCOPE.GROUP && trustCount < TRUST_PERCENTILE_MIN_COUNT;
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

export function getModeDisplayName(mode, scope = ANALYSIS_SCOPE.ALL) {
    if (mode === 'national') {
        return getNationalModeLabel(scope);
    }
    const modeNames = {
        'trust': 'NHS Trust',
        'region': 'Region',
        'icb': 'ICB',
        'product': 'Product',
        'productGroup': 'Product group',
        'ingredient': 'Ingredient',
        'unit': 'Unit'
    };
    return modeNames[mode] || mode;
}

export function formatInScopePopulationPhrase({
    trustCount = null,
    filterDescription = '',
    includeArticle = true
} = {}) {
    const hasCount = Number.isFinite(trustCount) && trustCount > 0;
    const trustNoun = hasCount
        ? (trustCount === 1 ? '<strong>1 trust</strong>' : `<strong>${trustCount} trusts</strong>`)
        : 'trusts';
    const article = includeArticle && hasCount ? 'the ' : '';
    const filterSuffix = filterDescription
        ? ` (<strong>${filterDescription}</strong>)`
        : '';
    return `${article}${trustNoun} in the scope for this analysis${filterSuffix}`;
}

function getDefaultTrustPopulationPhrase(scope, scopeDetails = {}) {
    if (scope === ANALYSIS_SCOPE.GROUP) {
        return formatInScopePopulationPhrase({
            trustCount: scopeDetails.trustCount,
            filterDescription: scopeDetails.filterDescription,
            includeArticle: false
        });
    }
    return 'all NHS Trusts in England';
}

function getChartTrustContext(scope, {
    hasSelectedOrganisations,
    selectedTrustLabel,
    preposition = 'across',
    trustCount = null,
    filterDescription = '',
    inScopePopulation = '',
} = {}) {
    if (hasSelectedOrganisations) {
        const selectedContext = `${preposition} the ${selectedTrustLabel}`;
        if (scope === ANALYSIS_SCOPE.GROUP && inScopePopulation) {
            return `${selectedContext} among ${inScopePopulation}`;
        }
        return selectedContext;
    }
    if (scope === ANALYSIS_SCOPE.NATIONAL) {
        return 'nationally';
    }
    if (scope === ANALYSIS_SCOPE.GROUP) {
        return `${preposition} ${formatInScopePopulationPhrase({ trustCount, filterDescription })}`;
    }
    return `${preposition} all NHS Trusts`;
}

function createScopeCopyContext({
    scope = ANALYSIS_SCOPE.ALL,
    inScopeTrustCount = null,
    scopeFilterDescription = '',
    hasSelectedOrganisations = false,
    selectedOrganisationsCount = 0,
} = {}) {
    const resolvedScope = normaliseScope(scope);
    const scopeDetails = {
        trustCount: inScopeTrustCount,
        filterDescription: scopeFilterDescription,
    };
    const inScopePopulation = formatInScopePopulationPhrase({
        trustCount: inScopeTrustCount,
        filterDescription: scopeFilterDescription,
    });
    const defaultPopulation = getDefaultTrustPopulationPhrase(resolvedScope, scopeDetails);
    const singleSelectedTrust = hasSelectedOrganisations && selectedOrganisationsCount === 1;
    const selectedTrustLabel = singleSelectedTrust ? 'selected NHS Trust' : 'selected NHS Trusts';
    const selectedTrustsPhrase = singleSelectedTrust
        ? 'your selected NHS Trust'
        : `your ${selectedOrganisationsCount} selected NHS Trusts`;
    const otherTrustsLabel = resolvedScope === ANALYSIS_SCOPE.GROUP
        ? 'All other in-scope trusts'
        : 'All other trusts';
    const productTrustContext = getChartTrustContext(resolvedScope, {
        hasSelectedOrganisations,
        selectedTrustLabel,
        preposition: 'across',
        inScopePopulation,
        ...scopeDetails,
    });
    const groupedTrustContext = getChartTrustContext(resolvedScope, {
        hasSelectedOrganisations,
        selectedTrustLabel,
        preposition: hasSelectedOrganisations ? 'within' : 'across',
        inScopePopulation,
        ...scopeDetails,
    });

    return {
        scope: resolvedScope,
        scopeDetails,
        inScopePopulation,
        defaultPopulation,
        singleSelectedTrust,
        selectedTrustLabel,
        selectedTrustsPhrase,
        otherTrustsLabel,
        productTrustContext,
        groupedTrustContext,
    };
}

function formatFilterGroup(labelSingular, labelPlural, values) {
    if (!values.length) return null;
    const label = values.length === 1 ? labelSingular : labelPlural;
    return `${label}: ${values.join(', ')}`;
}

export function formatScopeFilterDescription(scopeFilters = {}) {
    const trustTypes = Array.isArray(scopeFilters.trustTypes)
        ? scopeFilters.trustTypes.map(String).filter(Boolean).map((type) => type.toLowerCase())
        : [];
    const regions = Array.isArray(scopeFilters.regions)
        ? scopeFilters.regions.map(String).filter(Boolean)
        : [];
    const icbs = Array.isArray(scopeFilters.icbs)
        ? scopeFilters.icbs.map(String).filter(Boolean)
        : [];
    const cancerAlliances = Array.isArray(scopeFilters.cancerAlliances)
        ? scopeFilters.cancerAlliances.map(String).filter(Boolean).map(cancerAllianceDisplayName)
        : [];

    const parts = [
        formatFilterGroup('trust type', 'trust types', trustTypes),
        formatFilterGroup('region', 'regions', regions),
        formatFilterGroup('ICB', 'ICBs', icbs),
        formatFilterGroup('cancer alliance', 'cancer alliances', cancerAlliances),
    ].filter(Boolean);

    if (scopeFilters.shelford === 'in') {
        parts.push('Shelford Group: included');
    } else if (scopeFilters.shelford === 'not_in') {
        parts.push('Shelford Group: excluded');
    }

    return parts.join('; ');
}

export function getChartExplainerText(mode, options = {}) {
    const {
        hasSelectedOrganisations = false,
        currentModeHasData = true,
        vmpsCount = 0,
        selectedOrganisationsCount = 0,
        scope = ANALYSIS_SCOPE.ALL,
        inScopeTrustCount = null,
        scopeFilterDescription = ''
    } = options;
    const {
        scope: resolvedScope,
        inScopePopulation,
        defaultPopulation,
        singleSelectedTrust,
        productTrustContext,
        groupedTrustContext,
    } = createScopeCopyContext({
        scope,
        inScopeTrustCount,
        scopeFilterDescription,
        hasSelectedOrganisations,
        selectedOrganisationsCount,
    });

    const selectedAmongScopeSuffix = resolvedScope === ANALYSIS_SCOPE.GROUP
        ? ` among ${inScopePopulation}`
        : '';

    const baseExplainers = {
        'trust': () => {
            if (hasSelectedOrganisations) {
                if (currentModeHasData) {
                    if (singleSelectedTrust) {
                        return `This chart shows quantities over time for your selected NHS Trust${selectedAmongScopeSuffix}.`;
                    }
                    return `This chart shows individual NHS Trust quantities over time for your selected trusts${selectedAmongScopeSuffix}. Each line represents one trust, allowing you to compare their usage patterns.`;
                }
                if (singleSelectedTrust) {
                    return `This chart would show NHS Trust quantities${selectedAmongScopeSuffix}, but the selected trust has no data for these products.`;
                }
                return `This chart would show individual NHS Trust quantities${selectedAmongScopeSuffix}, but the selected trusts have no data for these products.`;
            }
            if (resolvedScope === ANALYSIS_SCOPE.GROUP) {
                return `This chart shows individual NHS Trust quantities over time. Each line represents one trust, allowing you to compare usage patterns across ${inScopePopulation}.`;
            }
            return "This chart shows individual NHS Trust quantities over time. Each line represents one trust, allowing you to compare usage patterns across different trusts.";
        },

        'icb': () => {
            return `This chart shows quantities grouped by integrated care board (ICB) over time. Each line represents one ICB, combining data from ${defaultPopulation} within that area.`;
        },

        'region': () => {
            return `This chart shows quantities grouped by NHS region over time. Each line represents one region, combining data from ${defaultPopulation} within that area.`;
        },

        'national': () => {
            if (resolvedScope === ANALYSIS_SCOPE.GROUP) {
                return `This chart shows the total quantities over time across ${inScopePopulation}.`;
            }
            return `This chart shows the total national quantities over time, combining data from ${defaultPopulation}.`;
        },

        'product': () => {
            if (vmpsCount > 1) {
                return `This chart compares quantities between different products over time. Each line represents one product, showing its total usage ${productTrustContext}.`;
            }
            return `This chart shows quantities for the selected product over time ${productTrustContext}.`;
        },

        'productGroup': () => {
            return `This chart shows quantities grouped by product category (VTM - Virtual Therapeutic Moiety) over time ${groupedTrustContext}. Each line represents a therapeutic group, combining all related products.`;
        },

        'ingredient': () => {
            return `This chart shows quantities grouped by active ingredient over time ${groupedTrustContext}. Each line represents one ingredient, combining the amount of that ingredient in the selected products.`;
        },

        'unit': () => {
            return `This chart shows quantities grouped by unit of measurement over time ${groupedTrustContext}. Each line represents one unit type (e.g., tablets, bottles, ampoules).`;
        }
    };

    const explainerFunc = baseExplainers[mode];
    if (explainerFunc) {
        return explainerFunc();
    }

    return "This chart shows the selected data over time.";
}

export function getTableExplainerText(mode, options = {}) {
    const {
        hasSelectedTrusts = false,
        selectedTrustsCount = 0,
        selectedPeriod = 'all',
        latestMonth = '',
        latestYear = '',
        dateRange = '',
        scope = ANALYSIS_SCOPE.ALL,
        inScopeTrustCount = null,
        scopeFilterDescription = ''
    } = options;
    const {
        scope: resolvedScope,
        inScopePopulation,
        defaultPopulation,
        singleSelectedTrust,
        selectedTrustsPhrase,
        otherTrustsLabel,
    } = createScopeCopyContext({
        scope,
        inScopeTrustCount,
        scopeFilterDescription,
        hasSelectedOrganisations: hasSelectedTrusts,
        selectedOrganisationsCount: selectedTrustsCount,
    });
    const filteredAmongSuffix = resolvedScope === ANALYSIS_SCOPE.GROUP
        ? ` among ${inScopePopulation}`
        : '';

    function getPeriodText() {
        switch (selectedPeriod) {
            case 'all':
                return dateRange ? `across the period ${dateRange}` : 'across the entire period';
            case 'latest_month':
                return latestMonth ? `for ${latestMonth}` : 'for the latest month';
            case 'latest_year':
                return latestYear ? `for ${latestYear}` : 'for the latest year';
            case 'current_fy':
                if (latestMonth) {
                    const latestDate = new Date(latestMonth);
                    const fyStartYear = latestDate.getMonth() >= 3
                        ? latestDate.getFullYear()
                        : latestDate.getFullYear() - 1;
                    return `for the current financial year to date (April ${fyStartYear} - ${latestMonth})`;
                }
                return 'for the current financial year to date';
            case 'last_12_months':
                return 'for the last 12 months';
            default:
                return 'across the entire period';
        }
    }

    const periodText = getPeriodText();

    const baseExplainers = {
        'trust': () => {
            if (hasSelectedTrusts) {
                if (singleSelectedTrust) {
                    return `This table shows the total quantities of the selected products issued by NHS trust ${periodText}. Data is grouped into "Selected trust" and "${otherTrustsLabel}", allowing you to compare your selected NHS Trust against others${filteredAmongSuffix}.`;
                }
                return `This table shows the total quantities of the selected products issued by NHS trust ${periodText}. Data is grouped into "Selected trusts" (${selectedTrustsCount} trusts) and "${otherTrustsLabel}", allowing you to compare your selected NHS trusts against others${filteredAmongSuffix}.`;
            }
            if (resolvedScope === ANALYSIS_SCOPE.GROUP) {
                return `This table shows the total quantities of the selected products issued by NHS trust ${periodText} for ${inScopePopulation}.`;
            }
            return `This table shows the total quantities of the selected products issued by NHS trust ${periodText} for ${defaultPopulation}.`;
        },

        'region': () => {
            if (resolvedScope === ANALYSIS_SCOPE.GROUP) {
                return `This table shows the total quantities of the selected products issued by ${inScopePopulation}, grouped by NHS region ${periodText}.`;
            }
            return `This table shows the total quantities of the selected products issued across ${defaultPopulation}, grouped by NHS region ${periodText}.`;
        },

        'icb': () => {
            if (resolvedScope === ANALYSIS_SCOPE.GROUP) {
                return `This table shows the total quantities of the selected products issued by ${inScopePopulation}, grouped by integrated care board (ICB) ${periodText}.`;
            }
            return `This table shows the total quantities of the selected products issued across ${defaultPopulation}, grouped by integrated care board (ICB) ${periodText}.`;
        },

        'national': () => {
            if (resolvedScope === ANALYSIS_SCOPE.GROUP) {
                return `This table shows the total quantity of the selected products issued by ${inScopePopulation} ${periodText}.`;
            }
            return `This table shows the total national quantity of the selected products issued across ${defaultPopulation} ${periodText}.`;
        },

        'product': () => {
            if (hasSelectedTrusts) {
                return `This table shows the total quantities of each of the selected products issued ${periodText}, filtered to only include data from ${selectedTrustsPhrase}${filteredAmongSuffix}.`;
            }
            return `This table shows the total quantities of each of the selected products issued ${periodText}, across ${defaultPopulation}.`;
        },

        'productGroup': () => {
            if (hasSelectedTrusts) {
                return `This table shows the total quantities of the selected products issued by product group ${periodText}, filtered to only include data from ${selectedTrustsPhrase}${filteredAmongSuffix}.`;
            }
            return `This table shows the total quantities of the selected products issued by product group ${periodText} across ${defaultPopulation}.`;
        },

        'ingredient': () => {
            if (hasSelectedTrusts) {
                return `This table shows total quantities by active ingredient ${periodText}, filtered to only include data from ${selectedTrustsPhrase}${filteredAmongSuffix}.`;
            }
            return `This table shows total quantities by active ingredient ${periodText} across ${defaultPopulation}.`;
        },

        'unit': () => {
            if (hasSelectedTrusts) {
                return `This table shows total quantities by unit of measurement ${periodText}, filtered to only include data from ${selectedTrustsPhrase}${filteredAmongSuffix}.`;
            }
            return `This table shows total quantities by unit of measurement ${periodText} across ${defaultPopulation}.`;
        }
    };

    const explainerFunc = baseExplainers[mode];
    if (explainerFunc) {
        return explainerFunc();
    }

    return `This table shows the total quantities of the selected products issued ${periodText}.`;
}

export function getPercentileChartIntroText({
    selectedOrganisationsCount = 0,
    currentModeHasData = true,
    singleSelectedTrust = false,
    percentilePopulationLabel = 'all NHS Trusts',
} = {}) {
    if (selectedOrganisationsCount <= 0) {
        return `This chart shows percentile ranges across ${percentilePopulationLabel} with data for the selected products. The bands represent the variation in quantities across those trusts.`;
    }

    if (currentModeHasData) {
        return singleSelectedTrust
            ? `This chart shows the selected NHS Trust quantity overlaid on percentile ranges. The selected trust appears as a coloured line, while percentile bands show the distribution across ${percentilePopulationLabel} with data.`
            : `This chart shows individual NHS Trust quantities overlaid on percentile ranges. Selected trusts appear as coloured lines, while percentile bands show the distribution across ${percentilePopulationLabel} with data.`;
    }

    return singleSelectedTrust
        ? `This chart shows percentile ranges across ${percentilePopulationLabel} with data. The selected trust has no data for these products, but percentile bands show the distribution across ${percentilePopulationLabel} with data.`
        : `This chart shows percentile ranges across ${percentilePopulationLabel} with data. The selected trusts have no data for these products, but percentile bands show the distribution across ${percentilePopulationLabel} with data.`;
}
