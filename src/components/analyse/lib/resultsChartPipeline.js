import pluralize from 'pluralize';
import { formatNumber } from '../../../utils/utils.js';
import { ChartDataProcessor, calculatePercentiles } from './analyseData.js';
import { filterDatasetsByOverlaySelection } from './chartOverlay.js';

export function formatUnitForTooltip(baseUnit, value) {
    if (!baseUnit) return 'units';

    if (baseUnit.startsWith('DDD (') && baseUnit.includes(')')) {
        const doseMatch = baseUnit.match(/^DDD (\(.+\))$/);
        if (doseMatch) {
            const doseInfo = doseMatch[1];
            return value === 1 ? `DDD ${doseInfo}` : `DDDs ${doseInfo}`;
        }
    }

    if (baseUnit === 'DDD') {
        return value === 1 ? 'DDD' : 'DDDs';
    }

    return pluralize(baseUnit, value);
}

const TOOLTIP_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export function formatChartTooltipDate(date) {
    const d = new Date(date);
    return `${TOOLTIP_MONTHS[d.getMonth()]} ${d.getFullYear()}`;
}

export function buildResultsTooltipContent(d, { vmps = [], yAxisLabel = 'units' } = {}) {
    const label = d.dataset.label || 'No label';
    const value = d.value;
    let unit;
    if (d.dataset.isProduct && vmps.length > 0) {
        const matchingVmp = vmps.find(vmp => vmp.vmp === d.dataset.label);
        unit = formatUnitForTooltip(matchingVmp?.unit || yAxisLabel || 'unit', value);
    } else {
        unit = formatUnitForTooltip(yAxisLabel || 'units', value);
    }

    const tooltipContent = [
        { text: label, class: 'font-medium' },
        { label: 'Date', value: formatChartTooltipDate(d.date) },
        { label: 'Value', value: `${formatNumber(value)} ${unit}` },
    ];

    if (
        (d.dataset.isOrganisation || d.dataset.isProduct || d.dataset.isProductGroup)
        && d.dataset.numerator !== undefined
        && d.dataset.denominator !== undefined
    ) {
        tooltipContent.push(
            { label: 'Numerator', value: formatNumber(d.dataset.numerator[d.index], { addCommas: true }) },
            { label: 'Denominator', value: formatNumber(d.dataset.denominator[d.index], { addCommas: true }) },
        );
    }

    return tooltipContent;
}

export function buildChartExportOrganisations(selectedOrgNames, availableTrusts, orgMaps = {}) {
    const allNames = (selectedOrgNames && selectedOrgNames.length > 0)
        ? selectedOrgNames
        : availableTrusts;
    const {
        orgCodes = new Map(),
        orgRegions = new Map(),
        orgIcbs = new Map(),
        trustTypes = new Map(),
    } = orgMaps;

    return allNames.map(name => ({
        name,
        code: orgCodes.get?.(name) ?? null,
        region: orgRegions.get?.(name) ?? null,
        icb: orgIcbs.get?.(name) ?? null,
        trustType: trustTypes.get?.(name) ?? null,
    }));
}

export function buildVmpsFromSelectedData(selectedData = [], searchType) {
    const vmpGroups = selectedData.reduce((acc, item) => {
        const key = item.vmp__name;
        if (!acc[key]) {
            acc[key] = {
                vmp: item.vmp__name,
                code: item.vmp__code,
                vtm: item.vmp__vtm__name,
                ingredients: item.ingredient_names || [],
                units: new Set(),
                searchType,
            };
        }
        if (item.unit) {
            acc[key].units.add(item.unit);
        }
        return acc;
    }, {});

    return Object.values(vmpGroups)
        .filter(vmp => vmp.vmp)
        .map(vmp => ({
            ...vmp,
            unit: vmp.units.size > 0 ? Array.from(vmp.units).join(', ') : 'nan',
            ingredients: Array.isArray(vmp.ingredients) ? vmp.ingredients : (vmp.ingredients || []),
            vtm: vmp.vtm || '',
        }));
}

function resolveYAxisLabel(uniqueUnits, combinedUnits) {
    if (uniqueUnits.length === 1) {
        const unit = uniqueUnits[0];
        if (unit && unit.startsWith('DDD (')) return 'DDDs';
        if (unit === 'DDD') return 'DDDs';
        return pluralize(unit);
    }
    if (uniqueUnits.length > 1) {
        const allDDD = uniqueUnits.every(unit => unit && unit.startsWith('DDD ('));
        return allDDD ? 'DDDs' : combinedUnits;
    }
    return 'units';
}

export function buildResultsChartPipeline({
    data,
    mode,
    aggregatedData,
    months = [],
    selectedOrganisations = [],
    availableTrusts = [],
    showPercentiles = true,
    percentilesDisabled = false,
    scope = 'all',
    selectedRegions = [],
    selectedIcbs = [],
} = {}) {
    const effectiveSelectedOrganisations = selectedOrganisations.length > 0
        ? selectedOrganisations
        : (percentilesDisabled ? availableTrusts : []);

    const processor = new ChartDataProcessor(
        data,
        aggregatedData,
        {
            months,
            selectedOrganisations: effectiveSelectedOrganisations,
            showPercentiles: showPercentiles !== false,
            scope,
        }
    );

    const { datasets: chartDatasets, maxValue, needsPercentiles, percentilesData } = processor.processMode(mode);
    const combinedUnits = processor.getCombinedUnits();
    let finalDatasets = chartDatasets;
    let percentilesPatch = {
        percentiles: [],
        trustCount: 0,
        excludedTrusts: [],
    };

    if (mode === 'trust' && needsPercentiles && showPercentiles) {
        const percentilesResult = calculatePercentiles(
            percentilesData,
            availableTrusts,
            months
        );
        percentilesPatch = {
            percentiles: percentilesResult.percentiles,
            trustCount: percentilesResult.trustCount,
            excludedTrusts: percentilesResult.excludedTrusts,
        };
        if (percentilesResult.percentiles.length > 0) {
            const percentileDatasets = processor.createPercentileDatasets(percentilesResult.percentiles);
            finalDatasets = [...percentileDatasets, ...chartDatasets];
        }
    }

    if (mode === 'region') {
        finalDatasets = filterDatasetsByOverlaySelection(finalDatasets, selectedRegions);
    }
    if (mode === 'icb') {
        finalDatasets = filterDatasetsByOverlaySelection(finalDatasets, selectedIcbs);
    }

    const yAxisLabel = resolveYAxisLabel(processor.uniqueUnits, combinedUnits);

    const chartConfig = {
        mode,
        yAxisLabel,
        yAxisRange: maxValue > 0 ? [0, maxValue] : [0, 100],
        visibleItems: new Set(finalDatasets.map(d => d.label)),
        yAxisBehavior: {
            forceZero: true,
            padTop: 1.1,
            resetToInitial: true,
        },
        yAxisTickFormat: value => {
            const range = maxValue;
            let decimals = 1;
            if (typeof value === 'number' && !isNaN(value)) {
                const step = range / 10;
                if (step > 0) {
                    decimals = Math.min(
                        Math.max(1, Math.ceil(Math.abs(Math.log10(step))) + 1),
                        5
                    );
                }
            }
            return formatNumber(value, { maxDecimals: decimals });
        },
        tooltipValueFormat: value => {
            const baseUnit = combinedUnits || 'units';
            const pluralizedUnit = formatUnitForTooltip(baseUnit, value);
            return formatNumber(value, { showUnit: true, unit: pluralizedUnit });
        },
    };

    return {
        chartData: {
            labels: processor.allDates,
            datasets: finalDatasets,
        },
        chartConfig,
        percentilesPatch,
    };
}
