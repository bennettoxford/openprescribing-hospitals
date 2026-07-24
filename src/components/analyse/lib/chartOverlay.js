import { formatArrayParam, parseArrayParam } from '../../../utils/utils.js';
import { isAggregationChartMode, TRUST_OVERLAY_MAX_COUNT } from './analysisScope.js';

export const CHART_OVERLAY_NONE = 'none';

export const OVERLAY_KIND = {
    ALL: 'all',
    SELECTED: 'selected',
};

export function allOverlaySelection() {
    return { kind: OVERLAY_KIND.ALL };
}

export function selectedOverlaySelection(items = []) {
    return {
        kind: OVERLAY_KIND.SELECTED,
        items: Array.isArray(items) ? [...items] : [],
    };
}

export function normaliseOverlaySelection(rawValue) {
    if (rawValue && typeof rawValue === 'object' && !Array.isArray(rawValue)
        && rawValue.kind === OVERLAY_KIND.SELECTED) {
        return selectedOverlaySelection(rawValue.items);
    }
    return allOverlaySelection();
}

export function isAllOverlaySelection(selection) {
    return normaliseOverlaySelection(selection).kind === OVERLAY_KIND.ALL;
}

export function overlaySelectionItems(selection, availableItems = []) {
    const normalised = normaliseOverlaySelection(selection);
    if (normalised.kind === OVERLAY_KIND.ALL) {
        return Array.from(availableItems || []);
    }
    return normalised.items;
}

export function encodeChartOverlayParam(selection, codeByName) {
    const normalised = normaliseOverlaySelection(selection);
    if (normalised.kind === OVERLAY_KIND.ALL) return null;
    if (normalised.items.length === 0) return CHART_OVERLAY_NONE;
    const codes = Array.from(new Set(
        normalised.items.map(name => codeByName.get(name) || name).filter(Boolean)
    )).sort();
    return codes.length > 0 ? formatArrayParam(codes) : CHART_OVERLAY_NONE;
}

export function parseChartOverlayParam(rawValue, nameByCode) {
    if (rawValue == null) return allOverlaySelection();
    const trimmed = String(rawValue).trim();
    if (!trimmed || trimmed.toLowerCase() === CHART_OVERLAY_NONE) {
        return selectedOverlaySelection([]);
    }
    return selectedOverlaySelection(
        parseArrayParam(trimmed).map(value => nameByCode.get(value) || value).filter(Boolean)
    );
}

export function isDefaultAllSelection(selectedItems = [], availableItems = []) {
    const sortedSelected = [...(selectedItems || [])].sort();
    const sortedAvailable = [...(availableItems || [])].sort();
    return sortedSelected.length > 0
        && sortedSelected.length === sortedAvailable.length
        && sortedSelected.every((value, index) => value === sortedAvailable[index]);
}

export function toStoredOverlaySelection(selectedItems = [], availableItems = []) {
    const nextSelection = Array.isArray(selectedItems) ? selectedItems : [];
    if (isDefaultAllSelection(nextSelection, availableItems)) {
        return allOverlaySelection();
    }
    return selectedOverlaySelection(nextSelection);
}

export function uniqueFieldValues(data, key) {
    return Array.from(new Set(
        (data || []).map(item => item?.[key]).filter(Boolean)
    )).sort();
}

export function filterDatasetsByOverlaySelection(datasets = [], selected = []) {
    const selectedSet = new Set(selected);
    return datasets.filter(dataset => selectedSet.has(dataset.label));
}

export function syncDimensionSelection({
    available = [],
    selected = [],
    selection = allOverlaySelection(),
} = {}) {
    const availableItems = Array.isArray(available) ? available : [];
    const normalised = normaliseOverlaySelection(selection);
    const baseSelected = normalised.kind === OVERLAY_KIND.ALL
        ? [...availableItems]
        : (selected || []).filter(item => availableItems.includes(item));
    const validSelected = normalised.kind === OVERLAY_KIND.ALL
        ? [...availableItems]
        : baseSelected;
    const selectionChanged = normalised.kind === OVERLAY_KIND.ALL
        ? false
        : validSelected.length !== (selected || []).length
            || validSelected.some((item, index) => item !== (selected || [])[index]);

    if (validSelected.length === 0 && availableItems.length > 0) {
        return {
            selected: [...availableItems],
            selection: normalised.kind === OVERLAY_KIND.SELECTED
                ? allOverlaySelection()
                : normalised,
            storeValue: normalised.kind === OVERLAY_KIND.SELECTED
                ? allOverlaySelection()
                : undefined,
            didChange: true,
        };
    }

    if (selectionChanged && normalised.kind === OVERLAY_KIND.SELECTED) {
        const storeValue = toStoredOverlaySelection(validSelected, availableItems);
        return {
            selected: validSelected,
            selection: storeValue,
            storeValue,
            didChange: true,
        };
    }

    if (selectionChanged) {
        return {
            selected: validSelected,
            selection: allOverlaySelection(),
            storeValue: undefined,
            didChange: true,
        };
    }

    return {
        selected: validSelected,
        selection: normalised,
        storeValue: undefined,
        didChange: false,
    };
}

export function clampTrustOverlaySelection(items = [], maxCount = TRUST_OVERLAY_MAX_COUNT) {
    if (!Array.isArray(items)) return [];
    return items.length > maxCount ? items.slice(0, maxCount) : items;
}

export function nextStashedOverlayState(currentSelected = [], currentRemembered = []) {
    const current = Array.isArray(currentSelected) ? currentSelected : [];
    if (current.length === 0) {
        return {
            selectedOrganisations: [],
            rememberedOverlayOrganisations: Array.isArray(currentRemembered) ? currentRemembered : [],
        };
    }
    return {
        selectedOrganisations: [],
        rememberedOverlayOrganisations: clampTrustOverlaySelection(current),
    };
}

export function nextRestoredOverlayState(currentSelected = [], remembered = []) {
    const selected = Array.isArray(currentSelected) ? currentSelected : [];
    const rememberedItems = Array.isArray(remembered) ? remembered : [];
    if (selected.length > 0 || rememberedItems.length === 0) {
        return null;
    }
    return {
        selectedOrganisations: clampTrustOverlaySelection(rememberedItems),
    };
}

export function overlayTrustsForModeChange(fromMode, toMode, {
    selectedOrganisations = [],
    rememberedOverlayOrganisations = [],
} = {}) {
    if (isAggregationChartMode(toMode) && !isAggregationChartMode(fromMode)) {
        return {
            type: 'stash',
            patch: nextStashedOverlayState(selectedOrganisations, rememberedOverlayOrganisations),
        };
    }
    if (!isAggregationChartMode(toMode) && isAggregationChartMode(fromMode)) {
        const restored = nextRestoredOverlayState(
            selectedOrganisations,
            rememberedOverlayOrganisations
        );
        return restored
            ? { type: 'restore', patch: restored }
            : { type: 'none', patch: null };
    }
    return { type: 'none', patch: null };
}

export function commitDimensionSelection(selectedItems = [], availableItems = []) {
    const nextSelection = Array.isArray(selectedItems) ? selectedItems : [];
    return {
        selected: nextSelection,
        selection: toStoredOverlaySelection(nextSelection, availableItems),
    };
}

export function hydrateOverlayLocals(storeSelection, availableItems = []) {
    const selection = normaliseOverlaySelection(storeSelection);
    return {
        selected: overlaySelectionItems(selection, availableItems),
        selection,
    };
}

export function resolveChartOverlayLocals(analyseOptions, selectedData = []) {
    const regionAvailable = uniqueFieldValues(selectedData, 'organisation__region');
    const icbAvailable = uniqueFieldValues(selectedData, 'organisation__icb');
    const regions = hydrateOverlayLocals(analyseOptions.selectedChartRegions, regionAvailable);
    const icbs = hydrateOverlayLocals(analyseOptions.selectedChartIcbs, icbAvailable);
    return {
        selectedRegions: regions.selected,
        regionOverlaySelection: regions.selection,
        selectedIcbs: icbs.selected,
        icbOverlaySelection: icbs.selection,
    };
}

export function commitChartDimensionSelection(dimension, selectedItems, availableItems = []) {
    const nextSelection = Array.isArray(selectedItems) ? selectedItems : [];
    if (nextSelection.length === 0) {
        return null;
    }
    const committed = commitDimensionSelection(nextSelection, availableItems);
    return {
        dimension,
        selected: committed.selected,
        selection: committed.selection,
    };
}

export function buildResultsModeSearchSync({
    selectedMode,
    availableTrusts = [],
    selectedOrganisations = [],
    selectedRegions = [],
    selectedIcbs = [],
    regionOverlaySelection,
    icbOverlaySelection,
    selectedData = [],
} = {}) {
    if (selectedMode === 'trust') {
        const cappedOrganisations = clampTrustOverlaySelection(selectedOrganisations);
        return {
            filterType: 'trust',
            availableItems: availableTrusts,
            selectedItems: cappedOrganisations,
            selectedOrganisations: cappedOrganisations.length !== selectedOrganisations.length
                ? cappedOrganisations
                : undefined,
            selectedRegions,
            selectedIcbs,
            regionOverlaySelection,
            icbOverlaySelection,
            chartRegionsStoreValue: undefined,
            chartIcbsStoreValue: undefined,
        };
    }

    if (selectedMode !== 'region' && selectedMode !== 'icb') {
        return null;
    }

    const field = selectedMode === 'region' ? 'organisation__region' : 'organisation__icb';
    const availableItems = uniqueFieldValues(selectedData, field);
    const localSelected = selectedMode === 'region' ? selectedRegions : selectedIcbs;
    const selection = selectedMode === 'region' ? regionOverlaySelection : icbOverlaySelection;
    const synced = syncDimensionSelection({
        available: availableItems,
        selected: localSelected,
        selection,
    });

    return {
        filterType: selectedMode,
        availableItems,
        selectedItems: synced.selected,
        selectedOrganisations: undefined,
        selectedRegions: selectedMode === 'region' ? synced.selected : selectedRegions,
        selectedIcbs: selectedMode === 'icb' ? synced.selected : selectedIcbs,
        regionOverlaySelection: selectedMode === 'region' ? synced.selection : regionOverlaySelection,
        icbOverlaySelection: selectedMode === 'icb' ? synced.selection : icbOverlaySelection,
        chartRegionsStoreValue: selectedMode === 'region' ? synced.storeValue : undefined,
        chartIcbsStoreValue: selectedMode === 'icb' ? synced.storeValue : undefined,
    };
}

export function buildOrganisationSelectionPatch({
    selectedMode,
    selectedItems = [],
    selectedData = [],
    selectedRegions = [],
    selectedIcbs = [],
} = {}) {
    if (selectedMode === 'trust') {
        const nextSelection = clampTrustOverlaySelection(selectedItems);
        return {
            type: 'trust',
            selectedOrganisations: nextSelection,
            searchSelection: nextSelection,
        };
    }

    if (selectedMode === 'region' || selectedMode === 'icb') {
        const field = selectedMode === 'region' ? 'organisation__region' : 'organisation__icb';
        const availableItems = uniqueFieldValues(selectedData, field);
        const committed = commitChartDimensionSelection(selectedMode, selectedItems, availableItems);
        if (!committed) {
            return {
                type: 'revert',
                searchSelection: selectedMode === 'region' ? selectedRegions : selectedIcbs,
            };
        }
        return {
            type: selectedMode,
            selected: committed.selected,
            selection: committed.selection,
            searchSelection: committed.selected,
            availableItems,
        };
    }

    return null;
}

export function applyResultsModeSearchSync(sync, {
    analyseOptions,
    resultsModeSearchStore,
    assignLocals,
} = {}) {
    if (!sync) return;
    assignLocals?.(sync);
    if (sync.selectedOrganisations !== undefined) {
        analyseOptions.setSelectedOrganisations(sync.selectedOrganisations);
    }
    if (sync.chartRegionsStoreValue !== undefined) {
        analyseOptions.setSelectedChartRegions(sync.chartRegionsStoreValue);
    }
    if (sync.chartIcbsStoreValue !== undefined) {
        analyseOptions.setSelectedChartIcbs(sync.chartIcbsStoreValue);
    }
    resultsModeSearchStore.setItems(sync.availableItems, sync.filterType);
    resultsModeSearchStore.updateSelection(sync.selectedItems);
}
