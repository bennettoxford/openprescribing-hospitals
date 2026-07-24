<svelte:options runes={false} customElement={{ tag: 'analysis-results', shadow: 'none' }} />

<script>
    import { onDestroy, onMount } from 'svelte';
    import TotalsTable from './TotalsTable.svelte';
    import ProductsTable from './ProductsTable.svelte';
    import ResultsChartControls from './ResultsChartControls.svelte';
    import { resultsStore } from '../../../stores/resultsStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import Chart from '../../common/Chart.svelte';
    import { modeSelectorStore, reconcileSelectedMode, selectDefaultMode } from '../../../stores/modeSelectorStore';
    import { createChartStore } from '../../../stores/chartStore';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { createResultsModeSearchStore } from '../../../stores/resultsModeSearchStore';
    import { getCurrentUrl, copyToClipboard, getUrlParams } from '../../../utils/utils';
    import { ViewModeCalculator, processTableDataByMode } from '../lib/analyseData.js';
    import {
        ANALYSIS_SCOPE,
        arePercentilesDisabled,
        computePercentileScopeConstraints,
        getChartExplainerText,
        getPercentileChartIntroText,
        formatScopeFilterDescription,
        formatInScopePopulationPhrase,
        normaliseMode,
        isAggregationChartMode,
    } from '../lib/analysisScope.js';
    import {
        allOverlaySelection,
        resolveChartOverlayLocals,
        commitChartDimensionSelection as commitChartDimensionSelectionHelper,
        buildOrganisationSelectionPatch,
        applyResultsModeSearchSync,
        buildResultsModeSearchSync,
        clampTrustOverlaySelection,
    } from '../lib/chartOverlay.js';
    import {
        buildResultsChartPipeline,
        buildResultsTooltipContent,
        buildChartExportOrganisations,
        buildVmpsFromSelectedData,
    } from '../lib/resultsChartPipeline.js';

    export let className = '';
    export let urlValidationErrors = [];
    export let isAuthenticated = false;

    $: isAuth = isAuthenticated === true || isAuthenticated === 'true';

    let selectedData = [];
    let vmps = [];
    let filteredData = [];
    let viewModes = [];
    let isCopyingShareLink = false;
    let showShareToast = false;
    let shareToastMessage = '';
    let shareToastVariant = 'success';
    let shareToastTimeout;
    let excludedVmps = [];
    let showTrustCountDetails = false;
    let modeFromUrl = null;
    let isModeFromUrlApplied = false;
    let previousSelectedMode = null;
    let availableTrusts = [];
    let selectedRegions = [];
    let selectedIcbs = [];
    let regionOverlaySelection = allOverlaySelection();
    let icbOverlaySelection = allOverlaySelection();

    if (typeof window !== 'undefined') {
        modeFromUrl = normaliseMode(getUrlParams().get('mode'));
    }

    const resultsChartStore = createChartStore({
        mode: 'trust',
        yAxisLabel: 'units',
        yAxisRange: [0, 100],
        visibleItems: new Set(),
        yAxisBehavior: { forceZero: true, padTop: 1.1, resetToInitial: true }
    });
    const resultsModeSearchStore = createResultsModeSearchStore();

    onMount(() => {
        resultsChartStore.setDimensions({
            height: 600,
            margin: { top: 10, right: 20, bottom: 30, left: 50 }
        });
    });

    onDestroy(() => {
        if (shareToastTimeout) clearTimeout(shareToastTimeout);
    });

    $: excludedVmps = Array.isArray($resultsStore.excludedVmps) ? $resultsStore.excludedVmps : [];
    $: analysisScope = $resultsStore.scope || ANALYSIS_SCOPE.ALL;
    $: availableTrusts = Array.isArray($resultsStore.inScopeTrusts) ? $resultsStore.inScopeTrusts : [];
    $: isFilteredScope = analysisScope === ANALYSIS_SCOPE.GROUP;
    $: isTrustScope = analysisScope === ANALYSIS_SCOPE.TRUST;
    $: scopeFilterDescription = isFilteredScope
        ? formatScopeFilterDescription($resultsStore.scopeFilters || {})
        : '';
    $: inScopeTrustCount = availableTrusts.length;
    $: inScopeTrustLabel = isFilteredScope ? 'in-scope trusts' : 'trusts';
    $: percentilePopulationLabel = isFilteredScope
        ? formatInScopePopulationPhrase({
            trustCount: inScopeTrustCount,
            filterDescription: scopeFilterDescription,
            includeArticle: false
        })
        : 'all NHS Trusts';
    $: isOverlaySelectionScope = [ANALYSIS_SCOPE.ALL, ANALYSIS_SCOPE.GROUP].includes(analysisScope);
    $: hasRegionMode = viewModes.some(mode => mode.value === 'region');
    $: hasIcbMode = viewModes.some(mode => mode.value === 'icb');
    $: shouldShowOrganisationSearch = isAuth && (
        ($modeSelectorStore.selectedMode === 'trust' && isOverlaySelectionScope) ||
        ($modeSelectorStore.selectedMode === 'region' && hasRegionMode) ||
        ($modeSelectorStore.selectedMode === 'icb' && hasIcbMode)
    );
    $: percentilesDisabled = arePercentilesDisabled(analysisScope, availableTrusts.length);
    $: trustPercentileToggleDisabled = percentilesDisabled || !$analyseOptions.selectedOrganisations?.length;

    function assignOverlayLocals(next) {
        selectedRegions = next.selectedRegions;
        selectedIcbs = next.selectedIcbs;
        regionOverlaySelection = next.regionOverlaySelection;
        icbOverlaySelection = next.icbOverlaySelection;
    }

    function syncResultsUi({ enforceConstraints = false } = {}) {
        let selectedOrganisations = $analyseOptions.selectedOrganisations || [];
        if (enforceConstraints) {
            const patches = computePercentileScopeConstraints({
                scope: $resultsStore.scope || ANALYSIS_SCOPE.ALL,
                inScopeTrusts: $resultsStore.inScopeTrusts,
                selectedOrganisations,
                showPercentiles: $resultsStore.showPercentiles,
                mode: $modeSelectorStore.selectedMode,
            });
            if (patches.resultsStore) {
                resultsStore.update(store => ({ ...store, ...patches.resultsStore }));
            }
            if (patches.selectedOrganisations) {
                analyseOptions.setSelectedOrganisations(patches.selectedOrganisations);
                selectedOrganisations = patches.selectedOrganisations;
            }
        }

        const sync = buildResultsModeSearchSync({
            selectedMode: $modeSelectorStore.selectedMode,
            availableTrusts,
            selectedOrganisations,
            selectedRegions,
            selectedIcbs,
            regionOverlaySelection,
            icbOverlaySelection,
            selectedData,
        });
        applyResultsModeSearchSync(sync, {
            analyseOptions,
            resultsModeSearchStore,
            assignLocals: assignOverlayLocals,
        });
    }

    function rebuildChart({ forceUpdate = false, data = null } = {}) {
        const chartSource = data
            ?? ($resultsStore.filteredData?.length > 0 ? $resultsStore.filteredData : selectedData);
        if (!$modeSelectorStore.selectedMode || !chartSource?.length) return;

        const scope = $resultsStore.scope || ANALYSIS_SCOPE.ALL;
        const selectedOrganisations = $analyseOptions.selectedOrganisations || [];
        const showPercentiles = $resultsStore.showPercentiles;
        const overridesPercentilesDisabled = arePercentilesDisabled(scope, availableTrusts.length);
        const overrides = {
            percentilesDisabled: overridesPercentilesDisabled,
            showPercentiles: overridesPercentilesDisabled ? false : showPercentiles,
            selectedOrganisations: clampTrustOverlaySelection(selectedOrganisations),
            selectedRegions,
            selectedIcbs,
        };
        const chartData = processChartData(chartSource, overrides);
        if (forceUpdate) {
            resultsChartStore.setData({ ...chartData, forceUpdate: Date.now() });
        }
    }

    function handleModeChange(nextMode) {
        if (previousSelectedMode !== null && previousSelectedMode !== nextMode) {
            analyseOptions.applyOverlayModeChange(previousSelectedMode, nextMode);
        }
        previousSelectedMode = nextMode;
        syncResultsUi({ enforceConstraints: true });
        rebuildChart();
    }

    function applySelectedMode(nextMode) {
        const fromMode = $modeSelectorStore.selectedMode;
        if (fromMode !== nextMode) {
            if (fromMode !== null) {
                analyseOptions.applyOverlayModeChange(fromMode, nextMode);
            }
            modeSelectorStore.setSelectedMode(nextMode);
        }
        previousSelectedMode = nextMode;
        syncResultsUi({ enforceConstraints: true });
        rebuildChart();
    }

    function hasValidData(item) {
        return Array.isArray(item?.data) && item.data.some(v => v > 0 && !isNaN(parseFloat(v)));
    }

    $: currentModeHasData = (() => {
        if (!selectedData?.length) return false;
        if ($modeSelectorStore.selectedMode === 'trust') {
            const selectedOrgNames = new Set($analyseOptions.selectedOrganisations || []);
            if (selectedOrgNames.size === 0) return selectedData.some(hasValidData);
            return selectedData
                .filter(item => selectedOrgNames.has(item.organisation__ods_name))
                .some(hasValidData);
        }
        const tableData = processTableDataByMode(
            selectedData,
            $modeSelectorStore.selectedMode,
            'all',
            $resultsStore.aggregatedData,
            null,
            $analyseOptions.selectedOrganisations || [],
            $organisationSearchStore.items || [],
            $resultsStore.analysisMonths || [],
            $organisationSearchStore.regionsHierarchy || [],
            $resultsStore.scope || 'all'
        );
        return tableData?.length > 0 && tableData.some(entry => entry.total > 0);
    })();

    $: isInTrustModeWithNoData = $modeSelectorStore.selectedMode === 'trust'
        && $analyseOptions.selectedOrganisations?.length > 0
        && !currentModeHasData
        && !$resultsStore.showPercentiles;
    $: canShowPercentilesWithoutTrustData = $modeSelectorStore.selectedMode === 'trust'
        && $analyseOptions.selectedOrganisations?.length > 0
        && !currentModeHasData
        && $resultsStore.showPercentiles;

    function showShareFeedback(message, variant = 'success') {
        shareToastMessage = message;
        shareToastVariant = variant;
        showShareToast = true;
        if (shareToastTimeout) clearTimeout(shareToastTimeout);
        shareToastTimeout = setTimeout(() => {
            showShareToast = false;
            shareToastTimeout = null;
        }, 2500);
    }

    async function handleCopyAnalysisLink() {
        if (isCopyingShareLink) return;
        isCopyingShareLink = true;
        try {
            await copyToClipboard(getCurrentUrl());
            showShareFeedback('Analysis link copied to clipboard!', 'success');
        } catch (error) {
            console.error('Failed to copy analysis link:', error);
            showShareFeedback('Could not copy link. Please try again.', 'error');
        } finally {
            isCopyingShareLink = false;
        }
    }

    function processChartData(data, overrides = {}) {
        const { chartData, chartConfig, percentilesPatch } = buildResultsChartPipeline({
            data,
            mode: $modeSelectorStore.selectedMode,
            aggregatedData: $resultsStore.aggregatedData,
            months: $resultsStore.analysisMonths || [],
            selectedOrganisations: overrides.selectedOrganisations
                ?? (Array.isArray($analyseOptions.selectedOrganisations)
                    ? $analyseOptions.selectedOrganisations
                    : []),
            availableTrusts,
            showPercentiles: overrides.showPercentiles ?? $resultsStore.showPercentiles,
            percentilesDisabled: overrides.percentilesDisabled ?? percentilesDisabled,
            scope: $resultsStore.scope || 'all',
            selectedRegions: overrides.selectedRegions ?? selectedRegions,
            selectedIcbs: overrides.selectedIcbs ?? selectedIcbs,
        });
        resultsStore.update(store => ({ ...store, ...percentilesPatch }));
        resultsChartStore.setData(chartData);
        resultsChartStore.setConfig(chartConfig);
        return chartData;
    }

    function applyChartOverlaySelectionFromStore() {
        ({
            selectedRegions,
            regionOverlaySelection,
            selectedIcbs,
            icbOverlaySelection,
        } = resolveChartOverlayLocals($analyseOptions, selectedData));
    }

    function commitChartDimensionSelection(dimension, selectedItems, availableItems = []) {
        const committed = commitChartDimensionSelectionHelper(dimension, selectedItems, availableItems);
        if (!committed) return false;
        if (dimension === 'region') {
            selectedRegions = committed.selected;
            regionOverlaySelection = committed.selection;
            analyseOptions.setSelectedChartRegions(committed.selection);
            return true;
        }
        selectedIcbs = committed.selected;
        icbOverlaySelection = committed.selection;
        analyseOptions.setSelectedChartIcbs(committed.selection);
        return true;
    }

    function recalculateViewModes(vmpsWithValidData) {
        if (vmpsWithValidData.length === 0) {
            viewModes = [];
            modeSelectorStore.setSelectedMode(null);
            return [];
        }
        const calculator = new ViewModeCalculator(
            $resultsStore,
            $analyseOptions,
            $organisationSearchStore,
            vmpsWithValidData,
            $resultsStore.scope || 'all',
            isAuth
        );
        viewModes = calculator.calculateAvailableModes();
        return viewModes;
    }

    function handleUpdateData(data) {
        selectedData = Array.isArray(data.data?.items) ? data.data.items : [];
        applyChartOverlaySelectionFromStore();

        try {
            vmps = buildVmpsFromSelectedData(
                selectedData,
                data.searchType || $analyseOptions.searchType
            );

            const availableCodes = new Set(vmps.map(vmp => String(vmp.code ?? '')));
            const currentExcluded = Array.isArray($resultsStore.excludedVmps) ? $resultsStore.excludedVmps : [];

            resultsStore.update(store => ({
                ...store,
                analysisData: selectedData,
                showResults: true,
                searchType: data.searchType || $analyseOptions.searchType,
                quantityType: data.quantityType || $analyseOptions.quantityType,
                excludedVmps: currentExcluded.filter(code => availableCodes.has(String(code)))
            }));

            if (!isModeFromUrlApplied && isAggregationChartMode(modeFromUrl)) {
                analyseOptions.applyOverlayModeChange($modeSelectorStore.selectedMode, modeFromUrl);
            }

            const nextViewModes = recalculateViewModes(vmps.filter(vmp => vmp.unit !== 'nan'));

            if (nextViewModes.length > 0) {
                const availableModeValues = nextViewModes.map(mode => mode.value);
                let nextMode = null;

                if (!isModeFromUrlApplied) {
                    if (modeFromUrl && availableModeValues.includes(modeFromUrl)) {
                        nextMode = modeFromUrl;
                    }
                    isModeFromUrlApplied = true;
                    modeFromUrl = null;
                }

                if (!nextMode) {
                    const currentMode = $modeSelectorStore.selectedMode;
                    nextMode = currentMode && availableModeValues.includes(currentMode)
                        ? currentMode
                        : selectDefaultMode(nextViewModes, $resultsStore.scope || 'all');
                }

                applySelectedMode(nextMode);
            } else {
                syncResultsUi({ enforceConstraints: true });
                rebuildChart();
            }
        } catch (error) {
            console.error('Error processing data:', error);
        }
    }

    function handleFilteredData(event) {
        const selectedVMPs = event.detail;
        filteredData = selectedData.filter(item =>
            selectedVMPs.some(vmp => vmp.vmp === item.vmp__name)
        );

        const filteredVMPs = vmps.filter(vmp =>
            selectedVMPs.some(selectedVmp => selectedVmp.vmp === vmp.vmp)
        );
        const nextViewModes = recalculateViewModes(filteredVMPs.filter(vmp => vmp.unit !== 'nan'));

        resultsStore.update(store => ({ ...store, filteredData }));

        if (nextViewModes.length > 0) {
            const reconciledMode = reconcileSelectedMode(
                $modeSelectorStore.selectedMode,
                nextViewModes
            );
            if (reconciledMode === null) {
                applySelectedMode(selectDefaultMode(nextViewModes, $resultsStore.scope || 'all'));
            } else if (reconciledMode !== $modeSelectorStore.selectedMode) {
                applySelectedMode(reconciledMode);
            } else {
                syncResultsUi();
                rebuildChart({ forceUpdate: true, data: filteredData });
            }
        } else {
            syncResultsUi();
            rebuildChart({ forceUpdate: true, data: filteredData });
        }
    }

    export function loadAnalysis(detail) {
        handleUpdateData(detail);
    }

    export function prepareForNewRun() {
        selectedData = [];
        filteredData = [];
        vmps = [];
        viewModes = [];
        previousSelectedMode = null;
    }

    export function syncOverlayFromBuilder() {
        if (!selectedData.length || !$modeSelectorStore.selectedMode) return;
        const nextViewModes = recalculateViewModes(vmps.filter(vmp => vmp.unit !== 'nan'));
        if (nextViewModes.length > 0) {
            const reconciledMode = reconcileSelectedMode(
                $modeSelectorStore.selectedMode,
                nextViewModes
            );
            if (reconciledMode === null) {
                applySelectedMode(selectDefaultMode(nextViewModes, $resultsStore.scope || 'all'));
                return;
            }
            if (reconciledMode !== $modeSelectorStore.selectedMode) {
                applySelectedMode(reconciledMode);
                return;
            }
        }
        syncResultsUi();
        rebuildChart({ forceUpdate: true });
    }

    function customTooltipFormatter(d) {
        return buildResultsTooltipContent(d, {
            vmps,
            yAxisLabel: $resultsChartStore.config?.yAxisLabel,
        });
    }

    function handlePercentileToggle() {
        if (percentilesDisabled || $modeSelectorStore.selectedMode !== 'trust'
            || !($analyseOptions.selectedOrganisations?.length > 0)) {
            return;
        }
        resultsStore.update(store => ({ ...store, showPercentiles: !store.showPercentiles }));
        rebuildChart();
    }

    function handleResultsOrganisationSelection(event) {
        const selectedItems = event?.detail?.selectedItems || [];
        const patch = buildOrganisationSelectionPatch({
            selectedMode: $modeSelectorStore.selectedMode,
            selectedItems,
            selectedData,
            selectedRegions,
            selectedIcbs,
        });
        if (!patch) return;

        if (patch.type === 'trust') {
            analyseOptions.setSelectedOrganisations(patch.selectedOrganisations);
            analyseOptions.setRememberedOverlayOrganisations(patch.selectedOrganisations);
            resultsModeSearchStore.updateSelection(patch.searchSelection);
            syncResultsUi();
            rebuildChart();
            return;
        }
        if (patch.type === 'revert') {
            resultsModeSearchStore.updateSelection(patch.searchSelection);
            return;
        }
        if (
            (patch.type === 'region' || patch.type === 'icb')
            && commitChartDimensionSelection(patch.type, selectedItems, patch.availableItems)
        ) {
            resultsModeSearchStore.updateSelection(
                patch.type === 'region' ? selectedRegions : selectedIcbs
            );
            syncResultsUi();
            rebuildChart();
        } else if (patch.type === 'region' || patch.type === 'icb') {
            resultsModeSearchStore.updateSelection(patch.searchSelection);
        }
    }

    $: selectedOrganisationsCount = $analyseOptions.selectedOrganisations?.length || 0;
    $: singleSelectedTrust = selectedOrganisationsCount === 1;
    $: chartExplainerText = getChartExplainerText($modeSelectorStore.selectedMode, {
        hasSelectedOrganisations: selectedOrganisationsCount > 0,
        currentModeHasData,
        vmpsCount: vmps.length,
        selectedOrganisationsCount,
        scope: $resultsStore.scope || 'all',
        inScopeTrustCount,
        scopeFilterDescription
    });
    $: percentileIntroText = getPercentileChartIntroText({
        selectedOrganisationsCount,
        currentModeHasData,
        singleSelectedTrust,
        isFilteredScope,
        percentilePopulationLabel,
    });
</script>

{#if $resultsStore.showResults || (urlValidationErrors && urlValidationErrors.length > 0)}
    <div class="results-box bg-white rounded-lg shadow-md h-full flex flex-col {className}">
        <div class="flex-grow overflow-y-auto rounded-t-lg">
            {#if urlValidationErrors && urlValidationErrors.length > 0}
                <div class="p-4 border-b border-gray-200 bg-yellow-50">
                    <div class="flex items-start">
                        <div class="flex-shrink-0">
                            <svg class="h-5 w-5 text-yellow-400" viewBox="0 0 20 20" fill="currentColor">
                                <path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
                            </svg>
                        </div>
                        <div class="ml-3">
                            <h3 class="text-sm font-medium text-yellow-800">URL Parameter Issues</h3>
                            <div class="mt-2 text-sm text-yellow-700">
                                <ul class="list-disc pl-5 space-y-1">
                                    {#each urlValidationErrors as error}
                                        <li>{error}</li>
                                    {/each}
                                </ul>
                                {#if urlValidationErrors.some(e => e.includes('unique products'))}
                                    <p class="mt-2">
                                        <a href="/contact/" target="_blank" class="underline font-medium hover:text-yellow-900">Contact us</a> if you need to analyse a larger selection.
                                    </p>
                                {/if}
                            </div>
                        </div>
                    </div>
                </div>
            {/if}
            {#if $resultsStore.isAnalysisRunning}
                <div class="flex items-center justify-center h-[500px] p-16">
                    <div class="flex items-center">
                        <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-oxford-600"></div>
                        <span class="ml-3 text-sm text-oxford-600">Running analysis...</span>
                    </div>
                </div>
            {:else if selectedData.length > 0}
                <div class="space-y-6 p-6">
                    <section class="bg-white rounded-lg p-4 border-2 border-oxford-300 shadow-sm">
                        <ProductsTable {vmps} {excludedVmps} on:dataFiltered={handleFilteredData} />
                    </section>
                    
                    {#if viewModes.length > 0}
                    <section class="p-4">
                        <ResultsChartControls
                            {resultsModeSearchStore}
                            {shouldShowOrganisationSearch}
                            {availableTrusts}
                            {viewModes}
                            {isTrustScope}
                            {percentilesDisabled}
                            {trustPercentileToggleDisabled}
                            {isCopyingShareLink}
                            on:selectionChange={handleResultsOrganisationSelection}
                            on:copyLink={handleCopyAnalysisLink}
                            on:percentileToggle={handlePercentileToggle}
                            on:modeChange={(e) => handleModeChange(e.detail.mode)}
                        />

                        {#if currentModeHasData || canShowPercentilesWithoutTrustData}
                        <div class="mb-4">
                            <p class="text-sm text-gray-700">
                                {#if $modeSelectorStore.selectedMode === 'trust' && $resultsStore.showPercentiles}
                                    {percentileIntroText}
                                    {#if $resultsStore.trustCount > 0}
                                        {#if isFilteredScope}
                                            Only in-scope trusts that have issued any of the selected products during the time period are included. For the selected products above, this is <strong>{$resultsStore.trustCount}/{availableTrusts.length} {inScopeTrustLabel}</strong>
                                        {:else}
                                            Trusts are only included if they have issued any of the selected products during the time period. For the selected products above, this is <strong>{$resultsStore.trustCount}/{availableTrusts.length} trusts</strong>
                                        {/if}
                                        {#if $resultsStore.excludedTrusts && $resultsStore.excludedTrusts.length > 0}
                                            <button
                                                type="button"
                                                class="text-blue-600 hover:text-blue-800 underline text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-1 rounded"
                                                on:click={() => showTrustCountDetails = !showTrustCountDetails}
                                            >
                                                ({showTrustCountDetails ? 'hide' : 'show'} excluded trusts)
                                            </button>
                                        {/if}
                                        .
                                    {/if}
                                    Variation can reflect differences in hospital size rather than genuine variation.
                                    See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret this chart.
                                {:else}
                                    {chartExplainerText}
                                    See <a href="/faq/" class="underline font-semibold" target="_blank">the FAQs</a> for more details about interpreting charts.
                                {/if}
                            </p>
                            
                            {#if $modeSelectorStore.selectedMode === 'trust' && $resultsStore.showPercentiles && showTrustCountDetails && $resultsStore.excludedTrusts && $resultsStore.excludedTrusts.length > 0}
                            <div class="mt-3 p-3 bg-gray-50 border border-gray-200 rounded">
                                <div class="text-sm text-gray-700 font-medium mb-2">
                                    Excluded trusts ({$resultsStore.excludedTrusts.length}):
                                </div>
                                <div class="max-h-32 overflow-y-auto bg-white border border-gray-200 rounded p-3">
                                    <div class="space-y-1">
                                        {#each $resultsStore.excludedTrusts as trust}
                                        <div class="text-xs text-gray-600">
                                            {trust.name}
                                        </div>
                                        {/each}
                                    </div>
                                </div>
                            </div>
                            {/if}

                        </div>
                        {/if}

                        {#if showShareToast}
                            <div class="fixed bottom-4 right-4 z-50" aria-live="polite">
                                <div class={`px-4 py-2 rounded-lg shadow-lg border transform translate-y-0 opacity-100 transition-all duration-300 ${shareToastVariant === 'success' ? 'bg-oxford-50 text-oxford-800 border-oxford-100' : 'bg-red-50 text-red-800 border-red-200'}`}>
                                    <div class="flex items-center gap-2">
                                        {#if shareToastVariant === 'success'}
                                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                                                <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                                            </svg>
                                        {:else}
                                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                                                <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m0 3.75h.007v.008H12v-.008z" />
                                                <path stroke-linecap="round" stroke-linejoin="round" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                                            </svg>
                                        {/if}
                                        <span class="text-sm font-medium">{shareToastMessage}</span>
                                    </div>
                                </div>
                            </div>
                        {/if}
                    </section>
                    <section>
                    {#if isInTrustModeWithNoData}
                            <div class="flex items-center justify-center h-[550px] border-2 border-dashed border-gray-300 rounded-lg p-8 bg-gray-50">
                                <div class="">
                                    <div class="text-center space-y-6">
                                        <div>
                                            <p class="text-oxford-600 text-xl font-medium mb-3">
                                                {singleSelectedTrust ? 'No data for selected trust' : 'No data for selected trusts'}
                                            </p>
                                            <p class="text-gray-600 text-base max-w-md mb-4">
                                                {singleSelectedTrust
                                                    ? 'The selected NHS Trust has no data for the chosen products.'
                                                    : 'The selected NHS Trusts have no data for the chosen products.'}
                                            </p>
                                            <p class="text-gray-600 text-base max-w-md">
                                                {#if percentilesDisabled}
                                                    Try <strong>selecting different trusts</strong> above, or choose different products.
                                                {:else}
                                                    Try <strong>turning on percentiles</strong>, to see variation across {isFilteredScope ? 'in-scope trusts' : 'all trusts'} that do have data, or <strong>select more trusts</strong> {isAuth ? 'above' : 'in the analysis builder'}.
                                                {/if}
                                                <a href="/faq/#why-is-there-no-quantity-for-some-products" class="link-oxford" target="_blank">
                                                    Learn more about why quantities might be missing
                                                </a>.
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        {:else if currentModeHasData || canShowPercentilesWithoutTrustData}
                            <div class="grid grid-cols-1 gap-4">
                                <div class="relative h-[650px] mb-8">
                                    <Chart 
                                        store={resultsChartStore} 
                                        data={filteredData.length > 0 ? filteredData : selectedData}
                                        mode={$modeSelectorStore.selectedMode || 'trust'}
                                        formatTooltipContent={customTooltipFormatter}
                                        exportData={{
                                            data: $resultsStore.filteredData && $resultsStore.filteredData.length > 0
                                                ? $resultsStore.filteredData
                                                : $resultsStore.analysisData,
                                            months: $resultsStore.analysisMonths || [],
                                            excludedVmps: $resultsStore.excludedVmps || [],
                                            selectedTrusts: $analyseOptions.selectedOrganisations || null,
                                            percentilesData: (
                                                $modeSelectorStore.selectedMode === 'trust'
                                                && $resultsStore.showPercentiles
                                                && !percentilesDisabled
                                            ) ? ($resultsStore.percentiles || []) : [],
                                            allOrganisations: buildChartExportOrganisations(
                                                $analyseOptions.selectedOrganisations,
                                                availableTrusts,
                                                {
                                                    orgCodes: $organisationSearchStore.orgCodes,
                                                    orgRegions: $organisationSearchStore.orgRegions,
                                                    orgIcbs: $organisationSearchStore.orgIcbs,
                                                    trustTypes: $organisationSearchStore.trustTypes,
                                                }
                                            ),
                                        }}
                                    />
                                </div>
                            </div>
                        {:else}
                            <div class="flex items-center justify-center h-[550px] p-6">
                                <div class="text-center space-y-6">
                                    <div>
                                        <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                                        <p class="text-oxford-400 text-base max-w-md">
                                            No data was returned for the selected view mode.
                                            <a href="/faq/#why-is-there-no-quantity-for-some-products" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                                                Learn more about why quantities might be missing
                                            </a>.
                                        </p>
                                    </div>
                                </div>
                            </div>
                        {/if}
                    </section>

                    <section class="bg-amber-50 border-l-4 border-amber-400 p-4 mx-4 mb-12 mt-16 relative z-10">
                      <div class="flex flex-col sm:flex-row">
                        <div class="flex-shrink-0 mb-2 sm:mb-0">
                          <svg class="h-5 w-5 text-amber-400" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v4.5a.75.75 0 01-1.5 0v-4.5A.75.75 0 0110 5zm0 10a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" />
                          </svg>
                        </div>
                        <div class="sm:ml-3">
                          <p class="text-sm text-amber-700">
                            Individual NHS Trust data submissions may be incomplete or inconsistent. 
                            <a href="/submission-history/" class="font-medium underline hover:text-amber-800">
                              View Submission History
                            </a> to understand data quality issues for individual NHS Trusts.
                          </p>
                        </div>
                      </div>
                    </section>

                    <section class="p-4">
                        <TotalsTable 
                            data={filteredData} 
                            quantityType={$analyseOptions.quantityType} 
                            searchType={$analyseOptions.searchType} 
                        />
                    </section>
                    {:else}
                        <div class="flex items-center justify-center h-[500px] p-6">
                            <div class="text-center space-y-6">
                                <div>
                                    <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                                    <p class="text-oxford-400 text-base max-w-md">
                                        No data was returned for the selected quantity type of the chosen products.
                                        <a href="/faq/#why-is-there-no-quantity-for-some-products" class="text-blue-600 hover:text-blue-800 hover:underline" target="_blank">
                                            Learn more about why quantities might be missing
                                        </a>.
                                    </p>
                                </div>
                            </div>
                        </div>
                    {/if}
                </div>
            {:else}
                <div class="flex items-center justify-center h-[500px] p-6">
                    <div class="text-center space-y-6">
                        <div>
                            <p class="text-oxford-600 text-xl font-medium mb-3">No data to display</p>
                            <p class="text-oxford-400 text-base max-w-md">
                                The analysis returned no chartable data.
                            </p>
                        </div>
                    </div>
                </div>
            {/if}
        </div>
    </div>
{:else}
    <div class="{className}">
        <div class="flex items-center justify-center h-[500px] p-6">
            <div class="text-center space-y-6">
                <div>
                    <p class="text-oxford-600 text-xl font-medium mb-3">No analysis results to show</p>
                    <p class="text-oxford-400 text-base max-w-md">
                        Please run an analysis to see results here.
                    </p>
                </div>
            </div>
        </div>
    </div>
{/if}
