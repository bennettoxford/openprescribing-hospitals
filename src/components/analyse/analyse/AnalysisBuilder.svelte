<svelte:options runes={false} customElement={{
    tag: 'analysis-builder',
    shadow: 'none'
  }} />

<script>
    import { onMount, tick } from 'svelte';
    import '../../../styles/styles.css';
    import ProductSearch from '../../common/ProductSearch.svelte';
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import AnalysisScopePanel from './AnalysisScopePanel.svelte';
    import { createEventDispatcher } from 'svelte';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { resultsStore, updateResults } from '../../../stores/resultsStore';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import {
        ANALYSIS_SCOPE,
        normaliseMode,
        isAggregationChartMode,
    } from '../lib/analysisScope.js';
    import {
        createEmptyScopeFilters,
        decodeTrustTypeFromUrl,
        hasAnyScopeFilters,
        normaliseScopeFilters,
    } from '../../../utils/scopeFilters.js';
    import {
        ANALYSIS_QUANTITY_PARAM,
        SUPPORTED_ANALYSIS_PARAMS,
        getShowPercentilesParam,
        getCancerAllianceCodeMaps,
        getRegionCodeMaps,
        updateAnalysisUrl as writeAnalysisUrlParams,
        planAnalysisHydrate,
        planValidationStorePatch,
        finishValidatedHydrate,
    } from '../lib/analyseUrlParams.js';
    import {
        validateAnalysisRun,
        buildAnalysisRunPlan,
        executeAnalysisFetch,
        completeAnalysisRun,
    } from '../lib/runAnalysis.js';
    import {
        getCookie,
        getUrlParams,
        setUrlParams,
        cleanupUrl,
    } from '../../../utils/utils';
    import { allOverlaySelection } from '../lib/chartOverlay.js';
    
    const dispatch = createEventDispatcher();

    const availableQuantityTypes = [
        'SCMD Quantity',
        'Unit Dose Quantity', 
        'Ingredient Quantity',
        'Defined Daily Dose Quantity'
    ];

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isSelectingQuantityTypes = false;
    let showAdvancedOptions = false;
    let recommendedQuantityTypes = [];
    let selectedQuantityType = null;
    let isQuantityDropdownOpen = false;
    let selectedScope = ANALYSIS_SCOPE.ALL;
    let selectedScopeFilters = createEmptyScopeFilters();

    let urlState = {
        mode: null,
        showPercentiles: null,
        excludedVmps: [],
        isHydrated: false,
        validationErrors: [],
        suppressSync: true
    };

    $: selectedVMPs = $analyseOptions.selectedVMPs;
    $: searchType = $analyseOptions.searchType;
    $: selectedQuantityType = $analyseOptions.quantityType;
    $: selectedTrusts = $organisationSearchStore.selectedItems || [];
    $: resultsOverlayTrusts = $analyseOptions.selectedOrganisations || [];
    $: selectedMode = $modeSelectorStore.selectedMode;
    $: showPercentiles = $resultsStore.showPercentiles;
    $: effectiveMode = normaliseMode(selectedMode) || urlState.mode;
    $: trustsForUrl = selectedScope === ANALYSIS_SCOPE.TRUST
        ? selectedTrusts
        : (isAggregationChartMode(effectiveMode)
            ? []
            : (isAuth ? resultsOverlayTrusts : selectedTrusts));

    $: urlState.excludedVmps = Array.isArray($resultsStore.excludedVmps)
        ? Array.from(new Set($resultsStore.excludedVmps.filter(Boolean).map(String))).sort()
        : [];

    $: if (selectedMode && urlState.mode) {
        urlState.mode = null;
    }

    $: chartRegionsForUrl = effectiveMode === 'region'
        ? $analyseOptions.selectedChartRegions
        : null;
    $: chartIcbsForUrl = effectiveMode === 'icb'
        ? $analyseOptions.selectedChartIcbs
        : null;

    $: if (!urlState.suppressSync && urlState.validationErrors.length === 0) {
        const { regionCodeByName, icbCodeByName } = getRegionCodeMaps(
            $organisationSearchStore.regionsHierarchy || []
        );
        const { codeByName: cancerAllianceCodeByName } = getCancerAllianceCodeMaps(
            typeof organisationSearchStore.getCancerAlliances === 'function'
                ? organisationSearchStore.getCancerAlliances()
                : []
        );
        const currentShowPercentiles = getShowPercentilesParam(
            effectiveMode,
            trustsForUrl,
            showPercentiles,
            selectedScope
        );
        writeAnalysisUrlParams({
            products: selectedVMPs,
            trusts: trustsForUrl,
            quantityType: selectedQuantityType,
            scope: selectedScope,
            scopeFilters: selectedScopeFilters,
            mode: effectiveMode,
            showPercentiles: currentShowPercentiles,
            excludedVmps: urlState.excludedVmps,
            chartRegions: chartRegionsForUrl,
            chartIcbs: chartIcbsForUrl,
            getOrgCode: (name) => organisationSearchStore.getOrgCode(name),
            regionCodeByName,
            icbCodeByName,
            cancerAllianceCodeByName,
        });
    }

    export let orgData = null;
    export let isAuthenticated = false;
    export let maxVmpCount = null;

    $: isAuth = isAuthenticated === true || isAuthenticated === 'true';
    $: if (!isAuth && selectedScope !== ANALYSIS_SCOPE.ALL) {
        selectedScope = ANALYSIS_SCOPE.ALL;
        selectedScopeFilters = createEmptyScopeFilters();
    }

    let resolvedVmpOverLimit = false;

    function handleVmpCountChange(event) {
        resolvedVmpOverLimit = event.detail.overLimit;
    }

    function handleValidationResponse(data) {
        if (data.errors?.length > 0) {
            urlState.validationErrors = data.errors;
            dispatch('urlValidationErrors', { errors: data.errors });
            return false;
        }

        urlState.validationErrors = [];
        dispatch('urlValidationErrors', { errors: [] });
        return true;
    }

    async function hydrateFromUrl() {
        if (urlState.isHydrated || typeof window === 'undefined') {
            urlState.suppressSync = false;
            return;
        }

        urlState.isHydrated = true;

        try {
            const urlParams = getUrlParams();
            const plan = planAnalysisHydrate({
                urlParams,
                regionsHierarchy: $organisationSearchStore.regionsHierarchy || [],
                cancerAlliances:
                    typeof organisationSearchStore.getCancerAlliances === 'function'
                        ? organisationSearchStore.getCancerAlliances()
                        : [],
                decodeTrustTypeFromUrl,
            });

            const hydrateScope = isAuth ? plan.scope : ANALYSIS_SCOPE.ALL;
            selectedScopeFilters = isAuth ? plan.scopeFilters : createEmptyScopeFilters();
            handleScopeChange(hydrateScope);
            if (isAuth && plan.shouldShowAdvancedOptions) {
                showAdvancedOptions = true;
            }
            urlState.mode = plan.mode;
            urlState.showPercentiles = plan.showPercentilesRaw;
            analyseOptions.setSelectedChartRegions(plan.chartRegions);
            analyseOptions.setSelectedChartIcbs(plan.chartIcbs);

            if (!plan.validationQuery.toString()) {
                urlState.suppressSync = false;
                return;
            }

            const response = await fetch(`/api/validate-analysis-params/?${plan.validationQuery}`, {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });

            if (!response.ok) {
                console.error('Failed to validate URL parameters:', response.status);
                return;
            }

            const data = await response.json();
            if (!handleValidationResponse(data)) {
                return;
            }

            const patch = planValidationStorePatch({
                data,
                scope: hydrateScope,
                mode: plan.mode,
                showPercentilesRaw: plan.showPercentilesRaw,
                hasExplicitTrustSelection: plan.hasExplicitTrustSelection,
            });

            const { excludedVmps } = await finishValidatedHydrate({
                patch,
                selectedScope: hydrateScope,
                selectedScopeFilters: isAuth ? plan.scopeFilters : createEmptyScopeFilters(),
                analyseOptions,
                organisationSearchStore,
                resultsStore,
                modeSelectorStore,
                applyQuantityType: async (hydratePatch) => {
                    if (hydratePatch.quantityType) {
                        analyseOptions.setQuantityType(hydratePatch.quantityType.name);
                        selectedQuantityType = hydratePatch.quantityType.name;
                        if (hydratePatch.quantityType.code) {
                            setUrlParams(
                                { [ANALYSIS_QUANTITY_PARAM]: hydratePatch.quantityType.code },
                                [ANALYSIS_QUANTITY_PARAM]
                            );
                        }
                        await selectQuantityType(hydratePatch.selectedVMPs, { preserveSelection: true });
                    } else {
                        await selectQuantityType(hydratePatch.selectedVMPs);
                    }
                },
                cleanupUrlParams: () => {
                    if (typeof window !== 'undefined') {
                        cleanupUrl(SUPPORTED_ANALYSIS_PARAMS);
                    }
                },
                waitForUi: () => tick(),
            });

            if (!isAuth && patch.selectedOrganisations?.length > 0) {
                organisationSearchStore.updateSelection(patch.selectedOrganisations);
            }

            urlState.excludedVmps = excludedVmps;

            const overlayFromUrl = hydrateScope === ANALYSIS_SCOPE.TRUST
                ? (patch.selectedOrganisations || []).slice(0, 1)
                : (patch.selectedOrganisations || []);
            await runAnalysis({ overlayOrganisations: overlayFromUrl });
        } catch (error) {
            console.error('Failed to hydrate analysis selections from URL:', error);
        } finally {
            urlState.suppressSync = false;
        }
    }


    onMount(async () => {
        try {
            if (orgData) {
                try {
                    const parsedData = typeof orgData === 'string' ? JSON.parse(orgData) : orgData;

                    organisationSearchStore.setOrganisationData({
                        orgs: parsedData.orgs || {},
                        org_codes: parsedData.org_codes || {},
                        trust_types: parsedData.trust_types || {},
                        org_regions: parsedData.org_regions || {},
                        org_icbs: parsedData.org_icbs || {},
                        org_cancer_alliances: parsedData.org_cancer_alliances || {},
                        org_shelford_group: parsedData.org_shelford_group || {},
                        regions_hierarchy: parsedData.regions_hierarchy || [],
                        cancer_alliances: parsedData.cancer_alliances || []
                    });
                } catch (error) {
                    console.error('Error parsing ODS data:', error);
                }
            }
        } catch (error) {
            console.error('Error in onMount:', error);
        }

        await hydrateFromUrl();
    });

    const csrftoken = getCookie('csrftoken');
    
    async function selectQuantityType(selectedVMPs, { preserveSelection = false } = {}) {

        if (!selectedVMPs || selectedVMPs.length === 0) {

            analyseOptions.setQuantityType(null);
            recommendedQuantityTypes = [];
            selectedQuantityType = null;
            return;
        }
        
        isSelectingQuantityTypes = true;
        
        try {
            const response = await fetch('/api/select-quantity-type/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrftoken
                },
                body: JSON.stringify({
                    names: selectedVMPs
                })
            });
            
            if (!response.ok) {
                console.error('Failed to select quantity type');
                return;
            }
            
            const data = await response.json();
            
            recommendedQuantityTypes = data.recommended_quantity_types || [];
            if (!preserveSelection) {
                selectedQuantityType = data.selected_quantity_type;
                analyseOptions.setQuantityType(selectedQuantityType);
            }
            
        } catch (error) {
            console.error('Error selecting quantity type:', error);
        } finally {
            isSelectingQuantityTypes = false;
        }
    }

    function resolveProductsToVMPs(selectedProducts) {
        if (!selectedProducts || selectedProducts.length === 0) {
            return [];
        }

        const resolvedProducts = [];

        selectedProducts.forEach(product => {
            if (product.type === 'vmp') {
                resolvedProducts.push(product);
            } else if (product.vmps && product.vmps.length > 0) {
                product.vmps.forEach(vmp => {
                    resolvedProducts.push({
                        code: vmp.code,
                        name: vmp.name,
                        type: 'vmp'
                    });
                });
            }
        });

        return resolvedProducts;
    }

    async function runAnalysis(options = {}) {
        if (isAnalysisRunning) return;

        const runScope = isAuth ? selectedScope : ANALYSIS_SCOPE.ALL;
        const runScopeFilters = isAuth ? selectedScopeFilters : createEmptyScopeFilters();

        errorMessage = '';

        const validationError = validateAnalysisRun({
            selectedVMPs,
            selectedScope: runScope,
            selectedTrusts,
            selectedScopeFilters: runScopeFilters,
            availableItems: $organisationSearchStore.availableItems || [],
        });
        if (validationError) {
            errorMessage = validationError;
            return;
        }

        modeSelectorStore.reset();
        isAnalysisRunning = true;
        dispatch('analysisStart');

        const hasExplicitOverlay = Object.prototype.hasOwnProperty.call(options, 'overlayOrganisations');
        const legacyOverlayOrganisations = !isAuth && !hasExplicitOverlay
            ? ($organisationSearchStore.selectedItems || [])
            : options.overlayOrganisations;

        const plan = buildAnalysisRunPlan({
            selectedScope: runScope,
            selectedItems: $organisationSearchStore.selectedItems || [],
            availableItems: $organisationSearchStore.availableItems || [],
            allItems: $organisationSearchStore.items || [],
            selectedTrusts,
            getOrgCode: (name) => organisationSearchStore.getOrgCode(name),
            overlayOrganisations: legacyOverlayOrganisations,
        });

        if (typeof window !== 'undefined' && window.plausible) {
            window.plausible('Analysis Run', {
                props: {
                    all_products: selectedVMPs.map(p => p.code).join(','),
                    // Names only for single-trust; larger cohorts are counted via organisation_count.
                    all_organisations: runScope === ANALYSIS_SCOPE.TRUST ? plan.cohortTrusts.join(',') : '',
                    product_count: selectedVMPs.length.toString(),
                    organisation_count: plan.cohortTrusts.length.toString(),
                    search_type: searchType,
                    quantity_type: selectedQuantityType || 'auto',
                    analysis_mode: effectiveMode || 'trust',
                    analysis_scope: runScope,

                    used_url_params: urlState.isHydrated && Object.keys(urlState).some(key => {
                        const value = urlState[key];
                        return value !== null && value !== undefined &&
                               (Array.isArray(value) ? value.length > 0 : true);
                    }),
                }
            });
        }

        try {
            const resolvedProducts = await resolveProductsToVMPs(selectedVMPs);
            const payload = await executeAnalysisFetch({
                csrftoken,
                resolvedProducts,
                quantityType: $analyseOptions.quantityType,
                selectedScope: runScope,
                odsCodes: plan.odsCodes,
            });

            const complete = completeAnalysisRun({
                plan,
                payload,
                selectedVMPs,
                searchType,
                quantityType: $analyseOptions.quantityType,
                selectedScope: runScope,
                selectedScopeFilters: runScopeFilters,
                showPercentilesFromUrl: urlState.showPercentiles,
                excludedVmps: urlState.excludedVmps,
                analyseOptions,
                updateResults,
            });
            dispatch('analysisComplete', complete);
        } catch (error) {
            console.error("Error fetching filtered data:", error);
            errorMessage = "An error occurred while fetching data. Please try again.";
            dispatch('analysisError', { error: errorMessage });
        } finally {
            isAnalysisRunning = false;
        }
    }


    function handleVMPSelection(event) {
        
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: event.detail.items
        }));

        selectQuantityType(event.detail.items);
    }

    function handleODSSelection(event) {
        const selectedItems = event.detail.selectedItems || [];
        const usedOrganisationSelection = event.detail.usedOrganisationSelection;
        const previouslyHadTrusts = selectedTrusts.length > 0;
        const willHaveTrusts = selectedItems.length > 0;

        organisationSearchStore.updateSelection(selectedItems, usedOrganisationSelection);

        if (!isAuth) {
            analyseOptions.setSelectedOrganisations(selectedItems);
            analyseOptions.setRememberedOverlayOrganisations(selectedItems);

            if (urlState.showPercentiles === null) {
                if (!previouslyHadTrusts && willHaveTrusts) {
                    resultsStore.update(store => ({ ...store, showPercentiles: false }));
                } else if (previouslyHadTrusts && !willHaveTrusts) {
                    resultsStore.update(store => ({ ...store, showPercentiles: true }));
                }
            }
        }
    }

    function handleScopeChange(scope) {
        selectedScope = scope;

        if (
            scope === ANALYSIS_SCOPE.ALL
            || scope === ANALYSIS_SCOPE.NATIONAL
            || scope === ANALYSIS_SCOPE.TRUST
        ) {
            organisationSearchStore.setAvailableItems(Array.from($organisationSearchStore.items || []));
            organisationSearchStore.setFiltersApplied(false);
        }

        if (scope === ANALYSIS_SCOPE.TRUST || scope === ANALYSIS_SCOPE.GROUP) {
            organisationSearchStore.setFilterType('trust');
        }

        organisationSearchStore.updateSelection([]);
    }

    function toggleAdvancedOptions() {
        showAdvancedOptions = !showAdvancedOptions;
    }

    function toggleQuantityDropdown() {
        if (selectedVMPs.length === 0 || isSelectingQuantityTypes) return;
        isQuantityDropdownOpen = !isQuantityDropdownOpen;
    }

    function selectQuantityTypeFromDropdown(quantityType) {
        selectedQuantityType = quantityType;
        analyseOptions.setQuantityType(quantityType);
        isQuantityDropdownOpen = false;
    }


    function resetSelections() {
        
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: [],
            searchType: 'vmp',
            quantityType: null
        }));
        organisationSearchStore.updateSelection([]);
        analyseOptions.setSelectedOrganisations([]);
        analyseOptions.setRememberedOverlayOrganisations([]);
        analyseOptions.setSelectedChartRegions(allOverlaySelection());
        analyseOptions.setSelectedChartIcbs(allOverlaySelection());
        
        recommendedQuantityTypes = [];
        showAdvancedOptions = false;
        selectedQuantityType = null;
        selectedScope = ANALYSIS_SCOPE.ALL;
        selectedScopeFilters = createEmptyScopeFilters();
        organisationSearchStore.setFilterType('trust');
        modeSelectorStore.reset();
        urlState = {
            mode: null,
            showPercentiles: null,
            excludedVmps: [],
            isHydrated: false,
            validationErrors: [],
            suppressSync: true
        };
        resultsStore.update(store => ({
            ...store,
            showPercentiles: true,
            excludedVmps: [],
            scope: ANALYSIS_SCOPE.ALL,
            scopeFilters: createEmptyScopeFilters(),
            inScopeTrusts: []
        }));
        dispatch('urlValidationErrors', { errors: [] });

        errorMessage = '';

        const searchComponent = document.querySelector('analysis-builder search-component');
        if (searchComponent) {
            searchComponent.clearInput();
        }

        if (typeof window !== 'undefined') {
            setUrlParams({}, SUPPORTED_ANALYSIS_PARAMS);
        }

        urlState.suppressSync = false;
    }


    function handleClearAnalysis() {
        resetSelections();
        dispatch('analysisClear');
    }

    function handleScopeFiltersChange(event) {
        selectedScopeFilters = normaliseScopeFilters(event?.detail || {});
        if (hasAnyScopeFilters(selectedScopeFilters)) {
            errorMessage = '';
        }
    }

</script>

<div class="mb-6">
  <div>
    <!-- Header -->
    <div class="p-4 sm:p-6 bg-white rounded-lg w-full">
      <div class="grid gap-6">
        <!-- Tabs -->
        <div>
          <p class="text-sm text-oxford">
           
              Run a custom analysis of hospitals stock control data using the options below. You can analyse 
              specific medicines or groups of medicines across different NHS Trusts.

          </p>
        </div>

        <div class="grid gap-6">
          <!-- Product Selection -->
          <div class="grid gap-4">
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">
                Select product(s)
              </h3>
              <div class="relative inline-block group">
                <button type="button" aria-label="Product selection information" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                  <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                  </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                            group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-5 rounded-md shadow-lg bg-white 
                            ring-1 ring-black ring-opacity-5 p-4">
                  <p class="text-sm text-gray-500">
                      Search for and select products to include to analyse. You can select individual products
                      or groups of products by ingredient, product group, or ATC code. See <a href="/faq/#which-medicines-and-devices-are-included" class="underline font-semibold" target="_blank">the FAQs</a> for more information of what products are available.
                  </p>
                </div>
              </div>
            </div>
            <div class="relative">
              <ProductSearch
                on:selectionChange={handleVMPSelection}
                on:vmpCountChange={handleVmpCountChange}
                {maxVmpCount}
              />
            </div>
          </div>

          {#if !isAuth}
          <!-- Trust Selection -->
          <div class="grid gap-0">
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Select NHS Trust(s) (optional)</h3>
              <div class="relative inline-block group">
                <button type="button" aria-label="Trust selection information" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                  <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                  </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                  group-hover:scale-100 w-[300px] -translate-x-[85%] left-1/2 top-5 rounded-md shadow-lg bg-white  
                  ring-1 ring-black ring-opacity-5 p-4">
                  <div class="text-sm text-gray-500 space-y-3">
                    <div class="space-y-1 text-xs">
                      <p>Select up to 10 NHS Trusts.</p>
                      <ul>
                        <li><strong>No trusts selected:</strong> Shows national data with regional/ICB breakdowns available</li>
                        <li><strong>Trusts selected:</strong> Filters analysis results to the selected trusts</li>
                      </ul>
                    </div>
                    <div>
                      <p class="text-xs">
                        See <a href="/faq/#how-do-i-see-icb-regional-and-national-breakdowns" class="underline font-semibold" target="_blank">the FAQs</a> for more details
                        of how trust selection affects analysis modes and <a href="/faq/#which-nhs-trusts-are-included" class="underline font-semibold" target="_blank">which trusts are included</a>.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <div class="relative min-w-0">
              <OrganisationSearch
                source={organisationSearchStore}
                overlayMode={false}
                on:selectionChange={handleODSSelection}
                maxItems={10}
                hideSelectAll={true}
                showTitle={false}
              />
            </div>
          </div>
          {/if}

        </div>

        <!-- Analysis Controls -->
        <div class="mt-2 pt-4 border-t border-gray-200">
          <div class="flex flex-col gap-4">
            <!-- Advanced Options -->
            <div class="grid gap-4">
              <button
                type="button"
                on:click={toggleAdvancedOptions}
                class="text-left text-sm font-medium text-oxford underline transition-colors duration-200"
              >
                {showAdvancedOptions ? 'Hide advanced options' : 'Show advanced options'}
              </button>

              {#if showAdvancedOptions}
                <div class="space-y-4">
                  {#if isAuth}
                    <AnalysisScopePanel
                      {selectedScope}
                      {selectedScopeFilters}
                      source={organisationSearchStore}
                      on:scopeChange={(e) => handleScopeChange(e.detail)}
                      on:filtersChange={handleScopeFiltersChange}
                      on:selectionChange={handleODSSelection}
                    />
                  {/if}

                  <div class="space-y-3">
                    <h3 class="text-base sm:text-lg font-semibold text-oxford">
                      {isAuth ? 'Quantity Type' : 'Quantity Type (optional)'}
                    </h3>
                    <p class="text-sm text-oxford">
                      There are different ways to <a href="/faq/#what-does-quantity-mean" class="underline font-semibold" target="_blank">measure the quantity of medicines issued</a>. The most appropriate quantity for the selected products is automatically selected (<a href="/faq/#how-is-the-quantity-type-used-for-an-analysis-chosen" class="underline font-semibold" target="_blank">see how in the FAQs</a>). If you would like to select an alternative quantity type, you can do so below.
                    </p>
                    <div class="space-y-2">
                        <div class="quantity-dropdown-container relative">
                            <button
                                type="button"
                                on:click={toggleQuantityDropdown}
                                disabled={selectedVMPs.length === 0 || isSelectingQuantityTypes}
                                class="w-full flex items-center justify-between p-2 border border-gray-300 bg-white focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oxford-500 text-sm
                                       disabled:bg-gray-100 disabled:text-gray-500 disabled:cursor-not-allowed
                                       {isQuantityDropdownOpen ? 'rounded-t-md border-b-0' : 'rounded-md hover:border-gray-400'}"
                            >
                                <div class="flex items-center space-x-2">
                                    {#if selectedQuantityType}
                                        <span class="text-gray-900">{selectedQuantityType}</span>
                                    {:else}
                                        <span class="text-gray-500">
                                            {isSelectingQuantityTypes ? 'Selecting...' : 'Choose quantity type...'}
                                        </span>
                                    {/if}
                                </div>
                                
                                <svg class="w-4 h-4 text-gray-400 transition-transform duration-200 {isQuantityDropdownOpen ? 'rotate-180' : ''}" 
                                     fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                                </svg>
                            </button>

                            {#if isQuantityDropdownOpen}
                                <div class="w-full bg-white border border-gray-300 border-t-0 rounded-b-md shadow-lg divide-y divide-gray-200">
                                    {#each availableQuantityTypes as quantityType}
                                        {@const isRecommended = recommendedQuantityTypes.includes(quantityType)}
                                        {@const isSelected = selectedQuantityType === quantityType}
                                        
                                        <button
                                            type="button"
                                            on:click={() => selectQuantityTypeFromDropdown(quantityType)}
                                            class="w-full p-2 text-left transition duration-150 ease-in-out hover:bg-gray-50 focus:bg-gray-50 focus:outline-none
                                                   {isSelected ? 'bg-oxford-100 text-oxford-500' : 'text-gray-900'}"
                                        >
                                            <div class="flex items-center justify-between">
                                                <div class="flex items-center gap-2">
                                                    <span class="text-sm">{quantityType}</span>
                                                </div>
                                                
                                                {#if isRecommended}
                                                    <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200">
                                                        Recommended
                                                    </span>
                                                {/if}
                                            </div>
                                        </button>
                                    {/each}
                                </div>
                            {/if}
                        </div>
                        
                        {#if selectedVMPs.length === 0}
                            <p class="text-xs text-gray-500">Select products first to enable quantity type selection</p>
                        {/if}
                    </div>
                  </div>
                </div>
              {/if}
            </div>

            <!-- Error Message -->
            {#if errorMessage}
              <div class="p-3 bg-red-100 border border-red-400 text-red-700 rounded">
                {errorMessage}
              </div>
            {/if}

            <!-- Action Buttons -->
            <div class="flex gap-2 justify-between">
              <button
                on:click={() => runAnalysis()}
                disabled={isAnalysisRunning || isSelectingQuantityTypes || resolvedVmpOverLimit}
                class="w-64 px-6 sm:px-8 py-2 sm:py-2.5 bg-oxford-50 text-oxford-600 font-semibold rounded-md hover:bg-oxford-100 transition-colors duration-200
                     disabled:bg-gray-50 disabled:text-gray-400 disabled:cursor-not-allowed"
              >
                {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
              </button>
              <button
                on:click={handleClearAnalysis}
                title="Clear Analysis"
                class="p-2 sm:p-2.5 bg-white text-gray-700 font-normal rounded-md hover:bg-gray-100 transition-colors duration-200 border border-gray-200"
              >
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
