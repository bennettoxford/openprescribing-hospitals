<svelte:options customElement={{
    tag: 'analysis-builder',
    shadow: 'none'
  }} />

<script>
    import { onMount, tick } from 'svelte';
    import '../../../styles/styles.css';
    import ProductSearch from '../../common/ProductSearch.svelte';
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import OrganisationSearchFiltered from '../../common/OrganisationSearchFiltered.svelte';
    import { createEventDispatcher } from 'svelte';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { resultsStore, updateResults } from '../../../stores/resultsStore';
    import { modeSelectorStore } from '../../../stores/modeSelectorStore';
    import { normaliseMode } from '../../../utils/analyseUtils.js';
    import {
        getCookie,
        getUrlParams,
        formatArrayParam,
        setUrlParams,
        cleanupUrl,
    } from '../../../utils/utils';
    
    const dispatch = createEventDispatcher();

    const availableQuantityTypes = [
        'SCMD Quantity',
        'Unit Dose Quantity', 
        'Ingredient Quantity',
        'Defined Daily Dose Quantity'
    ];

    const PRODUCT_PARAM_BY_TYPE = {
        vmp: 'vmps',
        vtm: 'vtms',
        ingredient: 'ingredients',
        atc: 'atcs'
    };

    const PRODUCT_TYPE_ORDER = Object.keys(PRODUCT_PARAM_BY_TYPE);

    const ANALYSIS_TRUSTS_PARAM = 'trusts';
    const ANALYSIS_QUANTITY_PARAM = 'quantity';
    const ANALYSIS_MODE_PARAM = 'mode';
    const ANALYSIS_PERCENTILES_PARAM = 'show_percentiles';
    const ANALYSIS_EXCLUDED_VMPS_PARAM = 'excluded_vmps';

    const SUPPORTED_ANALYSIS_PARAMS = [
        ...Object.values(PRODUCT_PARAM_BY_TYPE),
        ANALYSIS_TRUSTS_PARAM,
        ANALYSIS_QUANTITY_PARAM,
        ANALYSIS_MODE_PARAM,
        ANALYSIS_PERCENTILES_PARAM,
        ANALYSIS_EXCLUDED_VMPS_PARAM
    ];

    const QUANTITY_TYPE_CODES = {
        'SCMD Quantity': 'scmd',
        'Unit Dose Quantity': 'dose',
        'Ingredient Quantity': 'ingredient',
        'Defined Daily Dose Quantity': 'ddd'
    };

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isSelectingQuantityTypes = false;
    let showAdvancedOptions = false;
    let recommendedQuantityTypes = [];
    let selectedQuantityType = null;
    let isQuantityDropdownOpen = false;

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
    $: selectedMode = $modeSelectorStore.selectedMode;
    $: showPercentiles = $resultsStore.showPercentiles;

    $: urlState.excludedVmps = Array.isArray($resultsStore.excludedVmps)
        ? Array.from(new Set($resultsStore.excludedVmps.filter(Boolean).map(String))).sort()
        : [];

    $: if (selectedMode && urlState.mode) {
        urlState.mode = null;
    }

    $: effectiveMode = normaliseMode(selectedMode) || urlState.mode;

    $: if (!urlState.suppressSync && urlState.validationErrors.length === 0) {
        const currentMode = effectiveMode || 'trust';
        const currentShowPercentiles = getShowPercentilesParam(currentMode, selectedTrusts, showPercentiles);
        updateAnalysisUrl(
            selectedVMPs,
            selectedTrusts,
            selectedQuantityType,
            effectiveMode,
            currentShowPercentiles,
            urlState.excludedVmps
        );
    }

    export let orgData = null;
    export let isAuthenticated = false;
    $: isAuth = isAuthenticated === true;
    export let mindate = null;
    export let maxdate = null;

    function getShowPercentilesParam(mode, trusts, showPercentilesValue) {
        const hasTrusts = Array.isArray(trusts) && trusts.length > 0;
        if (mode === 'trust' && hasTrusts && showPercentilesValue === true) {
            return 'true';
        }
        return null;
    }


    function encodeQuantityType(quantityType) {
        if (!quantityType) return null;
        const trimmed = quantityType.trim();
        return QUANTITY_TYPE_CODES[trimmed] || null;
    }


    function updateAnalysisUrl(products = [], trusts = [], quantityType = null, mode = null, showPercentiles = null, excludedVmps = []) {
        if (typeof window === 'undefined') return;

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

        const trustCodes = trusts.map(name => organisationSearchStore.getOrgCode(name)).filter(Boolean);
        if (trustCodes.length > 0) {
            params[ANALYSIS_TRUSTS_PARAM] = formatArrayParam(trustCodes);
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

        if (excludedVmps.length > 0) {
            params[ANALYSIS_EXCLUDED_VMPS_PARAM] = formatArrayParam(excludedVmps);
        }

        setUrlParams(params, SUPPORTED_ANALYSIS_PARAMS);
    }

    function buildValidationParams(urlParams) {
        const queryParams = new URLSearchParams();

        PRODUCT_TYPE_ORDER.forEach(type => {
            const paramName = PRODUCT_PARAM_BY_TYPE[type];
            const paramValue = urlParams.get(paramName);
            if (paramValue?.trim()) {
                queryParams.append(paramName, paramValue);
            }
        });

        [ANALYSIS_TRUSTS_PARAM, ANALYSIS_QUANTITY_PARAM, ANALYSIS_MODE_PARAM,
         ANALYSIS_PERCENTILES_PARAM, ANALYSIS_EXCLUDED_VMPS_PARAM].forEach(param => {
            const value = urlParams.get(param);
            if (value?.trim()) {
                queryParams.append(param, value.trim());
            }
        });

        return queryParams;
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

    function updateStoresFromValidation(data) {
        if (data.valid_products?.length > 0) {
            analyseOptions.update(options => ({
                ...options,
                selectedVMPs: data.valid_products
            }));

            if (data.quantity_type) {
                analyseOptions.setQuantityType(data.quantity_type.name);
                selectedQuantityType = data.quantity_type.name;
                if (data.quantity_type.code) {
                    setUrlParams({ [ANALYSIS_QUANTITY_PARAM]: data.quantity_type.code }, [ANALYSIS_QUANTITY_PARAM]);
                }
            } else {
                selectQuantityType(data.valid_products);
            }
        }

        if (data.valid_trusts?.length > 0) {
            const trustNames = data.valid_trusts.map(trust => trust.ods_name);
            organisationSearchStore.updateSelection(trustNames);
            analyseOptions.setSelectedOrganisations(trustNames);
        }

        const hasValidTrusts = Array.isArray(data.valid_trusts) && data.valid_trusts.length > 0;
        const responseExcludedVmps = Array.isArray(data.excluded_vmps)
            ? Array.from(new Set(data.excluded_vmps.map(String).filter(Boolean))).sort()
            : [];


        let finalShowPercentiles;
        if (urlState.showPercentiles === 'true') {
            finalShowPercentiles = true;
        } else if (urlState.showPercentiles === 'false') {
            finalShowPercentiles = false;
        } else if (data.mode === 'trust' && hasValidTrusts) {
            finalShowPercentiles = data.show_percentiles ?? false;
        } else {
            finalShowPercentiles = !hasValidTrusts;
        }

        resultsStore.update(store => ({
            ...store,
            showPercentiles: finalShowPercentiles,
            excludedVmps: responseExcludedVmps
        }));

        urlState.excludedVmps = responseExcludedVmps;
    }

    async function hydrateFromUrl() {
        if (urlState.isHydrated || typeof window === 'undefined') {
            urlState.suppressSync = false;
            return;
        }

        urlState.isHydrated = true;

        try {
            const urlParams = getUrlParams();
            urlState.mode = normaliseMode(urlParams.get(ANALYSIS_MODE_PARAM));
            urlState.showPercentiles = urlParams.get(ANALYSIS_PERCENTILES_PARAM);

            const queryParams = buildValidationParams(urlParams);
            if (!queryParams.toString()) {
                urlState.suppressSync = false;
                return;
            }

            const response = await fetch(`/api/validate-analysis-params/?${queryParams}`, {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });

            if (response.ok) {
                const data = await response.json();
                const isValid = handleValidationResponse(data);

                if (isValid) {
                    updateStoresFromValidation(data);

                    if (typeof window !== 'undefined') {
                        cleanupUrl(SUPPORTED_ANALYSIS_PARAMS);
                    }

                    await tick();
                    runAnalysis();
                }
            } else {
                console.error('Failed to validate URL parameters:', response.status);
            }
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
                        predecessor_map: parsedData.predecessorMap || parsedData.predecessor_map || {},
                        trust_types: parsedData.trust_types || {},
                        org_regions: parsedData.org_regions || {},
                        org_icbs: parsedData.org_icbs || {},
                        regions_hierarchy: parsedData.regions_hierarchy || []
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
    
    async function selectQuantityType(selectedVMPs) {

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
            
            selectedQuantityType = data.selected_quantity_type;
            recommendedQuantityTypes = data.recommended_quantity_types || [];
            analyseOptions.setQuantityType(selectedQuantityType);
            
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

    async function runAnalysis() {
        if (isAnalysisRunning) return;

        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = "Please select at least one product or ingredient.";
            return;
        }

        
        modeSelectorStore.reset();

        isAnalysisRunning = true;
        dispatch('analysisStart');
        
        if (typeof window !== 'undefined' && window.plausible) {
            window.plausible('Analysis Run', {
                props: {
                    all_products: selectedVMPs.map(p => p.code).join(','),
                    all_organisations: $organisationSearchStore.selectedItems.join(','),
                    product_count: selectedVMPs.length.toString(),
                    organisation_count: $organisationSearchStore.selectedItems.length.toString(),
                    search_type: searchType,
                    quantity_type: selectedQuantityType || 'auto',
                    analysis_mode: effectiveMode || 'trust',

                    used_url_params: urlState.isHydrated && Object.keys(urlState).some(key => {
                        const value = urlState[key];
                        return value !== null && value !== undefined &&
                               (Array.isArray(value) ? value.length > 0 : true);
                    }),
                }
            });
        }
        
        let endpoint = '/api/get-quantity-data/';
        
        try {
            const resolvedProducts = await resolveProductsToVMPs(selectedVMPs);

            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrftoken
                },
                body: JSON.stringify({
                    names: resolvedProducts,
                    search_type: searchType,
                    quantity_type: $analyseOptions.quantityType
                })
            });

            if (!response.ok) {
                const data = await response.json();
                throw new Error(data.error || `HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
  
            analyseOptions.runAnalysis({
                selectedVMPs,
                searchType,
                organisations: $organisationSearchStore.selectedItems
            });

            const updateOptions = {
                searchType,
                quantityType: $analyseOptions.quantityType,
                selectedOrganisations: $organisationSearchStore.selectedItems,
                predecessorMap: $organisationSearchStore.predecessorMap
            };

            if (urlState.showPercentiles !== null) {
                updateOptions.showPercentiles = urlState.showPercentiles === 'true';
            }

            if (urlState.excludedVmps.length > 0) {
                updateOptions.excludedVmps = urlState.excludedVmps;
            }

            updateResults(data, updateOptions);

            dispatch('analysisComplete', { 
                data: Array.isArray(data) ? data : [data],
                searchType,
                selectedOrganisations: $organisationSearchStore.selectedItems
            });
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
        const { selectedItems, usedOrganisationSelection } = event.detail;
        const previouslyHadTrusts = selectedTrusts.length > 0;
        const willHaveTrusts = Array.isArray(selectedItems) && selectedItems.length > 0;

        organisationSearchStore.updateSelection(selectedItems, usedOrganisationSelection);
        analyseOptions.setSelectedOrganisations(selectedItems);

        if (!previouslyHadTrusts && willHaveTrusts) {
            if (urlState.showPercentiles === null) {
                resultsStore.update(store => ({ ...store, showPercentiles: false }));
            }
        } else if (previouslyHadTrusts && !willHaveTrusts) {
            if (urlState.showPercentiles === null) {
                resultsStore.update(store => ({ ...store, showPercentiles: true }));
            }
        }
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
        
        recommendedQuantityTypes = [];
        showAdvancedOptions = false;
        selectedQuantityType = null;
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
            excludedVmps: []
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
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
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
              <ProductSearch on:selectionChange={handleVMPSelection}/>
            </div>
          </div>

          <!-- Trust Selection -->
          <div class="grid gap-0">
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Select NHS Trust(s) (optional)</h3>
              <div class="relative inline-block group">
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                  <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                  </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                  group-hover:scale-100 w-[300px] -translate-x-[85%] left-1/2 top-5 rounded-md shadow-lg bg-white  
                  ring-1 ring-black ring-opacity-5 p-4">
                  <div class="text-sm text-gray-500 space-y-3">
                    <div class="space-y-1 text-xs">
                      <p>{#if isAuth}Select NHS Trusts to filter the analysis.{:else}Select up to 10 NHS Trusts.{/if}</p>
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
              {#if isAuth}
                <OrganisationSearchFiltered
                  source={organisationSearchStore}
                  overlayMode={false}
                  on:selectionChange={handleODSSelection}
                  showTitle={false}
                  filterAutoSelectsAll={false}
                />
              {:else}
                <OrganisationSearch
                  source={organisationSearchStore}
                  overlayMode={false}
                  on:selectionChange={handleODSSelection}
                  maxItems={10}
                  hideSelectAll={true}
                  showTitle={false}
                />
              {/if}
            </div>
          </div>

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
                  <div class="space-y-3">
                    <h3 class="text-base sm:text-lg font-semibold text-oxford">Quantity Type (optional)</h3>
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
                on:click={runAnalysis}
                disabled={isAnalysisRunning || isSelectingQuantityTypes}
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
