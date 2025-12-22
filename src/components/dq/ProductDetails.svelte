<svelte:options customElement={{
    tag: 'product-details',
    shadow: 'none',
}} />

<script>
  import { onMount } from 'svelte';
  import ProductSearch from '../common/ProductSearch.svelte';
  import { getCookie, formatStrength } from '../../utils/utils';
  import { analyseOptions } from '../../stores/analyseOptionsStore';
  
  const csrftoken = getCookie('csrftoken');
  
  let selectedItems = [];
  let isLoading = false;
  let products = [];
  let groupedProducts = {};
  let error = null;
  let searchRef;
  let expandedGroups = {};
  let expandedLogic = {};

  const API_ENDPOINTS = {
    PRODUCT_DETAILS: '/api/get-product-details/'
  };

  const EXTERNAL_LINKS = {
    DM_D_BROWSER_VTM: 'https://openprescribing.net/dmd/vtm/',
    DM_D_BROWSER_VMP: 'https://openprescribing.net/dmd/vmp/',
    ATC_DDD: 'https://atcddd.fhi.no/atc_ddd_index/?code='
  };

  onMount(() => {
    analyseOptions.update(options => ({
      ...options,
      selectedVMPs: [],
      searchType: 'product'
    }));
  });


  function groupProductsByVTM(products) {
    const grouped = {};
    
    products.forEach(product => {
      const vtmKey = product.vtm_name || 'Unknown VTM';
      if (!grouped[vtmKey]) {
        grouped[vtmKey] = {
          vtm_name: vtmKey,
          products: []
        };
      }
      grouped[vtmKey].products.push(product);
    });
    
    return grouped;
  }

  function toggleGroupExpansion(vtmName) {
    expandedGroups[vtmName] = !expandedGroups[vtmName];
    expandedGroups = {...expandedGroups};
  }
  
  async function fetchFromAPI(selectedItems) {
    const response = await fetch(API_ENDPOINTS.PRODUCT_DETAILS, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': csrftoken
      },
      body: JSON.stringify({
        names: selectedItems
      })
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || 'Failed to fetch product details');
    }

    return await response.json();
  }

  function processProductData(data) {
    products = data;
    groupedProducts = groupProductsByVTM(data);
    
    products.forEach(product => {
      expandedLogic[`${product.vmp_code}_dose`] = false;
      expandedLogic[`${product.vmp_code}_ddd`] = false;
      expandedLogic[`${product.vmp_code}_ingredient`] = false;
    });
    
    Object.keys(groupedProducts).forEach(vtmName => {
      expandedGroups[vtmName] = true;
    });
    expandedGroups = {...expandedGroups};
    expandedLogic = {...expandedLogic};
  }

  async function fetchProductDetails() {
    if (!validateSelection()) return;
    
    try {
      setLoadingState(true);
      const data = await fetchFromAPI(selectedItems);
      processProductData(data);
    } catch (err) {
      console.error('Error fetching product details:', err);
      error = err.message || 'An error occurred while fetching product details';
      products = [];
      groupedProducts = {};
    } finally {
      setLoadingState(false);
    }
  }

  function validateSelection() {
    if (selectedItems.length === 0) {
      error = "Please select at least one product";
      return false;
    }
    return true;
  }

  function setLoadingState(loading) {
    isLoading = loading;
    if (loading) {
      error = null;
    }
  }

  function handleSelectionChange(event) {
    selectedItems = event.detail.items;

    analyseOptions.update(options => ({
      ...options,
      selectedVMPs: selectedItems
    }));
    
    if (products.length > 0 || error) {
      products = [];
      groupedProducts = {};
      error = null;
    }
  }

  function clearResults() {
    products = [];
    groupedProducts = {};
    error = null;
    
    selectedItems = [];
    analyseOptions.update(options => ({
      ...options,
      selectedVMPs: []
    }));
  }

  function toggleLogicExpansion(productCode, logicType) {
    const key = `${productCode}_${logicType}`;
    expandedLogic[key] = !expandedLogic[key];
    expandedLogic = {...expandedLogic};
  }

  function createQuantityItem(product, type) {
    switch (type) {
      case 'scmd':
        return {
          label: 'SCMD',
          available: product.has_scmd_quantity,
          units: product.scmd_units,
          hasLogic: false,
          tooltip: 'Quantity data from Secondary Care Medicines Data'
        };
      case 'dose':
        return {
          label: 'Unit dose',
          available: product.has_dose,
          units: product.dose_units,
          hasLogic: true,
          tooltip: 'Quantity in basis units / udfs in basis units'
        };
      case 'ddd':
        return {
          label: 'DDD',
          available: product.has_ddd_quantity,
          units: product.has_ddd_quantity ? ['DDD'] : null,
          hasLogic: true,
          tooltip: 'Defined Daily Dose quantity'
        };
      case 'ingredient':
        return {
          label: 'Ingredient',
          available: product.has_ingredient_quantities,
          units: product.ingredient_units,
          hasLogic: true,
          tooltip: 'Individual ingredient quantities'
        };
      default:
        return null;
    }
  }
</script>

<div class="px-4 sm:px-8 py-4">
  <div>
  <div class="grid gap-4">
    <div class="flex items-center">
      <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Select products</h3>
    </div>
    
    <div class="relative">
      <ProductSearch 
        on:selectionChange={handleSelectionChange} 
        type="product"
        bind:this={searchRef}
      />
    </div>
  
  </div>

  <div class="mt-2 py-4 border-b border-gray-200">
    <div class="flex flex-col gap-4">
      {#if error}
        <div class="p-3 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      {/if}

      <div class="flex gap-2 justify-between">
        <button
          on:click={fetchProductDetails}
          disabled={isLoading || selectedItems.length === 0}
          class="w-64 px-6 sm:px-8 py-2 sm:py-2.5 bg-oxford-50 text-oxford-600 font-semibold rounded-md hover:bg-oxford-100 transition-colors duration-200
               disabled:bg-gray-50 disabled:text-gray-400 disabled:cursor-not-allowed"
        >
          {isLoading ? 'Loading details...' : 'Fetch Product Details'}
        </button>
        
        {#if selectedItems.length > 0 || products.length > 0 || error}
          <button
            on:click={clearResults}
            title="Clear Results"
            class="p-2 sm:p-2.5 bg-white text-gray-700 font-normal rounded-md hover:bg-gray-100 transition-colors duration-200 border border-gray-200"
          >
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
            </svg>
          </button>
        {/if}
      </div>
    </div>
  </div>
</div>

<div class="mt-8 border border-gray-300 shadow-sm rounded-lg overflow-hidden">
  <div class="px-4 py-5 bg-white">

    {#if isLoading}
      <div class="flex justify-center items-center py-8">
        <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-oxford-600"></div>
        <span class="ml-3 text-sm text-oxford-600">Loading product details...</span>
      </div>
    {:else if products.length > 0}
      <p class="text-sm text-gray-600 mb-4">
        Showing details for {products.length} product{products.length !== 1 ? 's' : ''} grouped by VTM.
      </p>
      
      <div class="overflow-x-auto">
        <div class="overflow-y-auto max-h-[80rem] space-y-4">
          {#each Object.entries(groupedProducts) as [vtmName, group]}
            <div class="rounded-md border border-gray-300 shadow-sm overflow-hidden bg-white">
              <!-- VTM Header -->
              <button 
                on:click={() => toggleGroupExpansion(vtmName)}
                class="w-full bg-oxford-100 border-b border-gray-300 px-3 py-3 flex justify-between items-center text-left"
              >
                <div class="flex items-center">
                  <div>
                    <div class="text-base font-medium text-oxford-800">
                      {vtmName}
                    </div>
                    <div class="text-xs text-oxford-600 mt-0.5">
                      {group.products.length} product{group.products.length !== 1 ? 's' : ''}
                    </div>
                  </div>
                </div>
                <div class="flex items-center gap-2">
                  <a 
                    href={`${EXTERNAL_LINKS.DM_D_BROWSER_VTM}${group.products[0]?.vtm_code || ''}/`}
                    target="_blank"
                    rel="noopener noreferrer" 
                    class="text-xs text-oxford-600 hover:text-oxford-800 hover:underline inline-flex items-center gap-1"
                  >
                    View VTM on dm+d browser
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                    </svg>
                  </a>
                  <div class="text-oxford-700">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 transition-transform duration-200 {expandedGroups[vtmName] ? 'rotate-180' : ''}" viewBox="0 0 20 20" fill="currentColor">
                      <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                    </svg>
                  </div>
                </div>
              </button>
              
              {#if expandedGroups[vtmName]}
                <!-- VMP Products -->
                <div class="divide-y divide-gray-200">
                  {#each group.products as product}
                    <div class="bg-white">
                      <!-- VMP Header -->
                      <div class="bg-oxford-50 border-b border-gray-300 px-3 py-2.5 flex justify-between gap- 2 items-center">
                        <div class="flex items-center">
                          <div>
                            <div class="text-sm font-medium text-gray-900">{product.vmp_name}</div>
                          </div>
                        </div>
                        <a 
                        href={`${EXTERNAL_LINKS.DM_D_BROWSER_VMP}${product.vmp_code}/`}
                        target="_blank"
                        rel="noopener noreferrer" 
                        class="text-xs text-oxford-600 hover:text-oxford-800 hover:underline inline-flex items-center gap-1"
                      >
                        View VMP on dm+d browser
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                        </svg>
                      </a>
                      </div>
                      
                      <!-- Details section -->
                      <div class="grid grid-cols-1 xl:grid-cols-2 gap-6 px-3 py-3 break-words">
                        <!-- Product Information Section -->
                        <div class="space-y-1">
                          <h4 class="text-sm font-semibold text-gray-900 border-b border-gray-200 pb-2">Product Information</h4>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">VTM:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.vtm_name && product.vtm_code}
                                {product.vtm_name} ({product.vtm_code})
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>

                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">VMP code:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">{product.vmp_code}</span>
                          </div>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-start">
                            <span class="text-sm font-medium whitespace-nowrap">Ingredients:</span>
                            <div class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.ingredient_logic && product.ingredient_logic.length > 0}
                                <div class="space-y-1">
                                  {#each product.ingredient_logic as ingredientCalc}
                                    <div class="flex flex-wrap items-baseline gap-2">
                                      {#if ingredientCalc.ingredient}
                                        <span class="text-gray-900">{ingredientCalc.ingredient}</span>
                                        {#if ingredientCalc.strength_info}
                                          <span class="text-xs text-gray-600">
                                            (<span class="font-medium">Strength:</span> {#if ingredientCalc.strength_info.numerator_value}{formatStrength(ingredientCalc.strength_info.numerator_value)}{ingredientCalc.strength_info.numerator_uom ? ` ${ingredientCalc.strength_info.numerator_uom}` : ''}{#if ingredientCalc.strength_info.denominator_value}/{formatStrength(ingredientCalc.strength_info.denominator_value)}{ingredientCalc.strength_info.denominator_uom ? ` ${ingredientCalc.strength_info.denominator_uom}` : ''}{/if}{/if})
                                          </span>
                                        {:else}
                                          <span class="text-xs text-gray-400 italic">(no strength info)</span>
                                        {/if}
                                      {:else}
                                        <span class="text-gray-400">-</span>
                                      {/if}
                                    </div>
                                  {/each}
                                </div>
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </div>
                          </div>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">Form + route:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.routes && product.routes.length > 0}
                                {product.routes.join(', ')}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>

                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">Dose form:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.df_ind}
                                {product.df_ind}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">Unit dose unit of measure:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.dose_logic?.unit_dose_uom}
                                {product.dose_logic.unit_dose_uom}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">Unit dose form size (UDFS):</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.dose_logic?.udfs}
                                {product.dose_logic.udfs}{product.dose_logic.udfs_uom ? ` ${product.dose_logic.udfs_uom}` : ''}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">ATC codes:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.atc_codes && product.atc_codes.length > 0}
                                {#each product.atc_codes as code, i}
                                  <a 
                                    href={`${EXTERNAL_LINKS.ATC_DDD}${code}`}
                                    target="_blank"
                                    rel="noopener noreferrer" 
                                    class="text-oxford-600 hover:text-oxford-800 hover:underline"
                                  >
                                    {code}
                                  </a>{i < product.atc_codes.length - 1 ? ', ' : ''}
                                {/each}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>
                          
                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">DDD:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.ddd_info}
                                {product.ddd_info}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>

                          <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                            <span class="text-sm font-medium whitespace-nowrap">DDD route:</span>
                            <span class="text-sm break-words hyphens-auto min-w-0">
                              {#if product.ddd_info && product.who_routes && product.who_routes.length > 0}
                                {product.who_routes.join(', ')}
                              {:else}
                                <span class="text-gray-400">-</span>
                              {/if}
                            </span>
                          </div>
                        </div>

                        <!-- Quantity Information Section -->
                        <div class="space-y-3">
                          <div class="flex items-center border-b border-gray-200 pb-2">
                            <h4 class="text-sm font-semibold text-gray-900">Quantity Information</h4>
                            <div class="relative inline-block group ml-2">
                              <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                                <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                  <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                                </svg>
                              </button>
                              <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                                          group-hover:scale-100 w-[280px] -translate-x-1/2 left-1/2 top-3 rounded-md shadow-lg bg-white 
                                          ring-1 ring-black ring-opacity-5 p-4">
                                <p class="text-sm text-gray-500">
                                  This section shows the available quantity measures for this product. See the <a href="/faq/#data-contents" class="underline font-semibold" target="_blank">FAQs</a> for more details.
                                </p>
                              </div>
                            </div>
                          </div>
                          
                          <div class="overflow-x-auto">
                            <table class="min-w-full divide-y divide-gray-200 border border-gray-200 rounded-lg">
                              <thead class="bg-gray-50">
                                <tr>
                                  <th scope="col" class="px-1 md:px-3 py-2.5 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Quantity Type
                                  </th>
                                  <th scope="col" class="px-1 md:px-3 py-2.5 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Status
                                  </th>
                                  <th scope="col" class="px-1 md:px-3 py-2.5 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Units
                                  </th>
                                  <th scope="col" class="px-1 md:px-3 py-2.5 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Details
                                  </th>
                                </tr>
                              </thead>
                              <tbody class="bg-white divide-y divide-gray-200">
                                {#each ['scmd', 'dose', 'ingredient', 'ddd'] as quantityType}
                                  {@const quantityItem = createQuantityItem(product, quantityType)}
                                  
                                  <tr class="hover:bg-gray-50 {quantityItem.hasLogic && expandedLogic[`${product.vmp_code}_${quantityType}`] ? 'has-[+tr:hover]:bg-gray-50' : ''}">
                                    <!-- Quantity Type Column -->
                                    <td class="px-1 md:px-3 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                                      {quantityItem.label}
                                    </td>
                                    
                                    <!-- Status Column -->
                                    <td class="px-1 md:px-3 py-3 whitespace-nowrap">
                                      {#if quantityItem.available}
                                        <span class="inline-flex items-center gap-1.5 rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-800">
                                          <svg class="h-3 w-3" viewBox="0 0 20 20" fill="currentColor">
                                            <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.857-9.809a.75.75 0 00-1.214-.882l-3.483 4.79-1.88-1.88a.75.75 0 10-1.06 1.061l2.5 2.5a.75.75 0 001.137-.089l4-5.5z" clip-rule="evenodd" />
                                          </svg>
                                          <span class="hidden sm:inline">Available</span>
                                        </span>
                                      {:else}
                                        <span class="inline-flex items-center gap-1.5 rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">
                                          <svg class="h-3 w-3" viewBox="0 0 20 20" fill="currentColor">
                                            <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.28 7.22a.75.75 0 00-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 101.06 1.06L10 11.06l1.72 1.72a.75.75 0 101.06-1.06L11.06 10l1.72-1.72a.75.75 0 00-1.06-1.06L10 8.94 8.28 7.22z" clip-rule="evenodd" />
                                          </svg>
                                          <span class="hidden sm:inline">Not available</span>
                                        </span>
                                      {/if}
                                    </td>
                                    
                                    <!-- Units Column -->
                                    <td class="px-1 md:px-3 py-3">
                                      {#if quantityItem.units && quantityItem.units.length > 0}
                                        <span class="text-xs text-gray-900 break-words">
                                          {quantityItem.units.join(', ')}
                                        </span>
                                      {:else}
                                        <span class="text-xs text-gray-400">-</span>
                                      {/if}
                                    </td>
                                    
                                    <!-- Details Column -->
                                    <td class="px-1 md:px-3 py-3 whitespace-nowrap">
                                      {#if quantityItem.hasLogic}
                                        <button 
                                          on:click={() => toggleLogicExpansion(product.vmp_code, quantityType)}
                                          class="inline-flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 transition-colors rounded-md px-2 py-1 hover:bg-gray-100"
                                          title="Toggle calculation details"
                                        >
                                          <svg class="h-3 w-3 transition-transform duration-200 {expandedLogic[`${product.vmp_code}_${quantityType}`] ? 'rotate-180' : ''}" viewBox="0 0 20 20" fill="currentColor">
                                            <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                                          </svg>
                                          <span class="font-medium">Details</span>
                                        </button>
                                      {:else}
                                        <span class="text-xs text-gray-400">-</span>
                                      {/if}
                                    </td>
                                  </tr>
                                  
                                  <!-- Logic Content Row -->
                                  {#if quantityItem.hasLogic && expandedLogic[`${product.vmp_code}_${quantityType}`]}
                                    <tr class="!border-t-0 hover:bg-gray-50 [tr:hover_+_&]:bg-gray-50">
                                      <td colspan="4" class="px-3 py-3">
                                        {#if quantityType === 'dose'}
                                          <div class="space-y-1">
                                            {#if product.dose_logic?.logic}
                                              <div class="flex gap-2">
                                                <span class="text-xs font-medium text-gray-700 whitespace-nowrap">Calculation:</span>
                                                <span class="text-xs text-gray-600">{product.dose_logic.logic}</span>
                                              </div>
                                            {:else if product.has_dose}
                                              <div class="text-xs text-gray-600">Calculation unavailable</div>
                                            {:else}
                                              <div class="text-xs text-gray-600">Not calculated</div>
                                            {/if}
                                          </div>
                                        {:else if quantityType === 'ddd'}
                                          <div class="space-y-2">
                                            {#if product.ddd_logic?.logic}
                                              <div class="flex gap-2">
                                                <span class="text-xs font-medium text-gray-700 whitespace-nowrap">Calculation:</span>
                                                <span class="text-xs text-gray-600">{product.ddd_logic.logic}</span>
                                              </div>
                                              {#if product.ddd_logic.ingredient}
                                                <div class="flex gap-2">
                                                  <span class="text-xs font-medium text-gray-700 whitespace-nowrap">Ingredient used:</span>
                                                  <span class="text-xs text-gray-600">{product.ddd_logic.ingredient}</span>
                                                </div>
                                              {/if}
                                            {:else if product.has_ddd_quantity}
                                              <div class="text-xs text-gray-600">Calculation unavailable</div>
                                            {:else}
                                              <div class="text-xs text-gray-600">Not calculated</div>
                                            {/if}
                                          </div>
                                        {:else if quantityType === 'ingredient'}
                                          <div class="space-y-3">
                                            {#if product.ingredient_logic && product.ingredient_logic.length > 0}
                                              {#each product.ingredient_logic as ingredientCalc, index}
                                                <div class="{index > 0 ? 'border-t border-gray-200 pt-2' : ''}">
                                                  <div class="space-y-1">
                                                    {#if ingredientCalc.ingredient}
                                                      <div class="flex gap-2">
                                                        <span class="text-xs font-medium text-gray-700 whitespace-nowrap">Ingredient:</span>
                                                        <span class="text-xs text-gray-600">{ingredientCalc.ingredient}</span>
                                                      </div>
                                                    {/if}
                                                    {#if ingredientCalc.logic}
                                                      <div class="flex gap-2">
                                                        <span class="text-xs font-medium text-gray-700 whitespace-nowrap">Calculation:</span>
                                                        <span class="text-xs text-gray-600">{ingredientCalc.logic}</span>
                                                      </div>
                                                    {:else}
                                                      <div class="text-xs text-gray-600">Not calculated</div>
                                                    {/if}
                                                  </div>
                                                </div>
                                              {/each}
                                            {:else if product.has_ingredient_quantities}
                                              <div class="text-xs text-gray-600">Calculation unavailable</div>
                                            {:else}
                                              <div class="text-xs text-gray-600">Not calculated</div>
                                            {/if}
                                          </div>
                                        {/if}
                                      </td>
                                    </tr>
                                  {/if}
                                {/each}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>
          {/each}
        </div>
      </div>
      
    {:else}
      <div class="text-center py-10">
          <div class="text-sm py-4">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10 mx-auto text-gray-400 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            <p class="text-gray-600 mb-2">No products selected</p>
            <p class="text-gray-500">Use the search box above to find and select products</p>
          </div>
      </div>
    {/if}
  </div>
</div>
</div>