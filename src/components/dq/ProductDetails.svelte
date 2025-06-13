<svelte:options customElement={{
    tag: 'product-details',
    shadow: 'none',
}} />

<script>
  import { onMount } from 'svelte';
  import Search from '../common/Search.svelte';
  import { getCookie, formatNumber } from '../../utils/utils';
  import { analyseOptions } from '../../stores/analyseOptionsStore';
  
  const csrftoken = getCookie('csrftoken');
  
  let selectedItems = [];
  let isLoading = false;
  let products = [];
  let groupedProducts = {};
  let error = null;
  let searchRef;
  let expandedGroups = {};

  const API_ENDPOINTS = {
    PRODUCT_DETAILS: '/api/product-details/'
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
      isAdvancedMode: true,
      searchType: 'product'
    }));
  });

  function formatQuantity(quantityObj) {
    if (!quantityObj) return '-';
    
    const quantity = parseFloat(quantityObj.quantity);
    if (isNaN(quantity)) return '-';
    
    const formattedQuantity = formatNumber(quantity, { 
      showUnit: true, 
      unit: quantityObj.unit 
    });
    
    if (quantityObj.ddd && !quantityObj.unit.toLowerCase().includes('ddd')) {
      return `${formattedQuantity} (${quantityObj.ddd})`;
    }
    
    return formattedQuantity;
  }

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
  
  function formatIngredientQuantity(ingredient) {
    if (!ingredient || !ingredient.quantity || !ingredient.unit) return '-';
    
    const quantity = parseFloat(ingredient.quantity);
    if (isNaN(quantity)) return '-';
    
    return formatNumber(quantity, { 
      showUnit: true, 
      unit: ingredient.unit 
    });
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
    
    Object.keys(groupedProducts).forEach(vtmName => {
      expandedGroups[vtmName] = true;
    });
    expandedGroups = {...expandedGroups};
    
  }

  function handleAPIError(err) {
    console.error('Error fetching product details:', err);
    error = err.message || 'An error occurred while fetching product details';
    products = [];
    groupedProducts = {};
  }

  async function fetchProductDetails() {
    if (!validateSelection()) return;
    
    try {
      setLoadingState(true);
      const data = await fetchFromAPI(selectedItems);
      processProductData(data);
    } catch (err) {
      handleAPIError(err);
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
</script>

<div class="px-4 sm:px-8 py-4">
  <div>
  <div class="grid gap-4">
    <div class="flex items-center">
      <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Select products</h3>
    </div>
    
    <div class="relative">
      <Search 
        on:selectionChange={handleSelectionChange} 
        isAdvancedMode={true} 
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
                      <div class="grid grid-cols-1 sm:grid-cols-2 gap-4 px-3 py-3 break-words">
                        {#each ["Product Information", "Quantity Information"] as sectionTitle, index}
                          <div class="bg-gray-50 p-3 rounded-md border border-gray-200">
                            <div class="flex items-center mb-2 border-b border-gray-200 pb-1">
                              <h4 class="text-sm font-medium">{sectionTitle}</h4>
                              {#if index === 1}
                                <div class="relative inline-block group ml-2">
                                  <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                                    <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                      <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                                    </svg>
                                  </button>
                                  <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                                              group-hover:scale-100 w-[280px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                                              ring-1 ring-black ring-opacity-5 p-4">
                                    <p class="text-sm text-gray-500">
                                      Below is an example of a reported quantity for this product from the SCMD. Where it is possible to calculate, the equivalent quantity using alternative measures is shown. See the <a href="/faq/#data-contents" class="underline font-semibold" target="_blank">FAQs</a> for more details.
                                    </p>
                                  </div>
                                </div>
                              {/if}
                            </div>
                            <div class="space-y-1">
                              {#if index === 0}
                                <!-- Product Information Section -->
                                <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                                  <span class="text-sm font-medium whitespace-nowrap">VMP code:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">{product.vmp_code}</span>
                                </div>
                                
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
                                  <span class="text-sm font-medium whitespace-nowrap">Ingredients:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">
                                    {#if product.example_ingredients && product.example_ingredients.length > 0}
                                      {product.example_ingredients.map(ingredient => ingredient.name).join(', ')}
                                    {:else}
                                      <span class="text-gray-400">-</span>
                                    {/if}
                                  </span>
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
                                  <span class="text-sm font-medium whitespace-nowrap">DDD:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">
                                    {#if product.ddd_info}
                                      {product.ddd_info}
                                    {:else if product.example_ddd && product.example_ddd.ddd && product.example_ddd.unit_type}
                                      {product.example_ddd.ddd} {product.example_ddd.unit_type}
                                    {:else}
                                      <span class="text-gray-400">-</span>
                                    {/if}
                                  </span>
                                </div>
                                
                                <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                                  <span class="text-sm font-medium whitespace-nowrap">DDD route:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">
                                    {#if product.who_routes && product.who_routes.length > 0}
                                      {product.who_routes.join(', ')}
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
                              {:else}
                                <!-- Quantity Information Section -->
                                <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                                  <span class="text-sm font-medium whitespace-nowrap">SCMD quantity:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">{formatQuantity(product.example_quantity)}</span>
                                </div>
                                
                                <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                                  <span class="text-sm font-medium whitespace-nowrap">Unit dose quantity:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">{formatQuantity(product.example_dose)}</span>
                                </div>
                                
                                <div class="grid grid-cols-[auto_1fr] gap-3 items-baseline">
                                  <span class="text-sm font-medium whitespace-nowrap">DDD quantity:</span>
                                  <span class="text-sm break-words hyphens-auto min-w-0">
                                    {#if product.example_ddd && product.example_ddd.quantity}
                                      {product.example_ddd.quantity}
                                    {:else}
                                      <span class="text-gray-400">-</span>
                                    {/if}
                                  </span>
                                </div>
                                
                                {#if product.example_ingredients && product.example_ingredients.length > 0}
                                  <div class="pt-1 mt-1 border-t border-gray-200">
                                    <span class="text-sm font-medium block mb-1.5">Ingredient quantities:</span>
                                    {#each product.example_ingredients as ingredient}
                                      <div class="grid grid-cols-[1fr_auto] gap-3 items-baseline pl-2 mb-1">
                                        <span class="text-sm font-medium break-words hyphens-auto">{ingredient.name}:</span>
                                        <span class="text-sm whitespace-nowrap min-w-[80px]">{formatIngredientQuantity(ingredient)}</span>
                                      </div>
                                    {/each}
                                  </div>
                                {/if}
                              {/if}
                            </div>
                          </div>
                        {/each}
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