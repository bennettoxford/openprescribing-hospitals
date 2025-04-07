<svelte:options customElement={{
    tag: 'product-details',
    shadow: 'none',
    props: {
        products: { type: 'String', attribute: 'products' },
        currentPage: { type: 'String', attribute: 'current-page' },
        totalPages: { type: 'String', attribute: 'total-pages' },
        search: { type: 'String', attribute: 'search' }
    }
}} />

<script>
  export let products;
  export let currentPage;
  export let totalPages;
  export let search = '';

  $: numCurrentPage = Number(currentPage) || 1;
  $: numTotalPages = Number(totalPages) || 1;
  
  $: parsedProducts = typeof products === 'string' ? JSON.parse(products) : products;

  let searchTerm = search;
  let searchTimeout;
  
  function handleSearch(event) {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
      const newSearchTerm = event.target.value;
      window.location.href = `?search=${encodeURIComponent(newSearchTerm)}`;
    }, 300);
  }

  function formatQuantity(quantityObj) {
    if (!quantityObj) return 'N/A';
    
    const quantity = quantityObj.ddd ? Number(quantityObj.quantity).toFixed(2) : quantityObj.quantity;
    let result = `${quantity} ${quantityObj.unit}`;
    
    if (quantityObj.ddd && !quantityObj.unit.toLowerCase().includes('ddd')) {
      result += ` (DDD: ${quantityObj.ddd})`;
    }
    return result;
  }

  function goToPage(page) {
    window.location.href = `?page=${page}`;
  }
</script>

<div class="container mx-auto px-4 py-8">
  <h1 class="text-2xl font-bold mb-4">Product Details</h1>
  
  <div class="mb-8 text-gray-600">
    <p class="mb-2">
      This page shows the quantity values available for each product (VMP).
    </p>
    <p>
      For each product, a sample VMP quantity is picked, and where available, the corresponding ingredient quantity and DDD quantity is shown.
    </p>
  </div>

  <div class="mb-6">
    <div class="relative max-w-xl">
      <input
        type="text"
        placeholder="Search by product name or code..."
        value={searchTerm}
        on:input={handleSearch}
        class="w-full p-2 pr-8 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
      />
      {#if searchTerm}
        <button
          class="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
          on:click={() => {
            searchTerm = '';
            window.location.href = '?';
          }}
        >
          ✕
        </button>
      {/if}
    </div>
  </div>

  <div class="overflow-x-auto">
    <table class="min-w-full bg-white border border-gray-200">
      <thead>
        <tr class="bg-gray-50">
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[300px] min-w-[200px]">VMP</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[300px] min-w-[200px]">VTM</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[150px] min-w-[120px]">Routes</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[150px] min-w-[120px]">WHO Routes</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[150px] min-w-[120px]">VMP Quantity</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[150px] min-w-[120px]">Dose</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[150px] min-w-[120px]">Ingredient Quantity</th>
          <th class="px-6 py-3 border-b text-left text-xs font-medium text-gray-500 uppercase tracking-wider max-w-[150px] min-w-[120px]">DDD Quantity</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-gray-200">
        {#each parsedProducts as product}
          <tr class="hover:bg-gray-50">
            <td class="px-6 py-4 max-w-[300px] min-w-[200px]">
              <div class="text-sm break-words">
                <a 
                  href={`https://openprescribing.net/dmd/vmp/${product.vmp_code}/`}
                  target="_blank"
                  rel="noopener noreferrer"
                  class="text-oxford-600 hover:text-oxford-800 hover:underline"
                >
                  {product.vmp_name}
                </a>
              </div>
              <div class="text-xs text-gray-500 break-words">
                {product.vmp_code}
              </div>
            </td>
            <td class="px-6 py-4 max-w-[300px] min-w-[200px]">
              <div class="text-sm break-words">{product.vtm_name || 'N/A'}</div>
              <div class="text-xs text-gray-500 break-words">{product.vtm_code || 'N/A'}</div>
            </td>
            <td class="px-6 py-4 text-sm max-w-[150px] min-w-[120px] break-words">
              {product.routes?.join(', ') || 'N/A'}
            </td>
            <td class="px-6 py-4 text-sm max-w-[150px] min-w-[120px] break-words">
              {product.who_routes?.join(', ') || 'N/A'}
            </td>
            <td class="px-6 py-4 text-sm max-w-[150px] min-w-[120px] break-words">{formatQuantity(product.example_quantity)}</td>
            <td class="px-6 py-4 text-sm max-w-[150px] min-w-[120px] break-words">{formatQuantity(product.example_dose)}</td>
            <td class="px-6 py-4 max-w-[150px] min-w-[120px]">
              {#if product.example_ingredients?.length}
                {#each product.example_ingredients as ingredient}
                  <div class="mb-2 last:mb-0">
                    <div class="text-sm break-words">
                      {ingredient.quantity} {ingredient.unit}
                    </div>
                    <div class="text-xs text-gray-500 break-words">
                      {ingredient.name}
                    </div>
                  </div>
                {/each}
              {:else}
                <div class="text-sm">N/A</div>
              {/if}
            </td>
            <td class="px-6 py-4 text-sm max-w-[150px] min-w-[120px] break-words">{formatQuantity(product.example_ddd)}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>

  <div class="mt-4 flex justify-between items-center">
    <button 
      class="px-4 py-2 border rounded {numCurrentPage === 1 ? 'bg-gray-100 cursor-not-allowed' : 'hover:bg-gray-50'}"
      on:click={() => goToPage(numCurrentPage - 1)}
      disabled={numCurrentPage === 1}
    >
      Previous
    </button>
    
    <div class="flex items-center gap-4">
      <span class="text-sm text-gray-600">
        Page
      </span>
      <div class="flex items-center gap-2">
        <input 
          type="number" 
          min="1" 
          max={numTotalPages}
          value={numCurrentPage}
          class="w-16 px-2 py-1 border rounded text-center"
          on:keydown={(e) => {
            if (e.key === 'Enter') {
              const newPage = Math.min(Math.max(1, Number(e.target.value)), numTotalPages);
              goToPage(newPage);
            }
          }}
          on:blur={(e) => {
            const newPage = Math.min(Math.max(1, Number(e.target.value)), numTotalPages);
            e.target.value = newPage;
          }}
        />
        <span class="text-sm text-gray-600">of {numTotalPages}</span>
      </div>
    </div>
    
    <button 
      class="px-4 py-2 border rounded {numCurrentPage === numTotalPages ? 'bg-gray-100 cursor-not-allowed' : 'hover:bg-gray-50'}"
      on:click={() => goToPage(numCurrentPage + 1)}
      disabled={numCurrentPage === numTotalPages}
    >
      Next
    </button>
  </div>
</div>