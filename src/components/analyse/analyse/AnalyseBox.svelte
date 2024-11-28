<svelte:options customElement={{
    tag: 'analyse-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import '../../../styles/styles.css';
    import Search from '../../common/Search.svelte';
    import OrganisationSearch from '../../common/OrganisationSearch.svelte';
    import { createEventDispatcher } from 'svelte';
    import RangeSlider from 'svelte-range-slider-pips';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    import { analyseOptions } from '../../../stores/analyseOptionsStore';
    import { getCookie } from '../../../utils/utils';
    
    const dispatch = createEventDispatcher();

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isOrganisationDropdownOpen = false;

    $: selectedVMPs = $analyseOptions.selectedVMPs;
    $: quantityType = $analyseOptions.quantityType;
    $: searchType = $analyseOptions.searchType;

    export let minDate = null;
    export let maxDate = null;
    export let odsData = null;
    
    let dateValues = [null, null];
    let dates = [];
    let formatOptions = { year: 'numeric', month: 'short' };

    function formatDate(dateStr) {
        if (!dateStr) return '';
        return new Date(dateStr).toLocaleDateString('en-GB', formatOptions);
    }

    $: if ($analyseOptions && $analyseOptions.dateRange) {
        if (dates.length > 0) {
            const startIndex = dates.indexOf($analyseOptions.dateRange.startDate);
            const endIndex = dates.indexOf($analyseOptions.dateRange.endDate);
            if (startIndex !== -1 && endIndex !== -1) {
                dateValues = [startIndex, endIndex];
            }
        }
    }

    function handleDateRangeChange(event) {
        const values = event.detail.values;
        
        if (!Array.isArray(values) || values.length !== 2) {
            console.error('Unexpected values format:', values);
            return;
        }

        const [startIndex, endIndex] = values;

        if (typeof startIndex !== 'number' || typeof endIndex !== 'number') {
            console.error('Invalid indices:', { startIndex, endIndex });
            return;
        }

        analyseOptions.update(store => ({
            ...store,
            dateRange: {
                startDate: dates[startIndex],
                endDate: dates[endIndex]
            }
        }));

        dateValues = [startIndex, endIndex];
    }

    onMount(async () => {
        try {
            if (minDate && maxDate) {
                const start = new Date(minDate);
                const end = new Date(maxDate);
                const dateArray = [];
                let current = new Date(start);

                while (current <= end) {
                    dateArray.push(current.toISOString().split('T')[0]);
                    current.setMonth(current.getMonth() + 1);
                }
                dates = dateArray;

                
                analyseOptions.update(store => ({
                    ...store,
                    minDate: dates[0],
                    maxDate: dates[dates.length - 1],
                    dateRange: {
                        startDate: dates[0],
                        endDate: dates[dates.length - 1]
                    }
                }));

                dateValues = [0, dates.length - 1];
            }
            
            if (odsData) {
                try {
                    const parsedData = typeof odsData === 'string' ? JSON.parse(odsData) : odsData;
                    organisationSearchStore.setItems(parsedData);
                } catch (error) {
                    console.error('Error parsing ODS data:', error);
                }
            }
        } catch (error) {
            console.error('Error in onMount:', error);
        }
    });

    const csrftoken = getCookie('csrftoken');
    const quantityOptions = ['--', 'VMP Quantity', 'Dose', 'Ingredient Quantity'];

    async function runAnalysis() {
        if (isAnalysisRunning) return;

        console.log("Run Analysis button clicked");
        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = isAdvancedMode 
                ? "Please select at least one product (VMP, Ingredient, VTM, or ATC)."
                : "Please select at least one product.";
            return;
        }

        if (!isAdvancedMode) {
            quantityType = "VMP Quantity";
        } else if (quantityType === '--') {
            errorMessage = "Please select a quantity type before running the analysis.";
            return;
        }

        isAnalysisRunning = true;
        dispatch('analysisStart');
        
        let endpoint = '/api/filtered-quantities/';
        
        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrftoken
                },
                body: JSON.stringify({
                    quantity_type: quantityType,
                    names: selectedVMPs,
                    ods_names: $organisationSearchStore.selectedItems,
                    search_type: searchType,
                    start_date: $analyseOptions.dateRange.startDate,
                    end_date: $analyseOptions.dateRange.endDate
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            
            dispatch('analysisComplete', { 
                data: Array.isArray(data) ? data : [data], 
                quantityType: quantityType, 
                searchType: searchType 
            });
        } catch (error) {
            console.error("Error fetching filtered data:", error);
            errorMessage = "An error occurred while fetching data. Please try again.";
            dispatch('analysisError', { error: errorMessage });
        } finally {
            isAnalysisRunning = false;
        }
    }

    function dispatchAnalysisRunningChange(running) {
        const analyseBox = document.querySelector('analyse-box');
        if (analyseBox) {
            analyseBox.dispatchEvent(new CustomEvent('analysisRunningChange', { detail: running }));
        }
    }

    function handleVMPSelection(event) {
        analyseOptions.update(options => ({
            ...options,
            selectedVMPs: event.detail.items,
            searchType: event.detail.type
        }));
        console.log("Selected Items:", $analyseOptions.selectedVMPs, "Search Type:", $analyseOptions.searchType);
    }

    function handleODSSelection(event) {
        const { selectedItems, usedOrganisationSelection } = event.detail;
        organisationSearchStore.updateSelection(selectedItems, usedOrganisationSelection);
    }

    function handleQuantityTypeChange(event) {
        analyseOptions.update(options => ({
            ...options,
            quantityType: event.target.value
        }));
        console.log('Quantity type changed:', $analyseOptions.quantityType);
    }

    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
    }

    function handleClearAnalysis() {
        clearAnalysisOptions();
        errorMessage = '';
        dateValues = [0, dates.length - 1];
        dispatch('analysisClear');
    }

    onMount(async () => {
    try {
        if (minDate && maxDate) {
            const start = new Date(minDate);
            const end = new Date(maxDate);
            const dateArray = [];
            let current = new Date(start);

            while (current <= end) {
                dateArray.push(current.toISOString().split('T')[0]);
                current.setMonth(current.getMonth() + 1);
            }
            dates = dateArray;
            
            analyseOptions.update(store => ({
                ...store,
                minDate: dates[0],
                maxDate: dates[dates.length - 1],
                dateRange: {
                    startDate: dates[0],
                    endDate: dates[dates.length - 1]
                }
            }));

            dateValues = [0, dates.length - 1];
        }
    } catch (error) {
        console.error('Error initializing date range:', error);
    }
});

    export let isAdvancedMode = false;

    function toggleAdvancedMode() {
        isAdvancedMode = !isAdvancedMode;
        dispatch('advancedModeChange', isAdvancedMode);
    }
</script>

<div class="p-4 sm:p-6 bg-white rounded-lg w-full">
  <div class="grid gap-8">
    <!-- Header -->
    <div>
      <h2 class="text-xl sm:text-2xl font-bold text-oxford mb-2">Analysis builder</h2>
      <p class="text-sm text-oxford">
        {#if isAdvancedMode}
          Run a custom analysis of hospitals stock control data using the options below. You can analyse 
          specific medicines or groups of medicines across different NHS Trusts.
        {:else}
          Search for specific medicines to analyse their stock control data across NHS Trusts.
        {/if}
      </p>
    </div>

    <!-- Selection Grid -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Product Selection -->
      <div class="grid gap-4 lg:self-start">
        <div class="flex items-center">
          <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Product selection</h3>
        </div>
        <div class="relative">
          <Search on:selectionChange={handleVMPSelection} {isAdvancedMode} />
        </div>
      </div>

      <!-- Trust Selection -->
      <div class="grid gap-4 lg:self-start">
        <div class="flex items-center">
          <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Trusts selection</h3>
        </div>
        <div class="relative">
          <OrganisationSearch 
            source={organisationSearchStore}
            overlayMode={false}
            on:selectionChange={handleODSSelection}
            on:dropdownToggle={handleOrganisationDropdownToggle}
          />
        </div>
        
        <!-- Trust Selection Box -->
        {#if $organisationSearchStore.selectedItems.length > 0}
          <div class="w-full">
            <h3 class="font-semibold my-2 text-md text-gray-700">
              Selected {$organisationSearchStore.filterType === 'icb' ? 'ICBs' : 'Trusts'}:
            </h3>
            <ul class="border border-gray-200 rounded-md">
              {#each $organisationSearchStore.selectedItems as item}
                <li class="flex items-center justify-between px-2 py-1">
                  <span class="text-gray-800">{item}</span>
                  <button 
                    on:click={() => {
                      const newSelection = $organisationSearchStore.selectedItems.filter(i => i !== item);
                      organisationSearchStore.updateSelection(newSelection);
                      handleODSSelection({ detail: { selectedItems: newSelection } });
                    }}
                    class="px-2 py-1 text-sm text-white bg-red-600 hover:bg-red-700 rounded-md"
                  >
                    Remove
                  </button>
                </li>
              {/each}
            </ul>
          </div>
        {/if}
      </div>
    </div>

    <!-- Second Selection Grid (for Quantity Type and Date Range) -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {#if isAdvancedMode}
        <!-- Quantity Type Selection (Only in Advanced Mode) -->
        <div class="grid gap-4 lg:self-start">
          <div class="flex items-center">
            <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Quantity type selection</h3>
          </div>
          <div class="relative">
            <select 
              id="quantityType"
              bind:value={$analyseOptions.quantityType}
              on:change={handleQuantityTypeChange}
              class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
            >
              {#each quantityOptions as option}
                <option value={option}>{option}</option>
              {/each}
            </select>
          </div>
        </div>

        <!-- Date Range (Moved inside the same grid) -->
        <div class="grid gap-4">
          <div>
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Date Range</h3>
            </div>
            {#if dates.length > 0}
              <div class="px-2">
                <div class="flex justify-between mb-2 text-sm text-gray-600">
                  <span>{formatDate(dates[0])}</span>
                  <span>{formatDate(dates[dates.length - 1])}</span>
                </div>
                <RangeSlider
                  min={0}
                  max={dates.length - 1}
                  step={1}
                  values={dateValues}
                  on:change={handleDateRangeChange}
                  float
                  all="hide"
                  first="pip"
                  last="pip"
                  pipstep={6}
                  formatter={index => formatDate(dates[index])}
                  handleFormatter={index => formatDate(dates[index])}
                  springValues={{ stiffness: 0.3, damping: 0.8 }}
                />
                <div class="mt-2 text-center text-sm text-gray-600">
                  Selected range: {formatDate(dates[dateValues[0]])} - {formatDate(dates[dateValues[1]])}
                </div>
              </div>
            {/if}
          </div>
        </div>
      {:else}
        <!-- Date Range (When not in advanced mode, takes single column) -->
        <div class="grid gap-4">
          <div>
            <div class="flex items-center">
              <h3 class="text-base sm:text-lg font-semibold text-oxford mr-2">Date Range</h3>
            </div>
            {#if dates.length > 0}
              <div class="px-2">
                <div class="flex justify-between mb-2 text-sm text-gray-600">
                  <span>{formatDate(dates[0])}</span>
                  <span>{formatDate(dates[dates.length - 1])}</span>
                </div>
                <RangeSlider
                  min={0}
                  max={dates.length - 1}
                  step={1}
                  values={dateValues}
                  on:change={handleDateRangeChange}
                  float
                  all="hide"
                  first="pip"
                  last="pip"
                  pipstep={6}
                  formatter={index => formatDate(dates[index])}
                  handleFormatter={index => formatDate(dates[index])}
                  springValues={{ stiffness: 0.3, damping: 0.8 }}
                />
                <div class="mt-2 text-center text-sm text-gray-600">
                  Selected range: {formatDate(dates[dateValues[0]])} - {formatDate(dates[dateValues[1]])}
                </div>
              </div>
            {/if}
          </div>
        </div>
      {/if}
    </div>

    <!-- Analysis Controls - Full width but more compact -->
    <div class="mt-8 bg-gray-50 rounded-lg p-4 sm:p-6">
      <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <!-- Mode Switch -->
        <button 
          class="text-sm text-oxford-600 hover:text-oxford-800 flex items-center gap-2"
          on:click={toggleAdvancedMode}
        >
          <span class="underline">Switch to {isAdvancedMode ? 'basic' : 'advanced'} mode</span>
        </button>

        <!-- Error Message -->
        {#if errorMessage}
          <div class="p-3 bg-red-100 border border-red-400 text-red-700 rounded flex-grow">
            {errorMessage}
          </div>
        {/if}

        <!-- Action Buttons -->
        <div class="flex gap-2 sm:min-w-[300px] justify-end">
          <button
            on:click={handleClearAnalysis}
            class="px-4 sm:px-6 py-2 sm:py-2.5 bg-white text-gray-700 font-medium rounded-md hover:bg-gray-100 transition-colors duration-200 border border-gray-200"
          >
            Clear Analysis
          </button>
          <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="px-4 sm:px-6 py-2 sm:py-2.5 bg-oxford-500 text-white font-medium rounded-md hover:bg-oxford-600 transition-colors duration-200
                 disabled:bg-oxford-300 disabled:cursor-not-allowed"
          >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
          </button>
        </div>
      </div>
    </div>
  </div>
</div>

<style>

    :root {
        --range-slider:            hsl(220, 13%, 91%);
        --range-handle-inactive:   hsl(212.1, 100%, 50.6%);
        --range-handle:            hsl(212.1, 100%, 50.6%);
        --range-handle-focus:      hsl(212, 99.2%, 50.4%);
        --range-handle-border:     hsl(212.1, 100%, 50.6%);
        --range-range-inactive:    hsl(212.1, 100%, 50.6%);
        --range-range:             hsl(212.1, 100%, 50.6%);
        --range-float-inactive:    hsl(212.2, 97.6%, 49.4%);
        --range-float:             hsl(212, 99.2%, 50.4%);
        --range-float-text:        hsl(220, 13%, 91%);
        --range-pip:               hsl(220, 13%, 91%);
        --range-pip-text:          hsl(220, 13%, 91%);
        --range-pip-active:        hsl(220, 13%, 91%);
        --range-pip-active-text:   hsl(220, 13%, 91%);
        --range-pip-hover:         hsl(220, 13%, 91%);
        --range-pip-hover-text:    hsl(220, 13%, 91%);
        --range-pip-in-range:      hsl(220, 13%, 91%);
        --range-pip-in-range-text: hsl(220, 13%, 91%);
    }
</style>
