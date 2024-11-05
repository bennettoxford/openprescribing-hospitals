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
    import { getCookie } from '../../../utils/utils';
    import { analyseOptions, clearAnalysisOptions } from '../../../stores/analyseOptionsStore';
    import RangeSlider from 'svelte-range-slider-pips';
    import { organisationSearchStore } from '../../../stores/organisationSearchStore';
    const dispatch = createEventDispatcher();

    let isAnalysisRunning = false;
    let errorMessage = '';
    let isOrganisationDropdownOpen = false;
    let filteredData = [];

    $: selectedVMPs = $analyseOptions.selectedVMPs;
    $: quantityType = $analyseOptions.quantityType;
    $: searchType = $analyseOptions.searchType;
    $: odsNames = $analyseOptions.odsNames;

    export let minDate = null;
    export let maxDate = null;
    
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

        // Ensure we have valid indices
        if (typeof startIndex !== 'number' || typeof endIndex !== 'number') {
            console.error('Invalid indices:', { startIndex, endIndex });
            return;
        }

        // Update the store with the new dates
        analyseOptions.update(store => ({
            ...store,
            dateRange: {
                startDate: dates[startIndex],
                endDate: dates[endIndex]
            }
        }));

        // Update local values for the slider
        dateValues = [startIndex, endIndex];

    }

    onMount(async () => {
        try {
            if (minDate && maxDate) {
                // Generate array of all dates between min and max
                const start = new Date(minDate);
                const end = new Date(maxDate);
                const dateArray = [];
                let current = new Date(start);

                while (current <= end) {
                    dateArray.push(current.toISOString().split('T')[0]);
                    current.setMonth(current.getMonth() + 1);
                }
                dates = dateArray;
                
                // Initialize date range in store with first and last dates
                analyseOptions.update(store => ({
                    ...store,
                    dateRange: {
                        startDate: dates[0],
                        endDate: dates[dates.length - 1]
                    }
                }));

                // Set initial slider values
                dateValues = [0, dates.length - 1];
            }
        } catch (error) {
            console.error('Error initializing date range:', error);
        }
    });

    const csrftoken = getCookie('csrftoken');
    // Define quantityOptions
    const quantityOptions = ['--', 'Dose', 'Ingredient Quantity'];

    async function fetchVMPNames() {
        const response = await fetch('/api/unique-vmp-names/');
        vmpNames = await response.json();
    }

    async function fetchODSNames() {
        const response = await fetch('/api/unique-ods-names/');
        odsNames = await response.json();
    }

    async function runAnalysis() {
        if (isAnalysisRunning) return; // Prevent multiple clicks

        console.log("Run Analysis button clicked");
        errorMessage = '';

        if (!selectedVMPs || selectedVMPs.length === 0) {
            errorMessage = "Please select at least one product (VMP, Ingredient, or VTM).";
            return;
        }

        if (quantityType === '--') {
            errorMessage = "Please select a quantity type before running the analysis.";
            return;
        }

        if ($organisationSearchStore.selectedItems.length === 0 && 
            $organisationSearchStore.usedOrganisationSelection) {
            errorMessage = "You've selected to filter by organisations, but haven't chosen any. Please select at least one organisation or clear the organisation filter.";
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

            filteredData = await response.json();
            console.log("Filtered data:", filteredData);

            if (!Array.isArray(filteredData)) {
                filteredData = [filteredData];
            }

            dispatch('analysisComplete', { 
                data: filteredData, 
                quantityType: quantityType, 
                searchType: searchType 
            });
        } catch (error) {
            console.error("Error fetching filtered data:", error);
            filteredData = [];
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
        // Reset slider to min/max values
        dateValues = [0, dates.length - 1];
        dispatch('analysisClear');
    }

    onMount(async () => {
        try {
            const response = await fetch('/api/get-search-items/');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();

            if (minDate && maxDate) {
                // Generate array of all dates between min and max
                const start = new Date(minDate);
                const end = new Date(maxDate);
                const dateArray = [];
                let current = new Date(start);

                while (current <= end) {
                    dateArray.push(current.toISOString().split('T')[0]);
                    current.setMonth(current.getMonth() + 1);
                }
                dates = dateArray;
                
                // Update store with all data including min/max dates
                analyseOptions.update(store => ({
                    ...store,
                    ...data,
                    minDate: dates[0],
                    maxDate: dates[dates.length - 1],
                    dateRange: {
                        startDate: dates[0],
                        endDate: dates[dates.length - 1]
                    }
                }));

                if (data.odsNames) {
                    organisationSearchStore.setItems(data.odsNames);
                }

                // Set initial slider values
                dateValues = [0, dates.length - 1];
            }
        } catch (error) {
            console.error("Error fetching analysis options:", error);
            errorMessage = "An error occurred while fetching analysis options. Please try again.";
        }
    });
</script>

<div class="p-4 bg-white rounded-lg w-full h-full flex flex-col">
    <div class="flex flex-col mb-2">
        <p class="text-sm text-oxford">
            Run a custom analysis of hospitals stock control data using the options below. You can analyse 
            specific medicines or groups of medicines across different NHS Trusts.
        </p>
    </div>

    <div class="mb-2 flex-shrink-0">
        <div class="flex items-center mb-2">
            <h3 class="text-lg font-semibold text-oxford mr-2">Product selection</h3>
            <div class="relative inline-block group">
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                    <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                        <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                    </svg>
                </button>
                <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                            group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                            ring-1 ring-black ring-opacity-5 p-4">
                    <p class="text-sm text-gray-500">
                        Search for and select products to include in the analysis. You can select individual products
                        (VMP) or groups of products by ingredient, therapeutic moiety (VTM), or 
                        therapeutic target (ATC). See <a href="/faq/#product-selection-types" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                    </p>
                </div>
            </div>
        </div>
        <Search on:selectionChange={handleVMPSelection} />
    </div>
    
    <div class="mb-4 flex-shrink-0">
        <div class="flex items-center mb-2">
            <h3 class="text-lg font-semibold text-oxford mr-2">Select quantity type</h3>
            <div class="relative inline-block group">
                <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                    <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                        <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                    </svg>
                </button>
                <div class="absolute z-[200] scale-0 transition-all duration-100 origin-top transform 
                            group-hover:scale-100 w-[200px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                            ring-1 ring-black ring-opacity-5 p-4">
                    <p class="text-sm text-gray-500">
                        Select the quantity unit most relevant to the selected products to use for the analysis.
                        See <a href="/faq/#quantity-type" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                    </p>
                </div>
            </div>
        </div>
        <select 
            bind:value={$analyseOptions.quantityType}
            on:change={handleQuantityTypeChange}
            class="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-oxford-500"
        >
            {#each quantityOptions as option}
                <option value={option}>{option}</option>
            {/each}
        </select>
    </div>

    <div class="mb-4 flex-grow overflow-visible relative">
        <div class="flex flex-col mb-2">
            <div class="flex items-center mb-2">
                <h3 class="text-lg font-semibold text-oxford mr-2">Select Trusts</h3>
                <div class="relative inline-block group">
                    <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                        <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                            <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                        </svg>
                    </button>
                    <div class="absolute z-[200] scale-0 transition-all duration-100 origin-top transform 
                                group-hover:scale-100 w-[250px] -translate-x-1/2 left-1/2 top-8 mt-1 rounded-md shadow-lg bg-white 
                                ring-1 ring-black ring-opacity-5 p-4">
                        <p class="text-sm text-gray-500">
                            By default, the analysis will include all NHS Trusts in England. You can restrict the analysis by selecting specific trusts below.
                            See <a href="/faq/#trusts-included" class="underline font-semibold" target="_blank">the FAQs</a> for more details.
                        </p>
                    </div>
                </div>
            </div>
        </div>
        <div class="relative overflow-visible z-[100] {$organisationSearchStore.usedOrganisationSelection ? (isOrganisationDropdownOpen ? 'mb-[400px]' : 'mb-2') : 'mb-2'}">
            {#if $analyseOptions}
                <OrganisationSearch 
                    source={organisationSearchStore}
                    overlayMode={true}
                    on:selectionChange={handleODSSelection}
                    on:dropdownToggle={handleOrganisationDropdownToggle}
                />
            {/if}
        </div>
    </div>
    
    <div class="flex-shrink-0 relative z-[40]">
        <div class="flex items-center mb-2">
            <h3 class="text-lg font-semibold text-oxford mr-2">Date Range</h3>
        </div>

        {#if dates.length > 0}
            <div class="px-4 py-2">
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
    
    {#if errorMessage}
        <div class="mb-4 p-2 bg-red-100 border border-red-400 text-red-700 rounded flex-shrink-0 relative z-[30]">
            {errorMessage}
        </div>
    {/if}
    
    <div class="mt-auto flex-shrink-0 space-y-2 relative z-[20]">
        <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="w-full bg-oxford-500 text-white font-bold py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-oxford-500 transition-colors duration-200
               hover:bg-oxford-600
               disabled:bg-oxford-300 disabled:cursor-not-allowed"
        >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
        </button>
        <button
            on:click={handleClearAnalysis}
            class="w-full bg-gray-100 text-gray-700 font-bold py-2 px-4 rounded focus:outline-none focus:ring-2 focus:ring-gray-400 transition-colors duration-200
               hover:bg-gray-200"
        >
            Clear Analysis
        </button>
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
