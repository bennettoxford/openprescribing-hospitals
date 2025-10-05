<svelte:options customElement={{
    tag: 'submission-history',
    shadow: 'none',
    props: {
        orgdata: { type: 'String' },
        latestDates: { type: 'String' },
        regions: { type: 'String' },
        regionsHierarchy: { type: 'String' }
    }
  }} />

<script>
    import { onMount } from 'svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';
    import OrgSubmissionChart from './OrgSubmissionChart.svelte';
    import OrgSubmissionChartLazy from './OrgSubmissionChartLazy.svelte';

    export let orgData = '{}';
    export let latestDates = '{}';
    export let regions = '[]';
    export let regionsHierarchy = '[]';

    let parsedOrgData = [];
    let parsedLatestDates = {};
    let parsedRegions = [];
    let parsedRegionsHierarchy = [];
    let months = [];
    let error = null;
    let sortType = 'missing_latest';
    let filteredOrganisations = [];
    let searchableOrgs = [];
    let showScrollButton = false;
    let selectedRegions = new Set();
    let selectedICBs = new Set();
    let isFilterOpen = false;
    let expandedRegions = new Set();

    function unescapeUnicode(str) {
        return str.replace(/\\u([a-fA-F0-9]{4})/g, function(match, grp) {
            return String.fromCharCode(parseInt(grp, 16));
        });
    }

    function calculateMissingMonths(org) {
        return Object.values(org.data).reduce((count, monthData) => {
            return count + (monthData.has_submitted ? 0 : 1);
        }, 0);
    }

    function calculateMissingDataProportion(org) {
        const submittedMonths = Object.values(org.data)
            .filter(monthData => monthData.has_submitted)
            .map(monthData => monthData.vmp_count || 0);
        
        if (submittedMonths.length === 0) return 0;
        
        const sorted = [...submittedMonths].sort((a, b) => a - b);
        const median = sorted[Math.floor(sorted.length / 2)];
        const deviations = submittedMonths.map(count => Math.abs(count - median));
        return deviations.reduce((sum, dev) => sum + dev, 0) / deviations.length;
    }

    function prepareOrganisationsForSearch(orgs) {
        let allOrgs = [];
        function collectOrgs(org) {
            allOrgs.push(org.name);
            if (org.predecessors) {
                org.predecessors.forEach(pred => collectOrgs(pred));
            }
        }
        orgs.forEach(org => collectOrgs(org));
        return [...new Set(allOrgs)];
    }

    function handleSearchSelect(event) {
        const { selectedItems, source } = event.detail;
        
        if (source === 'clearAll') {
            organisationSearchStore.updateSelection([]);
            filteredOrganisations = [];
        } else {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : [];
            organisationSearchStore.updateSelection(itemsArray);
        }
    }

    function handleOrganisationDropdownToggle(event) {
        isOrganisationDropdownOpen = event.detail.isOpen;
    }

    function handleScroll() {
        showScrollButton = window.scrollY > 500;
    }

    function scrollToTop() {
        const divider = document.querySelector('#chart-divider');
        if (divider) {
            divider.scrollIntoView({ behavior: 'smooth' });
        }
    }

    function toggleFilter() {
        isFilterOpen = !isFilterOpen;

        if (isFilterOpen && selectedICBs.size > 0) {
            parsedRegionsHierarchy.forEach(region => {
                        const hasSelectedICBs = region.icbs.some(icb => selectedICBs.has(icb.name));
                if (hasSelectedICBs) {
                    expandedRegions.add(region.region);
                }
            });
            expandedRegions = expandedRegions;
        }
    }

    function handleClickOutside(event) {
        const filterContainer = document.querySelector('.filter-container');
        if (isFilterOpen && filterContainer && !filterContainer.contains(event.target)) {
            isFilterOpen = false;
            expandedRegions.clear();
            expandedRegions = expandedRegions;
        }
    }

    function toggleRegionExpansion(region) {
        if (expandedRegions.has(region)) {
            expandedRegions.delete(region);
        } else {
            expandedRegions.add(region);
        }
        expandedRegions = expandedRegions;
    }

    onMount(() => {
        try {
            const unescapedData = unescapeUnicode(orgData);
            const parsedData = JSON.parse(unescapedData);

            parsedOrgData = parsedData.organisations;          
            parsedLatestDates = JSON.parse(unescapeUnicode(latestDates));
            parsedRegions = JSON.parse(unescapeUnicode(regions));
            parsedRegionsHierarchy = JSON.parse(unescapeUnicode(regionsHierarchy));
            
            const allMonths = new Set();
            function collectMonths(org) {
                if (org.data) {
                    Object.keys(org.data).forEach(month => allMonths.add(month));
                }
                if (org.predecessors) {
                    org.predecessors.forEach(pred => collectMonths(pred));
                }
            }
            parsedOrgData.forEach(org => collectMonths(org));
            
            months = Array.from(allMonths).sort();
            
            if (parsedOrgData.length > 0) {
                searchableOrgs = prepareOrganisationsForSearch(parsedOrgData);
                
                organisationSearchStore.setOrganisationData({
                    orgs: Object.fromEntries(searchableOrgs.map(name => [name, name])),
                    org_codes: parsedData.org_codes || {},
                    predecessor_map: parsedData.predecessor_map || buildPredecessorMap(parsedOrgData)
                });
                organisationSearchStore.updateSelection(searchableOrgs);
            }
            
            filteredOrganisations = parsedOrgData;
        } catch (e) {
            error = `Error parsing JSON data: ${e.message}`;
            console.error(error);
        }
        
        window.addEventListener('scroll', handleScroll);
        document.addEventListener('click', handleClickOutside);
        return () => {
            window.removeEventListener('scroll', handleScroll);
            document.removeEventListener('click', handleClickOutside);
        };
    });
   
    $: {
        if (parsedOrgData.length > 0) {
            let orgs = parsedOrgData;
            
            const hasRegionFilters = selectedRegions.size > 0 || selectedICBs.size > 0;
            const hasSearchFilters = $organisationSearchStore.selectedItems.length > 0;
            
            if (!hasSearchFilters) {
                filteredOrganisations = [];
            } else {
                if (hasRegionFilters || hasSearchFilters) {
                    orgs = orgs.filter(org => {
                        const passesRegionFilter = !hasRegionFilters || 
                            selectedRegions.has(org.region) || 
                            selectedICBs.has(org.icb);
                            
                        const passesSearchFilter = !hasSearchFilters || 
                            $organisationSearchStore.selectedItems.includes(org.name) ||
                            org.predecessors?.some(pred => 
                                $organisationSearchStore.selectedItems.includes(pred.name)
                            );
                            
                        return passesRegionFilter && passesSearchFilter;
                    });
                }

                filteredOrganisations = [...orgs].sort((a, b) => {
                    switch (sortType) {
                        case 'missing_months':
                            const aMissing = calculateMissingMonths(a);
                            const bMissing = calculateMissingMonths(b);
                            if (aMissing !== bMissing) {
                                return bMissing - aMissing;
                            }
                            break;
                            
                        case 'missing_latest':
                            const latestMonth = months[months.length - 1];
                            const aSubmitted = a.data[latestMonth]?.has_submitted;
                            const bSubmitted = b.data[latestMonth]?.has_submitted;
                            if (aSubmitted !== bSubmitted) {
                                return aSubmitted ? 1 : -1;
                            }
                            break;
                            
                        case 'missing_proportion':
                            const aProportion = calculateMissingDataProportion(a);
                            const bProportion = calculateMissingDataProportion(b);
                            if (aProportion !== bProportion) {
                                return bProportion - aProportion;
                            }
                            break;
                    }
                    return a.name.localeCompare(b.name);
                });
            }
        }
    }

    function buildPredecessorMap(orgData) {
        const predecessorMap = new Map();
        orgData.forEach(org => {
            if (org.predecessors && org.predecessors.length > 0) {
                predecessorMap.set(org.name, org.predecessors.map(p => p.name));
            }
        });
        return predecessorMap;
    }
</script>

<div class="flex flex-col w-full">
    <div class="w-full">
        <div class="filter-container relative mb-4">
            <div class="flex flex-col">
                <div class="flex flex-col gap-2">
                    <div class="flex justify-between items-center">
                        <label class="text-sm font-medium text-gray-700">
                            Filter by Region/ICB
                        </label>
                        <button 
                            class="text-red-600 hover:text-red-800 font-medium text-sm"
                            on:click={() => {
                                if (selectedRegions.size > 0 || selectedICBs.size > 0) {
                                    selectedRegions.clear();
                                    selectedICBs.clear();
                                    selectedRegions = selectedRegions;
                                    selectedICBs = selectedICBs;
                                    
                                    organisationSearchStore.setAvailableItems(searchableOrgs);
                                    organisationSearchStore.updateSelection(searchableOrgs);
                                    filteredOrganisations = parsedOrgData;
                                }
                            }}
                        >
                            Clear Filters
                        </button>
                    </div>
                    <div class="flex">
                        <button
                            on:click={toggleFilter}
                            class="flex-grow p-2 text-left border border-gray-300 rounded-l-md
                                   bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 
                                   focus:ring-black transition-all duration-200 relative
                                   {isFilterOpen ? 'rounded-bl-none z-[997]' : ''}"
                        >
                            <span class="text-gray-600">Select regions and ICBs...</span>
                        </button>

                        <div class="flex items-center gap-2 bg-gray-50 px-3 border border-l-0 border-gray-300 
                                    rounded-r-md {isFilterOpen ? 'rounded-br-none' : ''} min-w-[120px]">
                            <div class="flex flex-col items-center text-xs text-gray-500 py-1 w-full">
                                <span class="font-medium">
                                    {(() => {
                                        const totalICBs = parsedRegionsHierarchy.reduce((total, region) => total + region.icbs.length, 0);
                                        const selectedRegionICBs = Array.from(selectedRegions).reduce((count, regionName) => {
                                            const regionData = parsedRegionsHierarchy.find(r => r.region === regionName);
                                            return count + (regionData?.icbs.length || 0);
                                        }, 0);
                                        return `${selectedRegionICBs + selectedICBs.size}/${totalICBs}`;
                                    })()}
                                </span>
                                <span>ICBs</span>
                            </div>
                        </div>
                    </div>
                </div>

                {#if isFilterOpen}
                    <div class="absolute top-[calc(100%_-_1px)] left-0 right-0 bg-white border border-gray-300 
                                rounded-md rounded-t-none shadow-lg z-[996] flex flex-col max-h-72">
                        <div class="overflow-y-auto divide-y divide-gray-200">
                            {#each parsedRegionsHierarchy as region}
                                <div class="transition duration-150 ease-in-out divide-y divide-gray-200">
                                    <div class="flex items-center justify-between cursor-pointer p-2
                                                {selectedRegions.has(region.region) ? 'bg-oxford-100 text-oxford-500 hover:bg-oxford-200' : 'hover:bg-gray-100'}"
                                         on:click={(e) => {
                                             if (!e.target.closest('.expand-button')) {
                                                 if (selectedRegions.has(region.region)) {
                                                     selectedRegions.delete(region.region);
                                                 } else {
                                                     selectedRegions.add(region.region);
                                                     region.icbs.forEach(icb => selectedICBs.delete(icb.name));
                                                 }
                                                 selectedRegions = selectedRegions;

                                                 let availableOrgs = [];
                                                 selectedRegions.forEach(region => {
                                                     const orgsInRegion = parsedOrgData
                                                         .filter(org => org.region === region)
                                                         .map(org => org.name);
                                                     availableOrgs.push(...orgsInRegion);
                                                 });
                                                 
                                                 selectedICBs.forEach(icbName => {
                                                     const orgsInICB = parsedOrgData
                                                         .filter(org => org.icb === icbName)
                                                         .map(org => org.name);
                                                     availableOrgs.push(...orgsInICB);
                                                 });
                                                 
                                                 if (selectedRegions.size === 0 && selectedICBs.size === 0) {
                                                     availableOrgs = searchableOrgs;
                                                 }
                                                 
                                                 const uniqueAvailableOrgs = [...new Set(availableOrgs)];
                                                 organisationSearchStore.setAvailableItems(uniqueAvailableOrgs);
                                                 organisationSearchStore.updateSelection(uniqueAvailableOrgs);
                                             }
                                         }}>
                                        <div class="flex items-center gap-2 w-full">
                                            <button
                                                class="expand-button p-1 hover:bg-gray-200 rounded-full transition-colors"
                                                on:click|stopPropagation={() => toggleRegionExpansion(region.region)}
                                                aria-label={expandedRegions.has(region.region) ? "Collapse region" : "Expand region"}
                                            >
                                                <svg 
                                                    class="w-4 h-4 transform transition-transform duration-200 {expandedRegions.has(region.region) ? 'rotate-90' : ''}"
                                                    fill="none" 
                                                    stroke="currentColor" 
                                                    viewBox="0 0 24 24"
                                                >
                                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                                                </svg>
                                            </button>
                                                    <span>{region.region} ({region.region_code})</span>
                                            <span class="text-sm text-gray-500 ml-auto">
                                                ({(() => {
                                                    if (selectedRegions.has(region.region)) {
                                                        return `${region.icbs.length}/${region.icbs.length}`;
                                                    }
                                                    const selectedCount = region.icbs.filter(icb => selectedICBs.has(icb.name)).length;
                                                    return `${selectedCount}/${region.icbs.length}`;
                                                })()} ICBs)
                                            </span>
                                        </div>
                                        {#if selectedRegions.has(region.region)}
                                            <span class="ml-2 text-sm font-medium">Selected</span>
                                        {/if}
                                    </div>
                                    
                                    {#if expandedRegions.has(region.region)}
                                        {#each region.icbs as icb}
                                            <div class="pl-6 transition duration-150 ease-in-out relative p-2
                                                      {selectedRegions.has(region.region) ? 'text-gray-400 cursor-not-allowed' : 
                                                       selectedICBs.has(icb.name) ? 'bg-oxford-100 text-oxford-500 hover:bg-oxford-200' : 'cursor-pointer hover:bg-gray-100'}"
                                                 on:click={() => {
                                                     if (!selectedRegions.has(region.region)) {
                                                         if (selectedICBs.has(icb.name)) {
                                                             selectedICBs.delete(icb.name);
                                                         } else {
                                                             selectedICBs.add(icb.name);
                                                         }
                                                         selectedICBs = selectedICBs;
                                                         
                                                         let availableOrgs = [];
                                                         selectedRegions.forEach(region => {
                                                             const orgsInRegion = parsedOrgData
                                                                 .filter(org => org.region === region)
                                                                 .map(org => org.name);
                                                             availableOrgs.push(...orgsInRegion);
                                                         });
                                                         
                                                         selectedICBs.forEach(icbName => {
                                                             const orgsInICB = parsedOrgData
                                                                 .filter(org => org.icb === icbName)
                                                                 .map(org => org.name);
                                                             availableOrgs.push(...orgsInICB);
                                                         });
                                                         
                                                         if (selectedRegions.size === 0 && selectedICBs.size === 0) {
                                                             availableOrgs = searchableOrgs;
                                                         }
                                                         
                                                         const uniqueAvailableOrgs = [...new Set(availableOrgs)];
                                                         organisationSearchStore.setAvailableItems(uniqueAvailableOrgs);
                                                         organisationSearchStore.updateSelection(uniqueAvailableOrgs);
                                                     }
                                                 }}>
                                                <div class="flex items-center justify-between">
                                                    <div class="flex items-center text-sm">
                                                        <span class="mr-2">↳</span>
                                                        <span>{icb.name} ({icb.code})</span>
                                                    </div>
                                                    {#if selectedICBs.has(icb.name) && !selectedRegions.has(region.region)}
                                                        <span class="ml-auto text-sm font-medium">Selected</span>
                                                    {/if}
                                                </div>
                                            </div>
                                        {/each}
                                    {/if}
                                </div>
                            {/each}
                        </div>
                        <div class="py-2 px-3 border-t border-gray-200 flex justify-end bg-gray-50">
                            <button
                                on:click={() => {
                                    isFilterOpen = false;
                                }}
                                class="inline-flex justify-center items-center px-3 py-1.5 bg-oxford-50 text-oxford-600 rounded-md hover:bg-oxford-100 transition-colors duration-200 font-medium text-sm border border-oxford-200"
                            >
                                Done
                            </button>
                        </div>
                    </div>
                {/if}
            </div>
        </div>

        <div class="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-4 lg:gap-12 mb-4">
            <div class="w-full lg:max-w-[600px] relative z-50">
                {#if searchableOrgs.length > 0}
                    <OrganisationSearch 
                        source={organisationSearchStore}
                        overlayMode={true}
                        on:selectionChange={handleSearchSelect}
                        on:dropdownToggle={handleOrganisationDropdownToggle}
                    />
                {:else}
                    <div class="text-sm text-gray-600">Loading organisations...</div>
                {/if}
            </div>

            <div class="flex flex-col lg:flex-row gap-4">
                <select 
                    bind:value={sortType}
                    class="w-full lg:w-auto inline-flex items-center justify-center px-4 py-2 text-gray-600
                           bg-white border border-gray-300 rounded-md shadow-sm hover:bg-gray-50 
                           focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 
                           transition-all duration-200 h-[38px]"
                >
                    <option value="missing_latest">Sort by submission status in latest month</option>
                    <option value="missing_months">Sort by number of months with no data submission</option>
                    <option value="missing_proportion">Sort by variation in the number of unique products issued</option>
                    <option value="alphabetical">Sort alphabetically</option>
                </select>
            </div>
        </div>
    </div>

    {#if error}
        <p class="text-red-600">{error}</p>
    {:else if filteredOrganisations.length === 0}
        <div class="bg-white rounded-lg shadow-sm p-8 py-16 text-center">
            <div class="mx-auto flex items-center justify-center w-12 h-12 rounded-full bg-gray-100 mb-4">
                <svg class="w-6 h-6 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                </svg>
            </div>
            <h3 class="text-lg font-medium text-gray-900 mb-2">No organisations selected</h3>
            <p class="text-gray-600 mb-4">Use the search box above to select NHS Trusts and view their submission history.</p>
        </div>
    {:else}

        {#each filteredOrganisations as org (org.name)}
            <OrgSubmissionChartLazy>
                <OrgSubmissionChart 
                    {org}
                    latestDates={parsedLatestDates}
                    {months}
                />
            </OrgSubmissionChartLazy>
        {/each}
    {/if}

    {#if showScrollButton}
        <button
            on:click={scrollToTop}
            class="fixed bottom-8 right-8 bg-oxford-50 text-oxford-600 hover:bg-oxford-100 rounded-full p-3 shadow-lg transition-all duration-200 z-50"
            aria-label="Scroll to top"
        >
            <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 10l7-7m0 0l7 7m-7-7v18" />
            </svg>
        </button>
    {/if}
</div>

<style>
    :global(.tooltip-row) {
        line-height: 1.1;
        margin-bottom: 1px;
    }
    
    select option {
        font-family: system-ui, -apple-system, sans-serif;
    }
    
    select option[value^="region::"] {
        font-weight: 600;
        color: #1a1a1a;
        background-color: #f3f4f6;
    }
    
    select option[value^="icb::"] {
        color: #4b5563;
        padding-left: 1.5rem;
        border-left: 2px solid transparent;
    }
    
    select option:hover {
        background-color: #e5e7eb;
    }

    .filter-container {
        z-index: 997;
    }
</style>