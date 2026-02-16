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
    import LazyLoad from '../common/LazyLoad.svelte';
    import RegionIcbFilter from '../common/RegionIcbFilter.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';
    import {
        getOrgsFromRegionIcbFilter,
        prepareOrganisationsForSearch,
        updateRegionSelection,
        updateIcbSelection,
    } from '../../utils/regionIcbFilterUtils.js';
    import OrgSubmissionChart from './OrgSubmissionChart.svelte';

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

    function handleScroll() {
        showScrollButton = window.scrollY > 500;
    }

    function scrollToTop() {
        const divider = document.querySelector('#chart-divider');
        if (divider) {
            divider.scrollIntoView({ behavior: 'smooth' });
        }
    }

    function getAvailableItemsFromFilters() {
        return getOrgsFromRegionIcbFilter(
            parsedOrgData,
            (org) => org.region,
            (org) => org.icb,
            (org) => org.name,
            selectedRegions,
            selectedICBs,
            searchableOrgs
        );
    }

    function handleRegionClick(region) {
        const { selectedRegions: nextRegions, selectedICBs: nextICBs } = updateRegionSelection(
            region,
            selectedRegions,
            selectedICBs
        );
        selectedRegions = nextRegions;
        selectedICBs = nextICBs;
        const available = getAvailableItemsFromFilters();
        organisationSearchStore.setAvailableItems(available);
        organisationSearchStore.updateSelection(available);
    }

    function handleIcbClick(region, icb) {
        const updated = updateIcbSelection(region, icb, selectedRegions, selectedICBs);
        if (!updated) return;
        selectedRegions = updated.selectedRegions;
        selectedICBs = updated.selectedICBs;
        const available = getAvailableItemsFromFilters();
        organisationSearchStore.setAvailableItems(available);
        organisationSearchStore.updateSelection(available);
    }

    function handleFilterClear() {
        if (selectedRegions.size > 0 || selectedICBs.size > 0) {
            selectedRegions = new Set();
            selectedICBs = new Set();
            organisationSearchStore.setAvailableItems(searchableOrgs);
            organisationSearchStore.updateSelection(searchableOrgs);
        }
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
        return () => window.removeEventListener('scroll', handleScroll);
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
        <RegionIcbFilter
            regionsHierarchy={parsedRegionsHierarchy}
            selectedRegions={selectedRegions}
            selectedICBs={selectedICBs}
            onRegionClick={handleRegionClick}
            onIcbClick={handleIcbClick}
            onClear={handleFilterClear}
        />

        <div class="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-4 lg:gap-12 mb-4">
            <div class="w-full lg:max-w-[600px] relative z-50">
                {#if searchableOrgs.length > 0}
                    <OrganisationSearch 
                        source={organisationSearchStore}
                        overlayMode={true}
                        on:selectionChange={handleSearchSelect}
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
            <LazyLoad>
                <OrgSubmissionChart 
                    {org}
                    latestDates={parsedLatestDates}
                    {months}
                />
                <div slot="placeholder" class="bg-white rounded-lg shadow-sm p-4 mb-6 h-[200px] flex items-center justify-center">
                    <div class="flex space-x-4 w-full">
                        <div class="flex-1 space-y-4">
                            <div class="h-4 bg-gray-200 rounded w-1/4"></div>
                            <div class="h-[150px] bg-gray-200 rounded"></div>
                        </div>
                    </div>
                </div>
            </LazyLoad>
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
</style>