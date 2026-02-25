<svelte:options customElement={{
    tag: 'measure-trusts-list',
    props: {
        orgData: { type: 'String', reflect: true },
        percentileDataJson: { type: 'String', reflect: true },
        regionsHierarchy: { type: 'String', reflect: true },
        measureItemBaseUrl: { type: 'String', reflect: true },
        measureHasDenominators: { type: 'String', reflect: true, attribute: 'measure-has-denominators' },
        measureQuantityType: { type: 'String', reflect: true },
        measureLowerIsBetter: { type: 'String', reflect: true },
        selectedSort: { type: 'String', reflect: true },
    },
    shadow: 'none'
}} />

<script>
    import { onMount } from 'svelte';
    import './MeasureMiniChart.svelte';
    import LazyLoad from '../common/LazyLoad.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import RegionIcbFilter from '../common/RegionIcbFilter.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore.js';
    import { deriveSortMetricsFromChartData } from '../../utils/measuresSortUtils.js';
    import { setUrlParams } from '../../utils/utils.js';
    import {
        getOrgsFromRegionIcbFilter,
        flattenOrganisationsWithMetadata,
        updateRegionSelection,
        updateIcbSelection,
    } from '../../utils/regionIcbFilterUtils.js';

    export let orgData = '{}';
    export let percentileDataJson = '[]';
    export let regionsHierarchy = '[]';
    export let measureItemBaseUrl = '';
    export let measureHasDenominators = 'false';
    export let measureQuantityType = '';
    export let measureLowerIsBetter = '';
    export let selectedSort = 'name';

    let sortType = selectedSort || 'name';
    $: if (selectedSort) sortType = selectedSort;
    let parsedOrgData = {};
    let parsedPercentileData = [];
    let parsedRegionsHierarchy = [];
    let showScrollButton = false;
    let selectedRegions = new Set();
    let selectedICBs = new Set();
    let hasInitializedStore = false;

    function handleScroll() {
        showScrollButton = window.scrollY > 500;
    }

    function scrollToTop() {
        const top = document.querySelector('#measure-trusts-list-top');
        if (top) {
            top.scrollIntoView({ behavior: 'smooth' });
        } else {
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    function handleSearchSelect(event) {
        const { selectedItems, source } = event.detail;
        if (source === 'clearAll') {
            organisationSearchStore.updateSelection([]);
        } else {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : [];
            organisationSearchStore.updateSelection(itemsArray);
        }
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

    function getAvailableItemsFromFilters() {
        return getOrgsFromRegionIcbFilter(
            availableOrgs,
            (item) => item.region,
            (item) => item.icb,
            (item) => item.name,
            selectedRegions,
            selectedICBs,
            allAvailableOrgsNames
        );
    }

    function handleFilterClear() {
        if (selectedRegions.size > 0 || selectedICBs.size > 0) {
            selectedRegions = new Set();
            selectedICBs = new Set();
            organisationSearchStore.setAvailableItems(allAvailableOrgsNames);
            organisationSearchStore.updateSelection(allAvailableOrgsNames);
        }
    }

    onMount(() => {
        window.addEventListener('scroll', handleScroll);
        return () => window.removeEventListener('scroll', handleScroll);
    });

    $: {
        try {
            parsedOrgData = JSON.parse(orgData || '{}');
        } catch {
            parsedOrgData = {};
        }
    }

    $: parsedOrganisations = parsedOrgData?.organisations || [];
    $: orgsWithData = flattenOrganisationsWithMetadata(parsedOrganisations);
    $: allPredecessors = new Set(
        Object.values(parsedOrgData?.predecessor_map || {}).flat()
    );
    $: hasTrustData = (o) => (o.data?.length ?? 0) > 0 && o.available !== false;
    $: searchableOrgs = [...new Set(
        orgsWithData
            .filter((o) => !allPredecessors.has(o.name) && hasTrustData(o))
            .map((o) => o.name)
    )];

    $: allOrgsForSearch = [...new Set(orgsWithData.map((o) => o.name))];
    $: availableOrgs = orgsWithData.filter(hasTrustData);
    $: allAvailableOrgsNames = [...new Set(availableOrgs.map((o) => o.name))];
    $: orgDataByTrust = Object.fromEntries(orgsWithData.map((o) => [o.name, o]));

    $: {
        try {
            parsedPercentileData = JSON.parse(percentileDataJson || '[]');
        } catch {
            parsedPercentileData = [];
        }
    }

    $: {
        try {
            parsedRegionsHierarchy = JSON.parse(regionsHierarchy || '[]');
        } catch {
            parsedRegionsHierarchy = [];
        }
    }

    $: if (searchableOrgs.length > 0 && Object.keys(parsedOrgData).length > 0) {
        selectedRegions;
        selectedICBs;
        const orgs = Object.fromEntries(
            allOrgsForSearch.map((name) => [parsedOrgData.org_codes?.[name] || name, name])
        );
        organisationSearchStore.setOrganisationData({
            orgs,
            org_codes: parsedOrgData.org_codes || {},
            predecessor_map: parsedOrgData.predecessor_map || {},
        });
        organisationSearchStore.setFilterType('trust');
        const availableFromFilters = getAvailableItemsFromFilters();
        organisationSearchStore.setAvailableItems(availableFromFilters);
        if (!hasInitializedStore) {
            hasInitializedStore = true;
            organisationSearchStore.updateSelection(availableFromFilters);
        }
    }

    function buildPercentiles() {
        const grouped = {};
        for (const p of parsedPercentileData) {
            const month = typeof p.month === 'string' ? p.month : p.month?.isoformat?.() ?? String(p.month);
            if (!grouped[p.percentile]) grouped[p.percentile] = [];
            grouped[p.percentile].push([month, p.quantity ?? 0]);
        }
        for (const k of Object.keys(grouped)) {
            grouped[k].sort((a, b) => new Date(a[0]) - new Date(b[0]));
        }
        return grouped;
    }

    function buildTrustData(trustName) {
        const trustInfo = orgDataByTrust[trustName];
        if (!trustInfo?.data?.length) return [];
        return trustInfo.data
            .map((d) => {
                const month = typeof d.month === 'string' ? d.month : d.month?.isoformat?.() ?? String(d.month);
                return [month, d.quantity ?? 0];
            })
            .sort((a, b) => new Date(a[0]) - new Date(b[0]));
    }

    function getChartDataForTrust(trustName, percentiles) {
        const trustData = buildTrustData(trustName);
        if (trustData.length === 0) return null;
        if (Object.keys(percentiles).length === 0) return null;
        return { percentiles, trustData };
    }

    $: chartDataByTrust = (() => {
        const percentiles = buildPercentiles();
        return Object.fromEntries(
            searchableOrgs
                .map((name) => {
                    const data = getChartDataForTrust(name, percentiles);
                    return data ? [name, data] : null;
                })
                .filter(Boolean)
        );
    })();

    $: lowerIsBetter =
        measureLowerIsBetter === 'true'
            ? true
            : measureLowerIsBetter === 'false'
              ? false
              : null;

    $: trustMeasuresForSort = searchableOrgs.map((name) => ({
        slug: name,
        lower_is_better: lowerIsBetter,
    }));

    $: sortMetricsByTrust = deriveSortMetricsFromChartData(
        chartDataByTrust,
        trustMeasuresForSort
    );

    function applyTrustSort(trustList, sort) {
        if (sort === 'potential_improvement' || sort === 'most_improved') {
            const key = sort === 'potential_improvement' ? 'potential_improvement' : 'most_improved';
            return [...trustList].sort((a, b) => {
                const va = sortMetricsByTrust[a]?.[key];
                const vb = sortMetricsByTrust[b]?.[key];
                const nameA = (a || '').toLowerCase();
                const nameB = (b || '').toLowerCase();
                const noA = va == null;
                const noB = vb == null;
                if (noA && noB) return nameA.localeCompare(nameB, undefined, { sensitivity: 'base' });
                if (noA) return 1;
                if (noB) return -1;
                const diff = (vb ?? 0) - (va ?? 0);
                return diff !== 0 ? diff : nameA.localeCompare(nameB, undefined, { sensitivity: 'base' });
            });
        }
        return [...trustList].sort((a, b) =>
            (a || '').localeCompare(b || '', undefined, { sensitivity: 'base' })
        );
    }

    function handleSortChange(event) {
        const newSort = event.target.value;
        setUrlParams({ sort: newSort }, ['sort']);
    }

    $: filteredTrusts = (() => {
        const selectedItems = $organisationSearchStore?.selectedItems || [];
        const filtered = searchableOrgs.filter(
            (name) =>
                selectedItems.length > 0 &&
                selectedItems.includes(name) &&
                chartDataByTrust[name]
        );
        return applyTrustSort(filtered, sortType);
    })();

    function trustDetailUrl(trustName) {
        const code = parsedOrgData?.org_codes?.[trustName];
        if (!code || !measureItemBaseUrl) return measureItemBaseUrl;
        return `${measureItemBaseUrl}?mode=trust&trusts=${encodeURIComponent(code)}`;
    }

</script>

<div id="measure-trusts-list-top" class="flex flex-col w-full">
    <div class="w-full mb-6">
        <RegionIcbFilter
            regionsHierarchy={parsedRegionsHierarchy}
            selectedRegions={selectedRegions}
            selectedICBs={selectedICBs}
            onRegionClick={handleRegionClick}
            onIcbClick={handleIcbClick}
            onClear={handleFilterClear}
        />

        <div class="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-4 lg:gap-12">
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
                    on:change={handleSortChange}
                    class="dropdown-select dropdown-arrow w-full min-w-0 lg:w-[12rem] text-sm p-2 border border-gray-300 rounded-md bg-white h-[38px] lg:truncate"
                    aria-label="Sort list"
                >
                    <option value="name">Sort: Alphabetical</option>
                    <option value="potential_improvement">Sort: Potential for improvement</option>
                    <option value="most_improved">Sort: Most improved</option>
                </select>
            </div>
        </div>

        {#if sortType === 'potential_improvement' || sortType === 'most_improved'}
            <div class="text-sm text-gray-600 pt-2 border-t border-gray-100">
                {#if sortType === 'potential_improvement'}
                    Sorting by potential for improvement (over the last 12 months). <a href="/faq/#how-is-potential-for-improvement-and-most-improved-determined" target="_blank" rel="noopener noreferrer" class="text-oxford-600 hover:text-oxford-800 underline">See the FAQs for more detail on how this is calculated</a>.
                {:else}
                    Sorting by most improved (over the last 12 months). <a href="/faq/#how-is-potential-for-improvement-and-most-improved-determined" target="_blank" rel="noopener noreferrer" class="text-oxford-600 hover:text-oxford-800 underline">See the FAQs for more detail on how this is calculated</a>.
                {/if}
            </div>
        {/if}

        <div class="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-gray-600 pt-2 border-t border-gray-100">
            <span class="font-medium text-gray-700 mr-1">Key:</span>
            {#if searchableOrgs.length >= 30}
                <span class="inline-flex items-center gap-1.5">Median <span class="inline-block w-4 h-0.5 rounded" style="background-color: #DC3220;"></span></span>
                <span class="inline-flex items-center gap-1.5 flex-wrap">
                    <span class="text-gray-600 mr-0.5">Percentiles:</span>
                    {#each [{ lo: 5, hi: 95, opacity: 0.1 }, { lo: 15, hi: 85, opacity: 0.2 }, { lo: 25, hi: 75, opacity: 0.4 }, { lo: 35, hi: 65, opacity: 0.6 }, { lo: 45, hi: 55, opacity: 0.8 }] as band}
                        <span class="inline-flex items-center gap-1 text-xs">
                            <span
                                class="inline-block w-3 h-3 rounded-sm shrink-0 border border-gray-200"
                                style="background-color: rgba(0,90,181,{band.opacity});"
                                title="{band.lo}th–{band.hi}th"
                            ></span>
                            <span class="text-gray-600 whitespace-nowrap">{band.lo}th–{band.hi}th</span>
                        </span>
                    {/each}
                </span>
            {/if}
            <span class="inline-flex items-center gap-1.5"><span class="inline-block w-4 h-0.5 rounded" style="background-color: #D97706;"></span> Trust</span>
        </div>
    </div>

    {#if filteredTrusts.length === 0}
        <div class="bg-white rounded-lg shadow-sm p-8 py-16 text-center">
            <div class="mx-auto flex items-center justify-center w-12 h-12 rounded-full bg-gray-100 mb-4">
                <svg class="w-6 h-6 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                </svg>
            </div>
            <h3 class="text-lg font-medium text-gray-900 mb-2">No trusts selected</h3>
            <p class="text-gray-600 mb-4">Use the search box above to select NHS Trusts, or filter by Region/ICB to view charts.</p>
        </div>
    {:else}
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {#each filteredTrusts as trustName}
                <LazyLoad>
                    <div class="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col">
                        <div class="p-4 border-b border-gray-100">
                            <h3 class="text-base font-semibold text-gray-900 line-clamp-2 leading-tight">
                                {trustName}
                            </h3>
                        </div>
                        <div class="p-2 flex-grow min-h-0 overflow-visible" style="height: 280px;">
                            <measure-mini-chart
                                chartdata={chartDataByTrust[trustName] ? JSON.stringify({ ...chartDataByTrust[trustName], trust_count: searchableOrgs.length }) : '{}'}
                                mode="trust"
                                ispercentage={measureHasDenominators}
                                quantitytype={measureQuantityType}
                                slug=""
                                min-trusts-for-percentiles={30}
                            />
                        </div>
                        <div class="p-6 pt-2 flex flex-col gap-2">
                            <a
                                href={trustDetailUrl(trustName)}
                                class="inline-flex w-full justify-center items-center px-4 py-2 bg-oxford-50 text-oxford-600 hover:bg-oxford-100 rounded-lg transition-colors duration-200 font-medium"
                            >
                                View measure details
                            </a>
                        </div>
                    </div>
                    <div slot="placeholder" class="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col">
                        <div class="p-4 border-b border-gray-100">
                            <div class="h-4 bg-gray-200 rounded w-3/4 animate-pulse"></div>
                        </div>
                        <div class="p-2 flex-grow min-h-0" style="height: 280px;">
                            <div class="w-full h-full bg-gray-200 rounded animate-pulse"></div>
                        </div>
                        <div class="p-6 pt-2">
                            <div class="h-10 bg-gray-200 rounded-lg animate-pulse"></div>
                        </div>
                    </div>
                </LazyLoad>
            {/each}
        </div>
    {/if}

    {#if showScrollButton}
        <button
            type="button"
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
