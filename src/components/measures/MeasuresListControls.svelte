<svelte:options customElement={{
    tag: 'measures-list-controls',
    props: {
        orgData: { type: 'String', reflect: true },
        regionData: { type: 'String', reflect: true },
        chartDataJson: { type: 'String', reflect: true },
        selectedMode: { type: 'String', reflect: true },
        selectedCode: { type: 'String', reflect: true },
        selectedSort: { type: 'String', reflect: true },
        tagsData: { type: 'String', reflect: true },
        selectedTags: { type: 'String', reflect: true }
    },
    shadow: 'none'
}} />

<script>
    import { onMount, onDestroy } from 'svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import ModeSelector from '../common/ModeSelector.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore.js';
    import { modeSelectorStore } from '../../stores/modeSelectorStore.js';
    import {
        mode, selectedCode as selectedCodeStore, sort, selectedTags as selectedTagsStore,
        setMode, setSelectedCode, setSort, setSelectedTags,
        setChartData, setLoadingCharts
    } from '../../stores/measuresListStore.js';
    import { regionColors } from '../../utils/chartConfig.js';
    import { setUrlParams, formatArrayParam } from '../../utils/utils.js';


    export let orgData = '{}';
    export let regionData = '[]';
    export let chartDataJson = '{}';
    export let selectedMode = 'trust';
    export let selectedCode = '';
    export let selectedSort = 'name';
    export let tagsData = '[]';
    export let selectedTags = '';

    let parsedOrgData = {};
    let parsedRegionData = [];
    let parsedChartData = {};
    let parsedTags = [];
    let tagDropdownOpen = false;
    let tagDropdownEl;
    let isInitialLoad = true;
    let showShareToast = false;

    const modeOptions = [
        { value: 'national', label: 'National' },
        { value: 'region', label: 'Region' },
        { value: 'trust', label: 'NHS Trust' },
    ];

    const baseSortOptions = [
        { value: 'name', label: 'Sort: Alphabetical' },
        { value: 'newest', label: 'Sort: Newest first' },
    ];
    const trustSortOptions = [
        { value: 'potential_improvement', label: 'Sort: Potential for improvement' },
        { value: 'most_improved', label: 'Sort: Most improved' },
    ];
    const TRUST_ONLY_SORT_VALUES = ['potential_improvement', 'most_improved'];
    $: selectedItems = $organisationSearchStore?.selectedItems || [];
    $: singleTrustCode = $mode === 'trust' && selectedItems.length === 1
        ? findPrimaryTrustCode(selectedItems)
        : null;
    $: effectiveTrustCode = $selectedCodeStore || singleTrustCode || '';
    // Show all sorts; trust-only sorts are disabled when not in trust mode or no trust selected
    $: trustSortsEnabled = $mode === 'trust' && !!effectiveTrustCode;
    $: disabledSortTooltip = $mode === 'trust'
        ? 'Select a trust to use this sort'
        : 'Only available in NHS Trust mode';
    $: sortOptions = [
        ...baseSortOptions.map(opt => ({ ...opt, disabled: false, title: '' })),
        ...trustSortOptions.map(opt => ({
            ...opt,
            disabled: !trustSortsEnabled,
            title: trustSortsEnabled ? '' : disabledSortTooltip,
        })),
    ];

    function findPrimaryTrustCode(items) {
        const predecessorMap = parsedOrgData.predecessor_map || {};
        const allPredecessors = new Set(Object.values(predecessorMap).flat());
        const primaryName = items.find(name => !allPredecessors.has(name));
        return primaryName ? (parsedOrgData.org_codes || {})[primaryName] : null;
    }

    const LEGEND_REGIONS = Object.keys(regionColors);


    function syncUrl() {
        const params = {};
        if ($mode && $mode !== 'trust') params.mode = $mode;
        if ($mode === 'trust' && $selectedCodeStore) params.trust = $selectedCodeStore;
        if ($mode === 'region' && $selectedCodeStore) params.region = $selectedCodeStore;
        if ($sort) params.sort = $sort;
        if ($selectedTagsStore.length > 0) params.tags = formatArrayParam($selectedTagsStore);
        setUrlParams(params, ['mode', 'trust', 'region', 'sort', 'tags']);
    }

    function handleSortChange(event) {
        const newSort = event.target.value;
        if (newSort === $sort) return;
        setSort(newSort);
        syncUrl();
        if (TRUST_ONLY_SORT_VALUES.includes(newSort) && $mode === 'trust' && $selectedCodeStore) {
            fetchTrustOverlayAndMerge($selectedCodeStore);
        } else if (!TRUST_ONLY_SORT_VALUES.includes(newSort)) {
            if (!($mode === 'trust' && $selectedCodeStore)) {
                const regionNameForChart = $mode === 'region' && $selectedCodeStore
                    ? (parsedRegionData.find(r => r.code === $selectedCodeStore)?.name || null)
                    : null;
                deriveChartDataFromPrefetch($mode, regionNameForChart);
            }
        }
    }

    function toggleTag(slug) {
        const next = $selectedTagsStore.includes(slug)
            ? $selectedTagsStore.filter(s => s !== slug)
            : [...$selectedTagsStore, slug];
        setSelectedTags(next);
        syncUrl();
    }

    function clearAllTags() {
        setSelectedTags([]);
        syncUrl();
    }

    function handleClickOutsideTagDropdown(event) {
        if (tagDropdownEl && !tagDropdownEl.contains(event.target)) {
            tagDropdownOpen = false;
        }
    }

    function updateOrganisationStore(mode) {
        if (mode === 'trust') {
            organisationSearchStore.setOrganisationData(parsedOrgData);
            organisationSearchStore.setFilterType('trust');
            organisationSearchStore.setAvailableItems(Object.values(parsedOrgData.orgs || []));
        } else if (mode === 'region') {
            const regionOrgs = {};
            const regionCodes = {};
            parsedRegionData.forEach(r => {
                regionOrgs[r.code] = r.name;
                regionCodes[r.name] = r.code;
            });
            organisationSearchStore.setOrganisationData({
                orgs: regionOrgs, org_codes: regionCodes, predecessor_map: {}
            });
            organisationSearchStore.setFilterType('region');
            organisationSearchStore.setAvailableItems(parsedRegionData.map(r => r.name));
        }
        organisationSearchStore.updateSelection([]);
    }

    function hasValidChartData(data) {
        if (!data || typeof data !== 'object') return false;
        const keys = Object.keys(data);
        if (keys.length === 0) return false;
        if (data.data && Array.isArray(data.data) && data.data.length > 0) return true;
        if (data.regions && Object.keys(data.regions).length > 0) return true;
        if (data.percentiles && Object.keys(data.percentiles).length > 0) return true;
        return false;
    }

    function pickBestDataForSlug(slug, national, region, trust, preferred) {
        const preferredData = preferred === 'national' ? national?.[slug] : preferred === 'region' ? region?.[slug] : trust?.[slug];
        if (hasValidChartData(preferredData)) return preferredData;
        if (hasValidChartData(national?.[slug])) return national[slug];
        if (hasValidChartData(region?.[slug])) return region[slug];
        if (hasValidChartData(trust?.[slug])) return trust[slug];
        return preferredData ?? null;
    }

    function deriveChartDataFromPrefetch(modeVal, code = null) {
        if (!parsedChartData || !parsedChartData.national) return;
        const { national, region, trust_percentiles, modes_by_slug } = parsedChartData;
        const nat = national || {};
        const reg = region || {};
        const trust = trust_percentiles || {};
        const allSlugs = new Set([...Object.keys(nat), ...Object.keys(reg), ...Object.keys(trust)]);
        let chartBySlug = {};
        let modesBySlug = {};

        if (modeVal === 'national') {
            for (const s of allSlugs) {
                chartBySlug[s] = pickBestDataForSlug(s, nat, reg, trust, 'national');
                modesBySlug[s] = 'national';
            }
        } else if (modeVal === 'region') {
            const regionName = code || null;
            for (const s of allSlugs) {
                let d = pickBestDataForSlug(s, nat, reg, trust, 'region');
                if (d && regionName && d.regions) {
                    d = { ...d, highlightRegion: regionName };
                }
                chartBySlug[s] = d;
                modesBySlug[s] = 'region';
            }
        } else if (modeVal === 'trust') {
            for (const s of allSlugs) {
                chartBySlug[s] = pickBestDataForSlug(s, nat, reg, trust, 'trust');
                modesBySlug[s] = 'trust';
            }
        }
        setChartData(chartBySlug, modesBySlug);
    }

    async function fetchTrustOverlayAndMerge(code) {
        setLoadingCharts(true);
        try {
            const apiUrl = new URL('/api/measures-data/', window.location.origin);
            apiUrl.searchParams.set('trust', code);
            if (window.location.pathname.includes('/preview/')) apiUrl.searchParams.set('preview', 'true');

            const response = await fetch(apiUrl.toString());
            if (!response.ok) throw new Error(`Chart data API returned ${response.status}`);
            const data = await response.json();
            const { trust_overlay } = data || {};

            const { trust_percentiles = {}, modes_by_slug = {} } = parsedChartData || {};
            const allSlugs = new Set(Object.keys(trust_percentiles || {}));
            const chartBySlug = {};
            const modesBySlug = {};
            for (const slug of allSlugs) {
                const base = trust_percentiles[slug] || {};
                const overlay = trust_overlay?.[slug];

                chartBySlug[slug] = {
                    ...base,
                    ...(overlay?.trustData ? { trustData: overlay.trustData } : {}),
                };
                modesBySlug[slug] = 'trust';
            }
            setChartData(chartBySlug, modesBySlug);
        } catch (error) {
            console.error('Failed to fetch trust overlay:', error);
            syncUrl();
            window.location.href = window.location.href;
        } finally {
            setLoadingCharts(false);
        }
    }

    function handleModeChange(newMode) {
        if (isInitialLoad) return;

        if (newMode !== 'trust' && TRUST_ONLY_SORT_VALUES.includes($sort)) {
            setSort('name');
        }
        setMode(newMode);
        setSelectedCode('');
        modeSelectorStore.setSelectedMode(newMode);
        updateOrganisationStore(newMode);
        syncUrl();
        deriveChartDataFromPrefetch(newMode);
    }

    function handleSelectionChange(event) {
        if (isInitialLoad) return;

        const selectedItems = event.detail?.selectedItems || [];

        if (selectedItems.length === 0) {
            if (TRUST_ONLY_SORT_VALUES.includes($sort)) setSort('name');
            setSelectedCode('');
            syncUrl();
            deriveChartDataFromPrefetch($mode);
            return;
        }

        let code = null;
        let regionNameForChart = null;
        if ($mode === 'trust') {
            code = findPrimaryTrustCode(selectedItems);
        } else if ($mode === 'region') {
            const name = selectedItems[0];
            code = parsedRegionData.find(r => r.name === name)?.code || name;
            regionNameForChart = name;
        }

        if (code) {
            setSelectedCode(code);
            syncUrl();
            if ($mode === 'trust') {
                fetchTrustOverlayAndMerge(code);
            } else {
                deriveChartDataFromPrefetch($mode, regionNameForChart);
            }
        }
    }

    async function handleShare() {
        try {
            await navigator.clipboard.writeText(window.location.href);
            showShareToast = true;
            setTimeout(() => { showShareToast = false; }, 2000);
        } catch (error) {
            console.error('Failed to copy URL:', error);
        }
    }


    onMount(() => {
        try {
            parsedOrgData = JSON.parse(orgData);
            parsedRegionData = JSON.parse(regionData);
            parsedChartData = JSON.parse(chartDataJson || '{}');
            parsedTags = JSON.parse(tagsData || '[]');

            const params = new URLSearchParams(window.location.search);
            const urlMode = params.get('mode');
            const urlTrust = params.get('trust') || '';
            const urlRegion = params.get('region') || '';
            const urlSort = params.get('sort') || selectedSort || 'name';
            const urlTags = (params.get('tags') || selectedTags || '').split(',').map(s => s.trim()).filter(Boolean);

            const rawMode = urlMode || selectedMode || 'trust';
            const initialMode = ['national', 'region', 'trust'].includes(rawMode) ? rawMode : 'trust';
            const initialCode = initialMode === 'trust' ? (urlTrust || selectedCode || '')
                : initialMode === 'region' ? (urlRegion || selectedCode || '') : '';

            const initialRegionName = initialMode === 'region' && initialCode
                ? (parsedRegionData.find(r => r.code === initialCode)?.name || initialCode)
                : null;

            setMode(initialMode);
            setSelectedCode(initialCode);
            setSort(urlSort);
            setSelectedTags(urlTags);

            modeSelectorStore.setSelectedMode(initialMode);
            updateOrganisationStore(initialMode);

            if (initialMode === 'trust' && initialCode) {
                fetchTrustOverlayAndMerge(initialCode);
            } else {
                deriveChartDataFromPrefetch(initialMode, initialMode === 'region' ? initialRegionName : initialCode);
            }

            if (initialCode && initialMode !== 'national') {
                if (initialMode === 'trust') {
                    const name = parsedOrgData.orgs?.[initialCode];
                    if (name) organisationSearchStore.updateSelection(organisationSearchStore.getRelatedOrgs(name));
                } else if (initialMode === 'region' && initialRegionName) {
                    organisationSearchStore.updateSelection([initialRegionName]);
                }
            }

            setTimeout(() => { isInitialLoad = false; }, 100);

            document.addEventListener('click', handleClickOutsideTagDropdown);
        } catch (error) {
            console.error('MeasuresListControls: failed to initialise', error);
        }
    });

    onDestroy(() => {
        document.removeEventListener('click', handleClickOutsideTagDropdown);
    });
</script>

<div class="flex flex-col gap-2">
    <div class="flex justify-end">
        <button
            on:click={handleShare}
            class="measures-list-share-btn flex items-center justify-center gap-2 px-4 py-2 text-sm font-medium text-oxford-600 bg-white border border-oxford-200 rounded-md hover:bg-oxford-50 hover:border-oxford-300 transition-colors duration-200 h-[38px]"
            title="Copy link to share this measures list with current selection"
        >
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-4 h-4">
                <path stroke-linecap="round" stroke-linejoin="round" d="M7.217 10.907a2.25 2.25 0 100 2.186m0-2.186c.18.324.283.696.283 1.093s-.103.77-.283 1.093m0-2.186l9.566-5.314m-9.566 7.5l9.566 5.314m0 0a2.25 2.25 0 103.935 2.186 2.25 2.25 0 00-3.935-2.186zm0-12.814a2.25 2.25 0 103.935-2.186 2.25 2.25 0 00-3.935-2.186z" />
            </svg>
            Share
        </button>
    </div>

    <div class="flex flex-col xl:flex-row xl:items-end xl:justify-between gap-4 xl:gap-12">
        <div class="order-last xl:order-none w-full xl:max-w-[520px] xl:shrink-0 relative z-30">
            <OrganisationSearch
                source={organisationSearchStore}
                overlayMode={true}
                maxItems={1}
                hideSelectAll={true}
                showTitle={true}
                disabled={$mode === 'national'}
                on:selectionChange={handleSelectionChange}
            />
        </div>

        <div class="measures-list-controls-right order-first xl:order-none flex flex-col xl:flex-row items-stretch xl:items-end gap-4 w-full xl:w-auto">
            <div class="flex flex-col gap-2 w-full xl:w-auto xl:min-w-0 order-2 xl:order-1">
                <select
                    id="sort-select"
                    class="measures-list-sort-select dropdown-select dropdown-arrow w-full min-w-0 xl:w-[12rem] text-sm p-2 border border-gray-300 rounded-md bg-white h-[38px] xl:truncate"
                    aria-label="Sort list"
                    on:change={handleSortChange}
                    value={$sort}
                >
                    {#each sortOptions as option}
                        <option value={option.value} disabled={option.disabled} title={option.title}>{option.label}</option>
                    {/each}
                </select>
            </div>

            {#if parsedTags.length > 0}
            <div class="relative w-full xl:w-auto xl:min-w-0 order-3 xl:order-2" bind:this={tagDropdownEl}>
                <button
                    type="button"
                    class="measures-list-tag-select dropdown-arrow w-full min-w-0 xl:w-auto xl:min-w-[10.5rem] text-sm p-2 border border-gray-300 rounded-md bg-white h-[38px] text-left"
                    aria-label="Filter by tag"
                    aria-expanded={tagDropdownOpen}
                    aria-haspopup="true"
                    on:click={() => (tagDropdownOpen = !tagDropdownOpen)}
                >
                    <span class="truncate">
                        {$selectedTagsStore.length === 0
                            ? 'Filter by tag'
                            : `${$selectedTagsStore.length} tag${$selectedTagsStore.length === 1 ? '' : 's'} selected`}
                    </span>
                </button>
                {#if tagDropdownOpen}
                <div class="absolute left-0 right-0 xl:right-auto xl:min-w-[200px] mt-1 py-2 bg-white border border-gray-200 rounded-md shadow-lg z-40 max-h-64 overflow-y-auto"
                     role="group" aria-label="Tag filters">
                    {#if $selectedTagsStore.length > 0}
                        <button
                            type="button"
                            class="w-full text-left px-3 py-1.5 text-xs font-medium text-oxford-600 hover:bg-gray-50 border-b border-gray-100 mb-1"
                            on:click={clearAllTags}
                        >
                            Clear all
                        </button>
                    {/if}
                    {#each parsedTags as tag}
                        <label class="flex items-center gap-2 px-3 py-2 hover:bg-gray-50 cursor-pointer text-sm text-gray-700">
                            <input
                                type="checkbox"
                                class="rounded border-gray-300 text-oxford-600 focus:ring-oxford-500"
                                checked={$selectedTagsStore.includes(tag.slug)}
                                on:change={() => toggleTag(tag.slug)}
                            />
                            <span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium"
                                  style="background-color: {(tag.colour || '#6b7280') + '20'}; color: {tag.colour || '#6b7280'};">
                                <span class="w-1.5 h-1.5 rounded-full" style="background-color: {tag.colour || '#6b7280'};"></span>
                                {tag.name}
                            </span>
                        </label>
                    {/each}
                </div>
                {/if}
            </div>
            {/if}

            <div class="flex flex-col gap-2 w-full xl:w-auto xl:min-w-0 order-first xl:order-last">
                <ModeSelector
                    options={modeOptions}
                    initialMode={$mode}
                    label="View Mode"
                    onChange={handleModeChange}
                    variant="dropdown"
                />
            </div>
        </div>
    </div>

    {#if $mode === 'trust' && ($sort === 'potential_improvement' || $sort === 'most_improved')}
        <div class="text-sm text-gray-600 pt-2 border-t border-gray-100">
            {#if $sort === 'potential_improvement'}
                Sorting by potential for improvement (over the last 12 months). <a href="/faq/#how-is-potential-for-improvement-and-most-improved-determined" target="_blank" rel="noopener noreferrer" class="text-oxford-600 hover:text-oxford-800 underline">See the FAQs for more detail on how this is calculated</a>.
            {:else}
                Sorting by most improved (over the last 12 months). <a href="/faq/#how-is-potential-for-improvement-and-most-improved-determined" target="_blank" rel="noopener noreferrer" class="text-oxford-600 hover:text-oxford-800 underline">See the FAQs for more detail on how this is calculated</a>.
            {/if}
        </div>
    {/if}

    {#if $mode === 'region'}
        <div class="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-gray-600 pt-2 border-t border-gray-100">
            <span class="font-medium text-gray-700 mr-1">Key:</span>
            {#each LEGEND_REGIONS as name}
                <span class="inline-flex items-center gap-1.5">
                    <span class="inline-block w-4 h-0.5 rounded" style="background-color: {regionColors[name]};"></span>
                    {name}
                </span>
            {/each}
        </div>
    {:else if $mode === 'trust'}
        <div class="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-gray-600 pt-2 border-t border-gray-100">
            <span class="font-medium text-gray-700 mr-1">Key:</span>
            <span class="inline-flex items-center gap-1.5"><span class="inline-block w-4 h-0.5 rounded" style="background-color: #005AB5;"></span> Percentile range</span>
            <span class="inline-flex items-center gap-1.5"><span class="inline-block w-4 h-0.5 rounded" style="background-color: #DC3220;"></span> Median</span>
            {#if $organisationSearchStore.selectedItems?.length > 0}
                <span class="inline-flex items-center gap-1.5"><span class="inline-block w-4 h-0.5 rounded" style="background-color: #D97706;"></span> {$organisationSearchStore.selectedItems[0]}</span>
            {/if}
        </div>
    {/if}
</div>

<div class="fixed bottom-4 right-4 bg-oxford-50 text-oxford-800 px-4 py-2 rounded-lg shadow-lg border border-oxford-100 transform transition-all duration-300 z-50"
     class:translate-y-0={showShareToast} class:opacity-100={showShareToast}
     class:translate-y-full={!showShareToast} class:opacity-0={!showShareToast}>
    <div class="flex items-center gap-2">
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
            <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </svg>
        <span class="text-sm font-medium">Link copied to clipboard!</span>
    </div>
</div>
