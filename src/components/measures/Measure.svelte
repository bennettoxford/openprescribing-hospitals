<svelte:options customElement={{
    tag: 'measure-component',
    props: {
        orgdata: { type: 'String', reflect: true },
        regiondata: { type: 'String', reflect: true },
        icbdata: { type: 'String', reflect: true },
        nationaldata: { type: 'String', reflect: true },
        percentiledata: { type: 'String', reflect: true },
        quantitytype: { type: 'String', reflect: true },
        hasdenominators: { type: 'String', reflect: true },
        denominatorvmps: { type: 'String', reflect: true },
        numeratorvmps: { type: 'String', reflect: true },
        annotations: { type: 'String', reflect: true },
        defaultviewmode: { type: 'String', reflect: true },
        userauthenticated: { type: 'String', reflect: true, attribute: 'user-authenticated' }
    },
    shadow: 'none'
}} />

<script>
    import { onMount, tick } from 'svelte';
    import { createChartStore } from '../../stores/chartStore.js';
    import {
        selectedMode,
        orgdata as orgdataStore,
        regiondata as regionStore,
        icbdata as icbStore,
        nationaldata as nationalStore,
        percentiledata as percentileStore,
        visibleRegions,
        visibleTrusts,
        visibleICBs,
        showPercentiles,
        updatePercentilesVisibility,
        getDatasetVisibility,
        filteredData,
    } from '../../stores/measureChartStore.js';
    import Chart from '../common/Chart.svelte';
    import OrganisationSearch from '../common/OrganisationSearch.svelte';
    import OrganisationSearchFiltered from '../common/OrganisationSearchFiltered.svelte';
    import ModeSelector from '../common/ModeSelector.svelte';
    import { organisationSearchStore } from '../../stores/organisationSearchStore';
    import { modeSelectorStore } from '../../stores/modeSelectorStore.js';
    import { formatNumber, getUrlParams, setUrlParams, parseArrayParam, formatArrayParam, getCurrentUrl, copyToClipboard } from '../../utils/utils.js';
    import { flattenOrganisationsToData } from '../../utils/regionIcbFilterUtils.js';
    import pluralize from 'pluralize';

    export let orgdata = '[]';
    export let regiondata = '[]';
    export let icbdata = '[]';
    export let nationaldata = '[]';
    export let percentiledata = '[]';
    export let quantitytype = 'dose';
    export let hasdenominators = 'true';
    export let denominatorvmps = '[]';
    export let numeratorvmps = '[]';
    export let annotations = '[]';
    export let defaultviewmode = 'trust';
    export let userauthenticated = 'false';

    let trusts = [];
    let icbs = [];
    let regions = [];
    let uniqueUnits = [];
    let parsedOrgData = {};

    $: flatOrgData = flattenOrganisationsToData(parsedOrgData.organisations || []);
    let parsedRegionData = [];
    let parsedIcbData = [];
    let showToast = false;
    let isInitialLoad = true;

    function extractUniqueUnits() {
        try {
            const parsedDenominatorVmps = JSON.parse(denominatorvmps || '[]');
            const parsedNumeratorVmps = JSON.parse(numeratorvmps || '[]');
            
            const allProducts = [...parsedDenominatorVmps, ...parsedNumeratorVmps];
            const units = allProducts
                .map(product => product.unit ? pluralize(product.unit) : null)
                .filter(unit => unit && unit !== 'null' && unit !== 'undefined');
            
            return [...new Set(units)].sort();
        } catch (error) {
            console.error('Failed to parse products data:', error);
            return [];
        }
    }

    function loadFromUrlParams() {
        const params = getUrlParams();
        
        const urlMode = params.get('mode');
        const urlTrusts = parseArrayParam(params.get('trusts'));
        const urlRegions = parseArrayParam(params.get('regions'));
        const urlIcbs = parseArrayParam(params.get('icbs'));
        
        let urlShowPercentiles = true;
        if (params.has('show_percentiles')) {
            urlShowPercentiles = params.get('show_percentiles') !== 'false';
        }
        
        return {
            mode: urlMode,
            trusts: urlTrusts,
            regions: urlRegions,
            icbs: urlIcbs,
            showPercentiles: urlShowPercentiles
        };
    }

    function getAvailableTrusts() {
        return trusts.filter((trust) => flatOrgData[trust]?.available);
    }

    function isPercentilesDisabled() {
        return getAvailableTrusts().length < 30;
    }

    function updateUrlParams() {
        if (isInitialLoad) return;
        
        const params = {};
        
        if ($selectedMode !== defaultviewmode) {
            params.mode = $selectedMode;
        }
        
        if ($selectedMode === 'trust') {
            const availableTrusts = getAvailableTrusts();
            const percentilesDisabled = isPercentilesDisabled();
            
            const selectedTrustCodes = Array.from($visibleTrusts)
                .map(trustName => parsedOrgData.org_codes?.[trustName])
                .filter(Boolean);
            
            const allAvailableTrustCodes = availableTrusts
                .map(trustName => parsedOrgData.org_codes?.[trustName])
                .filter(Boolean)
                .sort();
            const selectedTrustCodesSorted = [...selectedTrustCodes].sort();
            
            if (percentilesDisabled) {
                // When percentiles are disabled, default is all available trusts
                // Only include trusts parameter if selection differs from all available trusts
                if (JSON.stringify(selectedTrustCodesSorted) !== JSON.stringify(allAvailableTrustCodes)) {
                    params.trusts = formatArrayParam(selectedTrustCodes);
                }
            } else {
                // When percentiles are enabled, default is no trusts selected
                // Include trusts parameter if any trusts are selected
                if (selectedTrustCodes.length > 0) {
                    if (JSON.stringify(selectedTrustCodesSorted) === JSON.stringify(allAvailableTrustCodes)) {
                        params.trusts = 'all';
                    } else {
                        params.trusts = formatArrayParam(selectedTrustCodes);
                    }
                }
            }
            
            if (!percentilesDisabled && !$showPercentiles) {
                params.show_percentiles = 'false';
            }
        } else if ($selectedMode === 'region') {
            const selectedRegionCodes = Array.from($visibleRegions)
                .map(regionName => parsedRegionData.find(r => r.name === regionName)?.code)
                .filter(Boolean);
            
            // Only add regions parameter if selection differs from default (all regions)
            if (selectedRegionCodes.length > 0 && selectedRegionCodes.length !== regions.length) {
                params.regions = formatArrayParam(selectedRegionCodes);
            }
        } else if ($selectedMode === 'icb') {
            const selectedIcbCodes = Array.from($visibleICBs)
                .map(icbName => parsedIcbData.find(i => i.name === icbName)?.code)
                .filter(Boolean);
            
            // Only add icbs parameter if selection differs from default (all ICBs)
            if (selectedIcbCodes.length > 0 && selectedIcbCodes.length !== icbs.length) {
                params.icbs = formatArrayParam(selectedIcbCodes);
            }
        }
        
        const supportedParams = ['mode', 'trusts', 'regions', 'icbs', 'show_percentiles'];
        setUrlParams(params, supportedParams);
    }

    function applySelectionFromCodes(codes, allItems, visibleStore, codeToNameMapper) {
        if (codes.length === 0) return;
        
        if (codes[0] === 'all') {
            visibleStore.set(new Set(allItems));
            organisationSearchStore.updateSelection(allItems);
        } else {
            const names = codes.map(codeToNameMapper).filter(Boolean);
            if (names.length > 0) {
                visibleStore.set(new Set(names));
                organisationSearchStore.updateSelection(names);
            }
        }
    }

    function applyUrlParams(urlParams) {
        selectedMode.set(urlParams.mode);
        modeSelectorStore.setSelectedMode(urlParams.mode);
        
        if (urlParams.mode === 'trust') {
            applySelectionFromCodes(
                urlParams.trusts,
                trusts.filter((trust) => flatOrgData[trust]?.available),
                visibleTrusts,
                code => {
                    for (const [name, trustCode] of Object.entries(parsedOrgData.org_codes || {})) {
                        if (trustCode === code) return name;
                    }
                    return null;
                }
            );
            
            const availableTrusts = trusts.filter((trust) => flatOrgData[trust]?.available);
            const percentilesDisabled = availableTrusts.length < 30;
            
            if (percentilesDisabled) {
                showPercentiles.set(false);
            } else {
                showPercentiles.set(urlParams.showPercentiles);
            }
        } else if (urlParams.mode === 'region') {
            applySelectionFromCodes(
                urlParams.regions,
                regions,
                visibleRegions,
                code => parsedRegionData.find(r => r.code === code)?.name
            );
        } else if (urlParams.mode === 'icb') {
            applySelectionFromCodes(
                urlParams.icbs,
                icbs,
                visibleICBs,
                code => parsedIcbData.find(i => i.code === code)?.name
            );
        }
    }

    async function handleShareMeasure() {
        try {
            const currentUrl = getCurrentUrl();
            await copyToClipboard(currentUrl);
            showToast = true;
            setTimeout(() => {
                showToast = false;
            }, 2000);
        } catch (error) {
            console.error('Failed to copy URL:', error);
        }
    }

    $: yAxisLabel = getYAxisLabel(quantitytype, hasdenominators, uniqueUnits);
    $: yAxisTickFormatter = getYAxisTickFormatter(quantitytype, hasdenominators);
    $: yAxisLimits = getYAxisLimits(hasdenominators, $filteredData);
    
    function getYAxisLabel(quantityType, hasDenominators, units) {
        const hasDenom = hasDenominators === 'true';
        
        if (hasDenom) {
            return '%';
        } else {
            if (quantityType === 'indicative_cost') {
                return 'Indicative Cost (£)';
            } else {

                if (units && units.length > 0) {
                    const unitsString = units.join(' / ');
                    return `Quantity (${unitsString})`;
                } else {
                    return 'Quantity';
                }
            }
        }
    }

    function getYAxisTickFormatter(quantityType, hasDenominators) {
        const hasDenom = hasDenominators === 'true';
        
        return function(value, range) {
            if (hasDenom) {
                // For measures with denominators, show percentages
                let decimals = 1;
                if (range <= 0.1) decimals = 3;
                else if (range <= 1) decimals = 2;
                return `${value.toFixed(decimals)}%`;
            } else if (quantityType === 'indicative_cost') {
                // For indicative cost, show currency formatting
                if (value === 0) return '£0';
                if (value >= 1000) {
                    return `£${(value / 1000)}K`;
                } else {
                    return `£${value}`;
                }
            } else {
                // For other quantity types, show plain numbers
                if (value === 0) return '0';
                if (value >= 1000) {
                    return `${(value / 1000)}K`;
                } else {
                    return value.toString();
                }
            }
        };
    }

    function getYAxisLimits(hasDenominators, chartData) {
        const hasDenom = hasDenominators === 'true';
        
        if (hasDenom) {
            return [0, 100];
        } else if (chartData && chartData.datasets && chartData.datasets.length > 0) {
            let maxValue = 0;
            
            chartData.datasets.forEach(dataset => {
                if (!dataset.hidden && dataset.data && Array.isArray(dataset.data)) {
                    dataset.data.forEach(value => {
                        if (typeof value === 'object' && value.upper !== undefined) {
                            maxValue = Math.max(maxValue, value.upper);
                        } else if (typeof value === 'number') {
                            maxValue = Math.max(maxValue, value);
                        }
                    });
                }
            });
            return [0, maxValue * 1.1];
        } else {
            return [0, 100];
        }
    }


    $: showFilter = ['trust', 'icb', 'region', 'national'].includes($selectedMode);

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.setOrganisationData({
                orgs: Object.fromEntries(icbs.map(name => [name, name])),
                predecessor_map: {},
                regions_hierarchy: parsedOrgData.regions_hierarchy || []
            });
            organisationSearchStore.setFilterType('icb');
        } else if ($selectedMode === 'trust') {
            organisationSearchStore.setOrganisationData({
                orgs: Object.fromEntries(trusts.map(name => [parsedOrgData.org_codes?.[name] || name, name])),
                org_codes: parsedOrgData.org_codes || {},
                predecessor_map: parsedOrgData.predecessor_map || {},
                trust_types: parsedOrgData.trust_types || {},
                org_regions: parsedOrgData.org_regions || {},
                org_icbs: parsedOrgData.org_icbs || {},
                regions_hierarchy: parsedOrgData.regions_hierarchy || []
            });
            organisationSearchStore.setFilterType('trust');
        } else if ($selectedMode === 'region') {
            organisationSearchStore.setOrganisationData({
                orgs: Object.fromEntries(regions.map(name => [name, name])),
                predecessor_map: {}
            });
            organisationSearchStore.setFilterType('region');
        }
    }

    const measureChartStore = createChartStore({
        mode: 'trust',
        yAxisLabel: yAxisLabel,
        yAxisTickFormat: yAxisTickFormatter,
        yAxisRange: yAxisLimits,
        yAxisBehavior: {
            forceZero: true,
            resetToInitial: true,
            fixedRange: hasdenominators === 'true'
        },
        percentileConfig: {
            medianColor: '#DC3220',
            rangeColor: 'rgb(0, 90, 181)'
        }
    });

    $: chartOptions = {
        chart: {
            type: 'line',
            height: 350
        },
        title: {
            text: undefined
        },
        yAxis: hasdenominators === 'true' ? {
        min: 0,
        max: 100,
        allowDecimals: false,
        tickInterval: 20
        } : {
            allowDecimals: true,
            tickAmount: 6,
            endOnTick: true,
            startOnTick: true,
        }
    };

    onMount(async () => {
        parsedOrgData = JSON.parse(orgdata);

        const flat = flattenOrganisationsToData(parsedOrgData.organisations || []);
        orgdataStore.set(flat);

        uniqueUnits = extractUniqueUnits();

        measureChartStore.setDimensions({
            height: 500,
            margin: { top: 10, right: 20, bottom: 30, left: 80 }
        });

        trusts = Object.keys(flat);

        const availableTrusts = trusts.filter((trust) => flat[trust]?.available);
        
        const shouldDisablePercentiles = availableTrusts.length < 30;
        
        organisationSearchStore.setOrganisationData({
            orgs: Object.fromEntries(trusts.map(name => [parsedOrgData.org_codes?.[name] || name, name])),
            org_codes: parsedOrgData.org_codes || {},
            predecessor_map: parsedOrgData.predecessor_map || {}
        });
        organisationSearchStore.setFilterType('trust');
        organisationSearchStore.setAvailableItems(availableTrusts);
        
        parsedIcbData = JSON.parse(icbdata);
        icbStore.set(parsedIcbData);
        icbs = parsedIcbData.map(icb => icb.name);
        
        parsedRegionData = JSON.parse(regiondata);
        regionStore.set(parsedRegionData);
        regions = parsedRegionData.map(region => region.name);
        
        percentileStore.set(JSON.parse(percentiledata));

        const urlParams = loadFromUrlParams();

        visibleICBs.set(new Set(icbs));
        visibleRegions.set(new Set(regions));

        if (shouldDisablePercentiles) {
            visibleTrusts.set(new Set(availableTrusts));
            showPercentiles.set(false);
        } else {
            visibleTrusts.set(new Set());
            showPercentiles.set(true);
        }

        const hasExplicitModeOverride = urlParams.mode && urlParams.mode !== defaultviewmode;

        const hasExplicitSelections = urlParams.trusts.length > 0 || urlParams.regions.length > 0 || urlParams.icbs.length > 0;

        const hasExplicitPercentileOverride = !shouldDisablePercentiles && getUrlParams().has('show_percentiles');
        await tick();

        if (hasExplicitModeOverride || hasExplicitSelections || hasExplicitPercentileOverride) {
            const effectiveUrlParams = {
                ...urlParams,
                mode: urlParams.mode || defaultviewmode
            };
            applyUrlParams(effectiveUrlParams);
            await tick();
        } else {
            selectedMode.set(defaultviewmode);
            modeSelectorStore.setSelectedMode(defaultviewmode);
            
            if (shouldDisablePercentiles) {
                organisationSearchStore.updateSelection(availableTrusts);
            } else {
                organisationSearchStore.updateSelection([]);
            }
        }

        isInitialLoad = false;

        const parsedNationalData = JSON.parse(nationaldata);
        nationalStore.set(parsedNationalData);
    });

    function handleSelectionChange(event) {
        const selectedItems = event.detail?.selectedItems || [];
        const source = event.detail?.source;
 
        if ($selectedMode === 'icb') {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : Array.from(selectedItems);
            visibleICBs.set(new Set(itemsArray));
            organisationSearchStore.updateSelection(itemsArray);
            
            const updatedData = {
                ...$filteredData,
                datasets: ($filteredData?.datasets || []).map(dataset => ({
                    ...dataset,
                    hidden: !itemsArray.includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        } else if ($selectedMode === 'region') {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : Array.from(selectedItems);
            visibleRegions.set(new Set(itemsArray));
            organisationSearchStore.updateSelection(itemsArray);

            const updatedData = {
                ...$filteredData,
                datasets: ($filteredData?.datasets || []).map(dataset => ({
                    ...dataset,
                    hidden: !itemsArray.includes(dataset.label)
                }))
            };
            measureChartStore.setData(updatedData);
        } else if ($selectedMode === 'trust') {
            const itemsArray = Array.isArray(selectedItems) ? selectedItems : Array.from(selectedItems);
            visibleTrusts.set(new Set(itemsArray));
            organisationSearchStore.updateSelection(itemsArray);
            if ($selectedMode === 'trust') {
                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: (!$showPercentiles && (
                            dataset.label === 'Median (50th percentile)' ||
                            dataset.label.includes('th percentile')
                        )) || (
                            !Array.from($visibleTrusts).includes(dataset.label) && 
                            !dataset.alwaysVisible
                        )
                    }))
                };
                measureChartStore.setData(updatedData);
            }
            measureChartStore.updateVisibleItems(new Set(itemsArray));
        }
    }

    const modeOptions = [
        { value: 'trust', label: 'NHS Trust' },
        { value: 'icb', label: 'ICB' },
        { value: 'region', label: 'Region' },
        { value: 'national', label: 'National' },
    ];

    $: currentMode = $modeSelectorStore.selectedMode;
    $: {
        if (currentMode) {
            selectedMode.set(currentMode);
            

            if (currentMode === 'icb') {
                organisationSearchStore.setOrganisationData({
                    orgs: Object.fromEntries(icbs.map(name => [name, name])),
                    predecessor_map: {},
                    regions_hierarchy: parsedOrgData.regions_hierarchy || []
                });
                organisationSearchStore.setFilterType('icb');
                organisationSearchStore.setAvailableItems(icbs);
                organisationSearchStore.updateSelection(Array.from($visibleICBs));

                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: !Array.from($visibleICBs).includes(dataset.label)
                    }))
                };
                measureChartStore.setData(updatedData);
            } else if (currentMode === 'region') {
                organisationSearchStore.setOrganisationData({
                    orgs: Object.fromEntries(regions.map(name => [name, name])),
                    predecessor_map: {}
                });
                organisationSearchStore.setFilterType('region');
                organisationSearchStore.setAvailableItems(regions);
                organisationSearchStore.updateSelection(Array.from($visibleRegions));

                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: !Array.from($visibleRegions).includes(dataset.label)
                    }))
                };
                measureChartStore.setData(updatedData);
            } else if (currentMode === 'trust') {
                organisationSearchStore.setOrganisationData({
                    orgs: Object.fromEntries(trusts.map(name => [parsedOrgData.org_codes?.[name] || name, name])),
                    org_codes: parsedOrgData.org_codes || {},
                    predecessor_map: parsedOrgData.predecessor_map || {},
                    trust_types: parsedOrgData.trust_types || {},
                    org_regions: parsedOrgData.org_regions || {},
                    org_icbs: parsedOrgData.org_icbs || {},
                    regions_hierarchy: parsedOrgData.regions_hierarchy || []
                });
                organisationSearchStore.setFilterType('trust');
                const availableTrusts = trusts.filter(trust => $orgdataStore[trust]?.available);
                organisationSearchStore.setAvailableItems(availableTrusts);

                organisationSearchStore.updateSelection(Array.from($visibleTrusts));
                measureChartStore.updateVisibleItems(new Set($visibleTrusts));
                if (currentMode === 'trust') {
                    const updatedData = {
                        ...$filteredData,
                        datasets: $filteredData.datasets.map(dataset => ({
                            ...dataset,
                            hidden: (!$showPercentiles && (
                                dataset.label === 'Median (50th percentile)' ||
                                dataset.label.includes('th percentile')
                            )) || (
                                !Array.from($visibleTrusts).includes(dataset.label) && 
                                !dataset.alwaysVisible
                            )
                        }))
                    };
                    measureChartStore.setData(updatedData);
                }
            } else if (currentMode === 'national') {

                const updatedData = {
                    ...$filteredData,
                    datasets: $filteredData.datasets.map(dataset => ({
                        ...dataset,
                        hidden: false
                    }))
                };
                measureChartStore.setData(updatedData);
            }
        }
    }

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.updateSelection(Array.from($visibleICBs));
        } else if ($selectedMode === 'region') {
            organisationSearchStore.updateSelection(Array.from($visibleRegions));
        } else if ($selectedMode === 'trust') {
            organisationSearchStore.updateSelection(Array.from($visibleTrusts));
        }
    }

    $: {
        $visibleTrusts;
        $visibleRegions; 
        $visibleICBs;
        $selectedMode;
        $showPercentiles;
        
        if (!isInitialLoad) {
            updateUrlParams();
        }
    }

    function handleModeChange(newMode) {
        if (isInitialLoad) return;

        selectedMode.set(newMode);

        if (newMode === 'region') {
            visibleRegions.set(new Set(regions));
            visibleICBs.set(new Set());
            visibleTrusts.set(new Set());
            organisationSearchStore.updateSelection([...regions]);
        } else if (newMode === 'icb') {
            visibleICBs.set(new Set(icbs));
            visibleRegions.set(new Set());
            visibleTrusts.set(new Set());
            organisationSearchStore.updateSelection([...icbs]);
        } else {
            visibleICBs.set(new Set());
            visibleRegions.set(new Set());
            visibleTrusts.set(new Set());
            organisationSearchStore.updateSelection([]);
        }

        if ($filteredData) {
            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => {
                    if (newMode === 'trust') {
                        return {
                            ...dataset,
                            hidden: !getDatasetVisibility(dataset, newMode, $visibleTrusts, $showPercentiles)
                        };
                    } else {
                        return {
                            ...dataset,
                            hidden: newMode === 'national' ? false :
                                newMode === 'region' ? !$visibleRegions.has(dataset.label) :
                                newMode === 'icb' ? !$visibleICBs.has(dataset.label) :
                                !$visibleTrusts.has(dataset.label)
                        };
                    }
                })
            };
            measureChartStore.setData(updatedData);
        }
    }

    $: {
        if ($filteredData && $selectedMode === 'trust') {
            const updatedData = {
                ...$filteredData,
                datasets: $filteredData.datasets.map(dataset => ({
                    ...dataset,
                    hidden: (!$showPercentiles && (
                        dataset.label === 'Median (50th percentile)' ||
                        dataset.label.includes('th percentile')
                    )) || (
                        !Array.from($visibleTrusts).includes(dataset.label) && 
                        !dataset.alwaysVisible
                    )
                }))
            };
            measureChartStore.setData(updatedData);
        }
    }

    $: if ($selectedMode) {
        const currentLimits = getYAxisLimits(hasdenominators, $filteredData);
        measureChartStore.setConfig({
            ...$measureChartStore.config,
            mode: $selectedMode,
            yAxisLabel: yAxisLabel,
            yAxisTickFormat: yAxisTickFormatter,
            yAxisRange: currentLimits,
            yAxisBehavior: {
                forceZero: true,
                resetToInitial: true,
                fixedRange: hasdenominators === 'true'
            },
            percentileConfig: {
                medianColor: '#DC3220',
                rangeColor: 'rgb(0, 90, 181)'
            }
        });
    }

    function handleClearAll() {
        if ($selectedMode === 'icb') {
            visibleICBs.set(new Set());
        } else if ($selectedMode === 'region') {
            visibleRegions.set(new Set());
        } else if ($selectedMode === 'trust') {
            visibleTrusts.set(new Set());
        }
        
        measureChartStore.updateVisibleItems(new Set());
        organisationSearchStore.updateSelection([]);

        const updatedData = {
            ...$filteredData,
            datasets: $filteredData.datasets.map(dataset => ({
                ...dataset,
                hidden: true
            }))
        };

        measureChartStore.setData(updatedData);
    }

    $: {
        if ($selectedMode === 'icb') {
            organisationSearchStore.setOrganisationData({
                orgs: Object.fromEntries(icbs.map(name => [name, name])),
                predecessor_map: {},
                regions_hierarchy: parsedOrgData.regions_hierarchy || []
            });
            organisationSearchStore.setFilterType('icb');
            organisationSearchStore.setAvailableItems(icbs);
        } else if ($selectedMode === 'region') {
            organisationSearchStore.setOrganisationData({
                orgs: Object.fromEntries(regions.map(name => [name, name])),
                predecessor_map: {}
            });
            organisationSearchStore.setFilterType('region');
            organisationSearchStore.setAvailableItems(regions);
        } else if ($selectedMode === 'trust') {
            organisationSearchStore.setOrganisationData({
                orgs: Object.fromEntries(trusts.map(name => [parsedOrgData.org_codes?.[name] || name, name])),
                org_codes: parsedOrgData.org_codes || {},
                predecessor_map: parsedOrgData.predecessor_map || {},
                trust_types: parsedOrgData.trust_types || {},
                org_regions: parsedOrgData.org_regions || {},
                org_icbs: parsedOrgData.org_icbs || {},
                regions_hierarchy: parsedOrgData.regions_hierarchy || []
            });
            organisationSearchStore.setFilterType('trust');
            const availableTrusts = trusts.filter(trust => $orgdataStore[trust]?.available);
            organisationSearchStore.setAvailableItems(availableTrusts);
        }
    }

    function customTooltipFormatter(d) {
        const label = d.dataset.name || d.dataset.label || 'No label';
        const date = new Date(d.date);
        const formattedDate = date.toLocaleString('en-GB', { month: 'short', year: 'numeric' });
        
        let value;
        if (hasdenominators === 'true') {
            value = (d.value).toFixed(1) + '%';
        } else if (quantitytype === 'indicative_cost') {
            value = '£' + formatNumber(d.value, { addCommas: true, decimalPlaces: 2 });
        } else {
            value = formatNumber(d.value, { addCommas: true });
        }
        
        const index = d.index;

        const tooltipContent = [
            { text: label, class: 'font-medium' }
        ];

        if ($selectedMode === 'region' || $selectedMode === 'icb' || $selectedMode === 'national') {
            const tooltipEntries = [
                { label: 'Date', value: formattedDate }
            ];
            
            if (hasdenominators === 'true') {
                tooltipEntries.push(
                    { label: 'Numerator', value: formatNumber(d.dataset.numerator?.[index] || 0, { addCommas: true }) },
                    { label: 'Denominator', value: formatNumber(d.dataset.denominator?.[index] || 0, { addCommas: true }) }
                );
            }
            
            tooltipEntries.push({ label: 'Value', value });
            tooltipContent.push(...tooltipEntries);
        } else if ($selectedMode === 'trust') {
            if (d.dataset.label === 'Median (50th percentile)' || d.dataset.name === 'Median (50th percentile)') {
                tooltipContent.push(
                    { label: 'Date', value: formattedDate },
                    { label: 'Value', value }
                );
            } else if (d.dataset.isTrust || d.dataset.isOrganisation) {
                const tooltipEntries = [
                    { label: 'Date', value: formattedDate }
                ];
                
                if (hasdenominators === 'true') {
                    tooltipEntries.push(
                        { label: 'Numerator', value: formatNumber(d.dataset.numerator?.[index] || 0, { addCommas: true }) },
                        { label: 'Denominator', value: formatNumber(d.dataset.denominator?.[index] || 0, { addCommas: true }) }
                    );
                }
                
                tooltipEntries.push({ label: 'Value', value });
                tooltipContent.push(...tooltipEntries);
            }
        }

        return tooltipContent;
    }

    $: percentilesDisabled = trusts.filter(trust => $orgdataStore[trust]?.available).length < 30;

    function handlePercentileToggle() {
        if (!percentilesDisabled) {
            showPercentiles.update(v => !v);
            const updatedData = updatePercentilesVisibility(!$showPercentiles);
            measureChartStore.setData(updatedData);
        }
    }
</script>

<div>
    <div class="flex justify-end px-4 sm:px-8 mb-2">
        <button
            type="button"
            class="flex items-center gap-1 px-3 py-2 text-sm font-medium text-oxford-600 bg-white border border-oxford-200 rounded-md hover:bg-oxford-50 hover:border-oxford-300 transition-colors duration-200"
            on:click={handleShareMeasure}
            title="Copy link to share this measure with current selections"
        >
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-4 h-4">
                <path stroke-linecap="round" stroke-linejoin="round" d="M7.217 10.907a2.25 2.25 0 100 2.186m0-2.186c.18.324.283.696.283 1.093s-.103.77-.283 1.093m0-2.186l9.566-5.314m-9.566 7.5l9.566 5.314m0 0a2.25 2.25 0 103.935 2.186 2.25 2.25 0 00-3.935-2.186zm0-12.814a2.25 2.25 0 103.935-2.186 2.25 2.25 0 00-3.935 2.186z" />
            </svg>
            Share Measure
        </button>
    </div>

    <div class="flex flex-col md:flex-row justify-between gap-4 px-4 sm:px-8">
        {#if showFilter}
            <div class="w-full md:w-7/12 relative z-10">
                {#if userauthenticated === 'true'}
                    <OrganisationSearchFiltered
                        source={organisationSearchStore}
                        filterResetKey={$selectedMode}
                        overlayMode={true}
                        filterAutoSelectsAll={false}
                        on:selectionChange={handleSelectionChange}
                        on:clearAll={handleClearAll}
                        disabled={$selectedMode === 'national'}
                    />
                {:else}
                    <OrganisationSearch
                        source={organisationSearchStore}
                        overlayMode={true}
                        on:selectionChange={handleSelectionChange}
                        on:clearAll={handleClearAll}
                        disabled={$selectedMode === 'national'}
                    />
                {/if}
            </div>
        {:else}
            <div class="w-full md:w-7/12"></div>
        {/if}

        <div class="w-full md:w-auto flex justify-between items-end gap-4">
            {#if $selectedMode === 'trust'}
            <div class="flex flex-col items-center gap-2">
                <span class="text-sm text-gray-600 leading-tight text-center">
                    Show<br>percentiles
                </span>
                <div class="flex items-center gap-2">
                    <label class="inline-flex items-center cursor-pointer {percentilesDisabled ? 'opacity-50 cursor-not-allowed' : ''}">
                        <input
                            type="checkbox"
                            class="sr-only peer"
                            checked={$showPercentiles}
                            on:change={handlePercentileToggle}
                            disabled={percentilesDisabled}
                        />
                        <div class="relative w-9 h-5 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-blue-600 {percentilesDisabled ? 'opacity-50' : ''}"></div>
                    </label>
                    <div class="relative inline-block group">
                        <button type="button" class="text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-oxford-500 flex items-center">
                            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                                <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd" />
                            </svg>
                        </button>
                        <div class="absolute z-10 scale-0 transition-all duration-100 origin-top transform 
                                    group-hover:scale-100 w-[250px] left-0 sm:-translate-x-1/2 sm:left-1/2 top-5 rounded-md shadow-lg bg-white 
                                    ring-1 ring-black ring-opacity-5 p-4">
                            <p class="text-sm text-gray-500">
                                {#if percentilesDisabled}
                                    Percentiles are disabled when there are fewer than 30 trusts in a measure.
                                {:else}
                                    Percentiles show variation in this measure across Trusts and allow easy comparison of Trust activity relative to the median Trust level. 
                                    {#if hasdenominators === 'false'}
                                    Variation can reflect differences in hospital size rather than genuine variation. 
                                    {/if}
                                    See <a href="/faq/#what-are-percentile-charts" class="underline font-semibold" target="_blank">the FAQs</a> for more details about how to interpret them.
                                {/if}
                            </p>
                        </div>
                    </div>
                </div>
            </div>
        {/if}
            <div>
                <ModeSelector 
                    options={modeOptions}
                    initialMode={defaultviewmode}
                    label="Select Mode"
                    onChange={handleModeChange}
                    variant="dropdown"
                />
            </div>

        </div>
    </div>

    <div class="lg:col-span-4 relative h-[550px]">
        <div class="chart-container absolute inset-0">
            {#if $orgdataStore.length === 0}
                <p class="text-center text-gray-500 pt-8">No data available.</p>
            {:else}
                <Chart 
                    data={$filteredData}
                    mode={$selectedMode}
                    yAxisLabel={yAxisLabel}
                    formatTooltipContent={customTooltipFormatter}
                    store={measureChartStore}
                    {chartOptions}
                    {annotations}
                />
            {/if}
        </div>
    </div>

    {#if showToast}
        <div class="fixed bottom-4 right-4 bg-oxford-50 text-oxford-800 px-4 py-2 rounded-lg shadow-lg border border-oxford-100 transform translate-y-0 opacity-100 transition-all duration-300 z-50">
            <div class="flex items-center gap-2">
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-5 h-5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                </svg>
                Measure link copied to clipboard!
            </div>
        </div>
    {/if}
</div>
