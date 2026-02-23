<svelte:options customElement={{
  tag: 'measures-list',
  props: {
    measures: { type: 'String', reflect: true },
    previewMeasures: { type: 'String', reflect: true },
    inDevelopmentMeasures: { type: 'String', reflect: true },
    chartData: { type: 'String', reflect: true },
    previewMode: { type: 'String', reflect: true },
    userAuthenticated: { type: 'String', reflect: true },
    measureTrustsBasePath: { type: 'String', reflect: true },
    orgData: { type: 'String', reflect: true },
    regionData: { type: 'String', reflect: true },
    tagsData: { type: 'String', reflect: true },
    initialMode: { type: 'String', reflect: true },
    initialCode: { type: 'String', reflect: true },
    initialSort: { type: 'String', reflect: true },
    initialTags: { type: 'String', reflect: true },
  },
  shadow: 'none'
}} />

<script>
  import MeasuresListControls from './MeasuresListControls.svelte';
  import MeasureCard from './MeasureCard.svelte';
  import LazyLoad from '../common/LazyLoad.svelte';
  import {
    selectedTags, sort, mode, selectedCode,
    chartData as chartDataStore
  } from '../../stores/measuresListStore.js';
  import { deriveSortMetricsFromChartData } from '../../utils/measuresSortUtils.js';

  export let measures = '[]';
  export let previewMeasures = '[]';
  export let inDevelopmentMeasures = '[]';
  export let chartData = '{}';
  export let previewMode = 'false';
  export let userAuthenticated = 'false';
  export let measureTrustsBasePath = '/measures/';
  export let orgData = '{}';
  export let regionData = '[]';
  export let tagsData = '[]';
  export let initialMode = 'trust';
  export let initialCode = '';
  export let initialSort = 'name';
  export let initialTags = '';

  $: parsedMeasures = (() => { try { return JSON.parse(measures || '[]'); } catch (e) { return []; } })();
  $: parsedPreviewMeasures = (() => { try { return JSON.parse(previewMeasures || '[]'); } catch (e) { return []; } })();
  $: parsedInDevelopmentMeasures = (() => { try { return JSON.parse(inDevelopmentMeasures || '[]'); } catch (e) { return []; } })();

  $: selectedTagsVal = $selectedTags;
  $: sortVal = $sort;
  $: chartDataStoreVal = $chartDataStore;
  $: trustOverlayActive = $mode === 'trust' && !!$selectedCode;

  function trustIncludedInMeasure(slug) {
    if (!trustOverlayActive || !chartDataStoreVal) return true;
    const trustData = chartDataStoreVal[slug]?.trustData;
    return Array.isArray(trustData) && trustData.length > 0;
  }

  $: allMeasures = [...parsedMeasures, ...parsedPreviewMeasures, ...parsedInDevelopmentMeasures];
  $: effectiveSortMetrics = $mode === 'trust' && chartDataStoreVal && Object.keys(chartDataStoreVal).length > 0
    ? deriveSortMetricsFromChartData(chartDataStoreVal, allMeasures)
    : {};

  $: filteredPublished = filterByTags(parsedMeasures, selectedTagsVal);
  $: filteredPreview = filterByTags(parsedPreviewMeasures, selectedTagsVal);
  $: filteredInDevelopment = filterByTags(parsedInDevelopmentMeasures, selectedTagsVal);

  $: sortedPublished = applySort(filteredPublished, sortVal, effectiveSortMetrics);
  $: sortedPreview = applySort(filteredPreview, sortVal, effectiveSortMetrics);
  $: sortedInDevelopment = applySort(filteredInDevelopment, sortVal, effectiveSortMetrics);

  $: hasFiltersWithNoResults = selectedTagsVal.length > 0 &&
    sortedPublished.length === 0 && sortedPreview.length === 0 && sortedInDevelopment.length === 0 &&
    (parsedMeasures.length > 0 || parsedPreviewMeasures.length > 0 || parsedInDevelopmentMeasures.length > 0);

  function filterByTags(measureList, tags) {
    if (!tags || !tags.length) return measureList;
    return measureList.filter((m) => {
      const slugs = (m.tag_slugs || '').split(',').map((s) => s.trim()).filter(Boolean);
      return tags.some((t) => slugs.includes(t));
    });
  }

  const TRUST_SORT_POTENTIAL_IMPROVEMENT = 'potential_improvement';
  const TRUST_SORT_MOST_IMPROVED = 'most_improved';

  function applySort(measureList, sort, sortMetrics) {
    if (sort === TRUST_SORT_POTENTIAL_IMPROVEMENT || sort === TRUST_SORT_MOST_IMPROVED) {
      return sortByTrustMetrics(measureList, sort, sortMetrics);
    }
    return sortMeasures(measureList, sort);
  }

  function sortByTrustMetrics(measureList, sort, sortMetrics) {
    if (!sortMetrics || typeof sortMetrics !== 'object') return sortMeasures(measureList, 'name');
    const key = sort === TRUST_SORT_POTENTIAL_IMPROVEMENT ? 'potential_improvement' : 'most_improved';
    return [...measureList].sort((a, b) => {
      const ma = sortMetrics[a.slug]?.[key];
      const mb = sortMetrics[b.slug]?.[key];
      const nameA = (a.short_name || a.name || '').toLowerCase();
      const nameB = (b.short_name || b.name || '').toLowerCase();
      const noMetricA = ma == null;
      const noMetricB = mb == null;
      if (noMetricA && noMetricB) return nameA.localeCompare(nameB, undefined, { sensitivity: 'base' });
      if (noMetricA) return 1;
      if (noMetricB) return -1;
      const valA = ma ?? 0;
      const valB = mb ?? 0;
      if (valA !== valB) return valB - valA;
      return nameA.localeCompare(nameB, undefined, { sensitivity: 'base' });
    });
  }

  function sortMeasures(measureList, sort) {
    if (sort === 'newest') {
      return [...measureList].sort((a, b) => {
        const da = a.first_published ? new Date(a.first_published).getTime() : 0;
        const db = b.first_published ? new Date(b.first_published).getTime() : 0;
        return db - da;
      });
    }
    return [...measureList].sort((a, b) =>
      (a.short_name || '').localeCompare(b.short_name || '', undefined, { sensitivity: 'base' })
    );
  }
</script>

{#if userAuthenticated === 'true'}
  <div class="mb-8">
    <MeasuresListControls
      orgData={orgData}
      regionData={regionData}
      chartDataJson={chartData}
      selectedMode={initialMode}
      selectedCode={initialCode}
      selectedSort={initialSort}
      tagsData={tagsData}
      selectedTags={initialTags}
    />
  </div>
{/if}

{#if hasFiltersWithNoResults}
  <div class="mb-8 p-6 bg-gray-50 border border-gray-200 rounded-lg text-center">
    <p class="text-gray-600">No measures match your selected filters.</p>
    <p class="text-sm text-gray-500 mt-1">Try clearing some tags to see more measures.</p>
  </div>
{:else}
{#if parsedMeasures.length > 0 && previewMode !== 'true'}
  <LazyLoad>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8" data-measures-section="published">
      {#each sortedPublished as measure (measure.slug)}
        <MeasureCard
          measure={measure}
          section="published"
          linkClasses="bg-oxford-50 text-oxford-600 hover:bg-oxford-100"
          linkText="View measure details"
          isAuthenticated={userAuthenticated === 'true'}
          trustIncludedInMeasure={trustIncludedInMeasure(measure.slug)}
          trustSelected={trustOverlayActive}
          {measureTrustsBasePath}
        />
      {/each}
    </div>
    <div slot="placeholder" class="grid grid-cols-1 lg:grid-cols-2 gap-8">
      {#each [1, 2] as _}
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <div class="h-[5rem] bg-gray-200 rounded animate-pulse mb-4"></div>
          <div class="space-y-3 mb-4">
            <div class="h-4 bg-gray-200 rounded w-3/4 animate-pulse"></div>
            <div class="h-4 bg-gray-200 rounded w-full animate-pulse"></div>
            <div class="h-4 bg-gray-200 rounded w-1/2 animate-pulse"></div>
          </div>
          <div class="h-[280px] bg-gray-200 rounded-lg animate-pulse mb-4"></div>
          <div class="h-10 bg-gray-200 rounded-lg animate-pulse"></div>
        </div>
      {/each}
    </div>
  </LazyLoad>
{/if}

{#if parsedPreviewMeasures.length > 0}
  <LazyLoad>
    <div class="mb-12">
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8" data-measures-section="preview">
        {#each sortedPreview as measure (measure.slug)}
          <MeasureCard
            measure={measure}
            section="preview"
            cardHeaderClass="bg-blue-50 py-2 px-4 border-b border-gray-100"
            statusBadge="Preview"
            statusBadgeClass="bg-blue-100 text-blue-800"
            linkClasses="bg-blue-50 text-blue-600 hover:bg-blue-100"
            linkText="View preview"
            isAuthenticated={userAuthenticated === 'true'}
            trustIncludedInMeasure={trustIncludedInMeasure(measure.slug)}
            trustSelected={trustOverlayActive}
            {measureTrustsBasePath}
          />
        {/each}
      </div>
    </div>
    <div slot="placeholder" class="mb-12">
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {#each [1, 2] as _}
          <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
            <div class="h-[5rem] bg-gray-200 rounded animate-pulse mb-4"></div>
            <div class="space-y-3 mb-4">
              <div class="h-4 bg-gray-200 rounded w-3/4 animate-pulse"></div>
              <div class="h-4 bg-gray-200 rounded w-full animate-pulse"></div>
              <div class="h-4 bg-gray-200 rounded w-1/2 animate-pulse"></div>
            </div>
            <div class="h-[280px] bg-gray-200 rounded-lg animate-pulse mb-4"></div>
            <div class="h-10 bg-gray-200 rounded-lg animate-pulse"></div>
          </div>
        {/each}
      </div>
    </div>
  </LazyLoad>
{/if}

{#if parsedInDevelopmentMeasures.length > 0}
  <LazyLoad>
    <div class="mb-12">
      <h2 class="text-2xl font-semibold mb-6 text-gray-900">In Development</h2>
      <p class="text-gray-600 mb-6">These measures are currently in development. Public previews are not available for these measures. You can see them because you are logged in.</p>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8" data-measures-section="in_development">
        {#each sortedInDevelopment as measure (measure.slug)}
          <MeasureCard
            measure={measure}
            section="in_development"
            cardHeaderClass="bg-amber-50 py-2 px-4 border-b border-gray-100"
            statusBadge="🚧 In development"
            statusBadgeClass="bg-amber-100 text-amber-800"
            linkClasses="bg-amber-50 text-amber-600 hover:bg-amber-100"
            linkText="View in development"
            isAuthenticated={userAuthenticated === 'true'}
            trustIncludedInMeasure={trustIncludedInMeasure(measure.slug)}
            trustSelected={trustOverlayActive}
            {measureTrustsBasePath}
          />
        {/each}
      </div>
    </div>
    <div slot="placeholder" class="mb-12">
      <div class="h-8 bg-gray-200 rounded w-48 animate-pulse mb-6"></div>
      <div class="h-4 bg-gray-200 rounded w-full animate-pulse mb-6"></div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {#each [1, 2] as _}
          <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
            <div class="h-[5rem] bg-gray-200 rounded animate-pulse mb-4"></div>
            <div class="space-y-3 mb-4">
              <div class="h-4 bg-gray-200 rounded w-3/4 animate-pulse"></div>
              <div class="h-4 bg-gray-200 rounded w-full animate-pulse"></div>
              <div class="h-4 bg-gray-200 rounded w-1/2 animate-pulse"></div>
            </div>
            <div class="h-[280px] bg-gray-200 rounded-lg animate-pulse mb-4"></div>
            <div class="h-10 bg-gray-200 rounded-lg animate-pulse"></div>
          </div>
        {/each}
      </div>
    </div>
  </LazyLoad>
{/if}
{/if}
