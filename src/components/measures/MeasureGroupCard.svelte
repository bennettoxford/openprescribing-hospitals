<script>
  import MeasureMiniChart from './MeasureMiniChart.svelte';
  import { mode, selectedCode, sort, selectedTags, showArchived, chartData } from '../../stores/measuresListStore.js';

  let {
    group,
    cardHeaderClass = 'py-2 px-4 border-b border-gray-100',
    statusBadge = '',
    statusBadgeClass = '',
    linkClasses = 'bg-oxford-50 text-oxford-600 hover:bg-oxford-100',
    cardClass = '',
    groupBasePath = '/measures/group/',
  } = $props();

  let measures = $derived(group.measures);
  let count = $derived(measures.length);
  let countLabel = $derived(count === 1 ? '1 measure' : `${count} measures`);
  let slideIndex = $state(0);
  let safeIndex = $derived(count === 0 ? 0 : Math.min(slideIndex, count - 1));
  let currentMeasure = $derived(measures[safeIndex] ?? null);
  let trustSelected = $derived($mode === 'trust' && !!$selectedCode);
  let currentChartData = $derived(currentMeasure ? $chartData[currentMeasure.slug] : undefined);
  let trustIncluded = $derived(!trustSelected || !currentChartData || !('trustData' in currentChartData)
    ? true
    : Array.isArray(currentChartData.trustData) && currentChartData.trustData.length > 0);
  let anyNew = $derived(measures.some((measure) => measure.is_new));
  let tags = $derived(uniqueTags(measures));
  let groupHref = $derived(buildGroupHref(
    groupBasePath,
    group.slug,
    $mode,
    $selectedCode,
    $sort,
    $selectedTags,
    $showArchived,
  ));

  function uniqueTags(measureList) {
    const seen = new Set();
    const result = [];
    for (const measure of measureList) {
      for (const tag of measure.tags || []) {
        const key = tag.slug || tag.name;
        if (!key || seen.has(key)) continue;
        seen.add(key);
        result.push(tag);
      }
    }
    return result;
  }

  function buildGroupHref(basePath, slug, currentMode, code, currentSort, tagList, archived) {
    const base = `${(basePath || '/measures/group/').replace(/\/?$/, '/')}${slug}/`;
    const params = new URLSearchParams();
    if (currentMode && currentMode !== 'trust') params.set('mode', currentMode);
    if (currentMode === 'trust' && code) params.set('trust', code);
    if (currentMode === 'region' && code) params.set('region', code);
    if (currentSort) params.set('sort', currentSort);
    if (tagList && tagList.length) params.set('tags', tagList.join(','));
    if (archived && archived !== 'off') params.set('show_archived', archived);
    const query = params.toString();
    return query ? `${base}?${query}` : base;
  }

  function measureHref(measure) {
    const base = (measure.detail_base_url || '/measures/').replace(/\/?$/, '/');
    return `${base}${measure.slug}/`;
  }

  function showPreviousMeasure() {
    if (count < 2) return;
    slideIndex = (safeIndex - 1 + count) % count;
  }

  function showNextMeasure() {
    if (count < 2) return;
    slideIndex = (safeIndex + 1) % count;
  }
</script>

<div
  class="flex flex-col h-full measure-group-card"
  data-measure-group={group.slug}
  data-group-count={count}
>
  <div class="relative h-full">
    <div class="rounded-xl shadow-sm hover:shadow-md transition-all duration-300 flex flex-col h-full border border-gray-100 {cardClass || 'bg-white'}">
      {#if anyNew}
        <div class="absolute -top-3 -right-3 z-10">
          <span class="inline-flex items-center justify-center text-4xl" aria-label="New measure">🆕</span>
        </div>
      {/if}
      <div class="{cardHeaderClass} flex items-center rounded-t-xl relative min-h-[5rem]">
        {#if statusBadge}
          <div class="absolute top-0 left-0 right-0 flex items-center justify-center py-1 text-xs font-medium rounded-t-xl {statusBadgeClass}">
            {statusBadge}
          </div>
          <div class="w-full mt-6">
            <h3 class="text-xl font-semibold text-gray-900 line-clamp-2 leading-tight">
              {group.name}
            </h3>
            <p class="text-sm text-gray-500 mt-1">{countLabel}</p>
          </div>
        {:else}
          <div class="w-full">
            <h3 class="text-xl font-semibold text-gray-900 line-clamp-2 leading-tight">
              {group.name}
            </h3>
            <p class="text-sm text-gray-500 mt-1">{countLabel}</p>
          </div>
        {/if}
      </div>
      {#if tags.length > 0}
        <div class="px-4 pt-4">
          <div class="inline-block">
            <span class="text-sm font-medium text-gray-500 mr-2">Tags:</span>
            {#each tags as tag (tag.slug || tag.name)}
              <span
                class="inline-flex items-center text-sm font-normal px-3 py-1 rounded-full mb-2 mr-2"
                style="background-color: {(tag.colour || '#6b7280')}20; color: {tag.colour || '#6b7280'};"
              >
                <span class="w-2 h-2 rounded-full mr-2" style="background-color: {tag.colour || '#6b7280'}"></span>
                {tag.name}
              </span>
            {/each}
          </div>
        </div>
      {/if}
      {#if currentMeasure}
        <div class="px-4 pt-4 text-gray-600 flex-grow">
          <div class="mb-2 flex items-center gap-2">
            <button
              type="button"
              class="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-gray-200 bg-white text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-40"
              aria-label="Previous measure in this group"
              onclick={showPreviousMeasure}
              disabled={count < 2}
            >
              <svg class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                <path fill-rule="evenodd" d="M12.79 5.23a.75.75 0 010 1.06L9.06 10l3.73 3.71a.75.75 0 11-1.06 1.06l-4.25-4.25a.75.75 0 010-1.06l4.25-4.25a.75.75 0 011.06 0z" clip-rule="evenodd" />
              </svg>
            </button>
            <div class="min-w-0 flex-1 text-center" aria-live="polite">
              <a
                href={measureHref(currentMeasure)}
                class="block text-sm font-medium text-gray-900 hover:text-oxford-700 line-clamp-2"
              >
                {currentMeasure.short_name}
              </a>
              <p class="text-xs text-gray-500">
                {safeIndex + 1} of {count}
                {#if currentMeasure.status === 'archived'}
                  <span class="ml-1 font-medium">Archived</span>
                {/if}
              </p>
            </div>
            <button
              type="button"
              class="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-gray-200 bg-white text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-40"
              aria-label="Next measure in this group"
              onclick={showNextMeasure}
              disabled={count < 2}
            >
              <svg class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
                <path fill-rule="evenodd" d="M7.21 14.77a.75.75 0 010-1.06L10.94 10 7.21 6.29a.75.75 0 111.06-1.06l4.25 4.25a.75.75 0 010 1.06l-4.25 4.25a.75.75 0 01-1.06 0z" clip-rule="evenodd" />
              </svg>
            </button>
          </div>
          {#key currentMeasure.slug}
            {#if currentMeasure.has_chart_data !== false}
              <div class="relative h-[280px] w-full overflow-hidden rounded-lg border border-gray-200">
                <div class="absolute inset-2">
                  <MeasureMiniChart
                    slug={currentMeasure.slug}
                    chartdata={'{}'}
                    mode={$mode}
                    chartkind={currentMeasure.chart_kind || (currentMeasure.has_denominators ? 'percentage' : 'absolute')}
                    quantitytype={currentMeasure.quantity_type || ''}
                  />
                </div>
              </div>
            {:else}
              <div class="flex w-full items-center justify-center rounded-lg border border-gray-200 text-sm text-gray-400" style="height: 280px;">
                No data
              </div>
            {/if}
          {/key}
          {#if trustSelected}
            <div class="mt-2 flex h-4 items-center" data-trust-included={trustIncluded ? 'true' : 'false'}>
              {#if !trustIncluded}
                <span class="text-xs font-medium text-gray-700">Selected trust is not included in this measure</span>
              {/if}
            </div>
          {/if}
        </div>
      {/if}
      <div class="p-6 pt-2">
        <a
          href={groupHref}
          class="inline-flex w-full justify-center items-center px-4 py-2 {linkClasses} rounded-lg transition-colors duration-200 font-medium"
        >
          View measures
        </a>
      </div>
    </div>
  </div>
</div>
