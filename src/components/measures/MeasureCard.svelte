<script>
  import MeasureMiniChart from './MeasureMiniChart.svelte';
  import { mode, detailLinkQuery } from '../../stores/measuresListStore.js';

  export let measure;
  export let section = 'published';
  export let cardHeaderClass = 'py-2 px-4 border-b border-gray-100';
  export let statusBadge = '';
  export let statusBadgeClass = '';
  export let linkClasses = 'bg-oxford-50 text-oxford-600 hover:bg-oxford-100';
  export let linkText = 'View measure details';

  $: detailHref = measure.detail_base_url + measure.slug + '/' + ($detailLinkQuery || '');
  $: initialChartData = '{}';
</script>

<div class="flex flex-col h-full measure-card" data-measure-slug={measure.slug} data-tag-slugs={measure.tag_slugs}>
  <div class="relative h-full">
    <div class="bg-white rounded-xl shadow-sm hover:shadow-md transition-all duration-300 flex flex-col h-full border border-gray-100">
      {#if measure.is_new}
        <div class="absolute -top-3 -right-3 z-10">
          <span class="inline-flex items-center justify-center text-4xl" aria-label="New measure">🆕</span>
        </div>
      {/if}
      <div class="{cardHeaderClass} flex items-center rounded-t-xl relative h-[5rem]">
        {#if statusBadge}
          <div class="absolute top-0 left-0 right-0 flex items-center justify-center py-1 text-xs font-medium {statusBadgeClass}">
            {statusBadge}
          </div>
          <h3 class="text-xl font-semibold text-gray-900 line-clamp-2 leading-tight w-full mt-6">
            {measure.short_name}
          </h3>
        {:else}
          <h3 class="text-xl font-semibold text-gray-900 line-clamp-2 leading-tight w-full">
            {measure.short_name}
          </h3>
        {/if}
      </div>
      {#if measure.tags && measure.tags.length > 0}
        <div class="px-4 pt-4">
          <div class="inline-block">
            <span class="text-sm font-medium text-gray-500 mr-2">Tags:</span>
            {#each measure.tags as tag}
              <span
                class="inline-flex items-center text-sm font-normal px-3 py-1 rounded-full cursor-help relative group mb-2 mr-2"
                style="background-color: {(tag.colour || '#6b7280')}20; color: {tag.colour || '#6b7280'};"
              >
                <span class="w-2 h-2 rounded-full mr-2" style="background-color: {tag.colour || '#6b7280'}"></span>
                {tag.name}
                {#if tag.description}
                  <div
                    class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 p-4 bg-white rounded-lg shadow-lg invisible opacity-0 group-hover:visible group-hover:opacity-100 transition-all duration-300 text-sm text-gray-600 z-10"
                  >
                    {tag.description}
                    <div class="absolute -bottom-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-white transform rotate-45"></div>
                  </div>
                {/if}
              </span>
            {/each}
          </div>
        </div>
      {/if}
      <div class="p-4 text-gray-600 flex-grow">
        <div class="prose prose-sm max-w-none line-clamp-3">{measure.description || ''}</div>
      </div>
      <div class="px-4 pb-4">
        {#if measure.has_chart_data !== false}
          <div class="w-full rounded-lg border border-gray-200 overflow-visible p-2" style="height: 280px;">
            <MeasureMiniChart
              slug={measure.slug}
              chartdata={initialChartData}
              mode={$mode === 'default' ? 'national' : $mode}
              ispercentage={measure.has_denominators ? 'true' : 'false'}
              quantitytype={measure.quantity_type || ''}
            />
          </div>
        {:else}
          <div class="w-full rounded-lg border border-gray-200 overflow-hidden" style="height: 280px;">
            <div class="w-full h-full flex items-center justify-center text-gray-400 text-sm">No data</div>
          </div>
        {/if}
      </div>
      <div class="p-6 pt-2 flex flex-col gap-2">
        <a
          href={detailHref}
          class="inline-flex w-full justify-center items-center px-4 py-2 {linkClasses} rounded-lg transition-colors duration-200 font-medium"
        >
          {linkText}
        </a>
      </div>
    </div>
  </div>
</div>
