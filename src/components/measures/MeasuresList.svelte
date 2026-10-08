<svelte:options customElement={{
  tag: 'measures-list',
  props: {
    measures: { type: 'String', reflect: true },
    previewMeasures: { type: 'String', reflect: true },
    inDevelopmentMeasures: { type: 'String', reflect: true },
    archivedMeasures: { type: 'String', reflect: true },
    chartData: { type: 'String', reflect: true },
    previewMode: { type: 'String', reflect: true },
    measureTrustsBasePath: { type: 'String', reflect: true },
    orgData: { type: 'String', reflect: true },
    regionData: { type: 'String', reflect: true },
    tagsData: { type: 'String', reflect: true },
    initialMode: { type: 'String', reflect: true },
    initialCode: { type: 'String', reflect: true },
    initialSort: { type: 'String', reflect: true },
    initialTags: { type: 'String', reflect: true },
    initialQuery: { type: 'String', reflect: true },
    collapseGroups: { type: 'String', reflect: true },
    groupName: { type: 'String', reflect: true },
    groupBasePath: { type: 'String', reflect: true },
  },
  shadow: 'none'
}} />

<script>
  import MeasuresListControls from './MeasuresListControls.svelte';
  import MeasureCard from './MeasureCard.svelte';
  import MeasureGroupCard from './MeasureGroupCard.svelte';
  import {
    selectedTags, selectedGroups, sort, mode, selectedCode, showArchived, searchQuery,
    setSearchQuery, setSelectedGroups
  } from '../../stores/measuresListStore.js';
  import {
    buildMeasureCards, countMeasuresInCards, filterMeasuresByGroups, measureGroupOptions,
    measureListTitle, sortMeasureCards
  } from './lib/measureList.js';

  export let measures = '[]';
  export let previewMeasures = '[]';
  export let inDevelopmentMeasures = '[]';
  export let archivedMeasures = '[]';
  export let chartData = '{}';
  export let previewMode = 'false';
  export let measureTrustsBasePath = '/measures/';
  export let orgData = '{}';
  export let regionData = '[]';
  export let tagsData = '[]';
  export let initialMode = 'trust';
  export let initialCode = '';
  export let initialSort = 'name';
  export let initialTags = '';
  export let initialQuery = '';
  export let collapseGroups = 'true';
  export let groupName = '';
  export let groupBasePath = '/measures/group/';

  function seedSearchQuery() {
    if (typeof window !== 'undefined') {
      const urlQuery = new URLSearchParams(window.location.search).get('q');
      if (urlQuery !== null) {
        setSearchQuery(urlQuery);
        return;
      }
    }
    setSearchQuery(initialQuery || '');
  }
  seedSearchQuery();

  function parseListParam(value) {
    return (value || '').split(',').map((item) => item.trim()).filter(Boolean);
  }

  function seedSelectedGroups() {
    if (typeof window !== 'undefined') {
      const urlGroups = new URLSearchParams(window.location.search).get('groups');
      if (urlGroups !== null) {
        setSelectedGroups(parseListParam(urlGroups));
        return;
      }
    }
    setSelectedGroups([]);
  }
  seedSelectedGroups();

  $: parsedMeasures = (() => { try { return JSON.parse(measures || '[]'); } catch (e) { return []; } })();
  $: parsedPreviewMeasures = (() => { try { return JSON.parse(previewMeasures || '[]'); } catch (e) { return []; } })();
  $: parsedInDevelopmentMeasures = (() => { try { return JSON.parse(inDevelopmentMeasures || '[]'); } catch (e) { return []; } })();
  $: parsedArchivedMeasures = (() => { try { return JSON.parse(archivedMeasures || '[]'); } catch (e) { return []; } })();

  $: selectedTagsVal = $selectedTags;
  $: selectedGroupsVal = $selectedGroups;
  $: sortVal = $sort;
  $: searchQueryVal = $searchQuery;
  $: shouldCollapseGroups = collapseGroups !== 'false';
  $: trustOverlayActive = $mode === 'trust' && !!$selectedCode;

  $: publishedWithArchived = $showArchived === 'only'
    ? parsedArchivedMeasures
    : $showArchived === 'include'
      ? [...parsedMeasures, ...parsedArchivedMeasures]
      : parsedMeasures;
  $: groupOptions = measureGroupOptions([
    parsedMeasures,
    parsedArchivedMeasures,
    parsedPreviewMeasures,
    parsedInDevelopmentMeasures,
  ]);
  $: sortedPublished = cardsFor(publishedWithArchived, selectedTagsVal, selectedGroupsVal, sortVal, searchQueryVal, shouldCollapseGroups);
  $: sortedPreview = cardsFor(parsedPreviewMeasures, selectedTagsVal, selectedGroupsVal, sortVal, searchQueryVal, shouldCollapseGroups);
  $: sortedInDevelopment = cardsFor(parsedInDevelopmentMeasures, selectedTagsVal, selectedGroupsVal, sortVal, searchQueryVal, shouldCollapseGroups);

  $: parsedTags = (() => { try { return JSON.parse(tagsData || '[]'); } catch (e) { return []; } })();
  $: selectedTagNames = selectedTagsVal.map((slug) => {
    const tag = parsedTags.find((item) => item.slug === slug);
    return tag?.name || slug;
  });
  $: selectedGroupNames = selectedGroupsVal.map((slug) => {
    const group = groupOptions.find((item) => item.slug === slug);
    return group?.name || slug;
  });
  $: listedCards = [
    ...(previewMode === 'true' ? [] : sortedPublished),
    ...sortedPreview,
    ...sortedInDevelopment,
  ];
  $: visibleMeasureCount = countMeasuresInCards(listedCards);
  $: listTitle = measureListTitle({
    query: searchQueryVal,
    tagNames: selectedTagNames,
    groupNames: selectedGroupNames,
    showArchived: $showArchived,
    groupName,
    previewMode: previewMode === 'true',
    includeInDevelopment: parsedInDevelopmentMeasures.length > 0,
  });

  $: hasActiveQuery = !!(searchQueryVal && searchQueryVal.trim());
  $: hasFiltersWithNoResults = (selectedTagsVal.length > 0 || selectedGroupsVal.length > 0 || hasActiveQuery) &&
    sortedPublished.length === 0 && sortedPreview.length === 0 && sortedInDevelopment.length === 0 &&
    (parsedMeasures.length > 0 || parsedPreviewMeasures.length > 0 || parsedInDevelopmentMeasures.length > 0 || parsedArchivedMeasures.length > 0);

  const defaultCardProps = {
    cardHeaderClass: 'py-2 px-4 border-b border-gray-100',
    statusBadge: '',
    statusBadgeClass: '',
    linkClasses: 'bg-oxford-50 text-oxford-600 hover:bg-oxford-100',
    linkText: 'View measure details',
    cardClass: '',
  };
  const archivedCardProps = {
    cardHeaderClass: 'bg-gray-50 py-2 px-4 border-b border-gray-200',
    statusBadge: 'Archived',
    statusBadgeClass: 'bg-gray-200 text-gray-700',
    linkClasses: 'bg-gray-100 text-gray-600 hover:bg-gray-200',
    linkText: 'View archived measure',
    cardClass: 'bg-gray-50',
  };
  const previewCardProps = {
    cardHeaderClass: 'bg-blue-50 py-2 px-4 border-b border-gray-100',
    statusBadge: 'Preview',
    statusBadgeClass: 'bg-blue-100 text-blue-800',
    linkClasses: 'bg-blue-50 text-blue-600 hover:bg-blue-100',
    linkText: 'View preview',
    cardClass: '',
  };
  const inDevelopmentCardProps = {
    cardHeaderClass: 'bg-amber-50 py-2 px-4 border-b border-gray-100',
    statusBadge: '🚧 In development',
    statusBadgeClass: 'bg-amber-100 text-amber-800',
    linkClasses: 'bg-amber-50 text-amber-600 hover:bg-amber-100',
    linkText: 'View in development',
    cardClass: '',
  };

  function cardsFor(measureList, tags, groups, sortMode, query, collapse) {
    const filtered = filterMeasuresByGroups(filterByTags(measureList, tags), groups);
    return sortMeasureCards(
      buildMeasureCards(filtered, { collapseGroups: collapse, query }),
      sortMode,
    );
  }

  function cardProps(item, section) {
    const status = item.type === 'group' ? item.group.status : item.measure.status;
    if (status === 'archived') return archivedCardProps;
    if (section === 'preview') return previewCardProps;
    if (section === 'in_development') return inDevelopmentCardProps;
    return defaultCardProps;
  }

  function filterByTags(measureList, tags) {
    if (!tags || !tags.length) return measureList;
    return measureList.filter((m) => {
      const slugs = (m.tag_slugs || '').split(',').map((s) => s.trim()).filter(Boolean);
      return tags.some((t) => slugs.includes(t));
    });
  }

</script>

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
    archivedCount={parsedArchivedMeasures.length}
    previewMode={previewMode}
    {initialQuery}
    {groupName}
    {collapseGroups}
    {groupOptions}
  />
</div>

<h2 class="mb-6 text-xl font-semibold text-gray-900" data-measure-count aria-live="polite">
  {listTitle}
  <span class="font-medium text-gray-500">({visibleMeasureCount})</span>
</h2>

{#snippet cardGrid(items, section)}
  {#each items as item (item.key)}
    {@const props = cardProps(item, section)}
    {#if item.type === 'group'}
      <MeasureGroupCard
        group={item.group}
        cardHeaderClass={props.cardHeaderClass}
        statusBadge={props.statusBadge}
        statusBadgeClass={props.statusBadgeClass}
        linkClasses={props.linkClasses}
        cardClass={props.cardClass}
        {groupBasePath}
      />
    {:else}
      <MeasureCard
        measure={item.measure}
        cardHeaderClass={props.cardHeaderClass}
        statusBadge={props.statusBadge}
        statusBadgeClass={props.statusBadgeClass}
        linkClasses={props.linkClasses}
        linkText={props.linkText}
        cardClass={props.cardClass}
        trustSelected={trustOverlayActive}
        {measureTrustsBasePath}
      />
    {/if}
  {/each}
{/snippet}

{#if hasFiltersWithNoResults}
  <div class="mb-8 p-6 bg-gray-50 border border-gray-200 rounded-lg text-center">
    <p class="text-gray-600">No measures match your search.</p>
    <p class="text-sm text-gray-500 mt-1">Try a different name, or clear the search, tags, and groups.</p>
  </div>
{:else}
{#if sortedPublished.length > 0 && previewMode !== 'true'}
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-8" data-measures-section="published">
    {@render cardGrid(sortedPublished, 'published')}
  </div>
{/if}

{#if sortedPreview.length > 0}
  <div class="mb-12">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8" data-measures-section="preview">
      {@render cardGrid(sortedPreview, 'preview')}
    </div>
  </div>
{/if}

{#if sortedInDevelopment.length > 0}
  <div class="mb-12">
    <h2 class="text-2xl font-semibold mb-6 text-gray-900">In Development</h2>
    <p class="text-gray-600 mb-6">These measures are currently in development. Public previews are not available for these measures. You can see them because you are logged in.</p>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8" data-measures-section="in_development">
      {@render cardGrid(sortedInDevelopment, 'in_development')}
    </div>
  </div>
{/if}
{/if}
