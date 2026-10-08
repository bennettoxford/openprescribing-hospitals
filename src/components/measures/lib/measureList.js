const MIN_GROUP_SIZE = 2;

export function measureMatchesSearch(measure, query) {
  const q = (query || '').trim().toLowerCase();
  if (!q) return true;
  const tagNames = (measure.tags || []).map((tag) => tag.name || '');
  const haystack = [
    measure.short_name,
    measure.name,
    measure.description,
    measure.measure_group,
    ...tagNames,
  ].join('\n').toLowerCase();
  return haystack.includes(q);
}

function compareNames(a, b) {
  return (a || '').localeCompare(b || '', undefined, { sensitivity: 'base' });
}

function cardName(item) {
  if (item.type === 'group') return item.group.name || '';
  return item.measure.short_name || '';
}

function cardNewest(item) {
  const dates = item.type === 'group'
    ? item.group.measures.map((measure) => measure.first_published)
    : [item.measure.first_published];
  return Math.max(0, ...dates.map((value) => (value ? new Date(value).getTime() : 0)));
}

export function buildMeasureCards(measureList, { collapseGroups = true, query = '' } = {}) {
  const matched = (measureList || []).filter((measure) => measureMatchesSearch(measure, query));
  const searching = !!(query && query.trim());
  if (!collapseGroups || searching) {
    return matched.map((measure) => ({
      type: 'measure',
      key: measure.slug,
      measure,
    }));
  }

  const grouped = new Map();
  const items = [];
  for (const measure of matched) {
    const groupSlug = (measure.measure_group_slug || '').trim();
    const groupName = (measure.measure_group || '').trim();
    if (!groupSlug || !groupName) {
      items.push({ type: 'measure', key: measure.slug, measure });
      continue;
    }
    const groupKey = `${groupSlug}:${measure.status || ''}`;
    let item = grouped.get(groupKey);
    if (!item) {
      item = {
        type: 'group',
        key: `group:${groupKey}`,
        group: {
          slug: groupSlug,
          name: groupName,
          status: measure.status || '',
          measures: [],
        },
      };
      grouped.set(groupKey, item);
      items.push(item);
    }
    item.group.measures.push(measure);
  }

  for (const item of items) {
    if (item.type === 'group') {
      item.group.measures.sort((a, b) => compareNames(a.short_name, b.short_name));
    }
  }

  return items.flatMap((item) => {
    if (item.type === 'group' && item.group.measures.length < MIN_GROUP_SIZE) {
      const measure = item.group.measures[0];
      return [{ type: 'measure', key: measure.slug, measure }];
    }
    return [item];
  });
}

export function countMeasuresInCards(cards) {
  return cards.reduce((total, item) => {
    if (item.type === 'group') return total + item.group.measures.length;
    return total + 1;
  }, 0);
}

export function measureGroupOptions(measureLists) {
  const seen = new Map();
  for (const list of measureLists || []) {
    for (const measure of list || []) {
      const slug = (measure.measure_group_slug || '').trim();
      const name = (measure.measure_group || '').trim();
      if (!slug || !name || seen.has(slug)) continue;
      seen.set(slug, { slug, name });
    }
  }
  return [...seen.values()].sort((a, b) => compareNames(a.name, b.name));
}

export function filterMeasuresByGroups(measureList, groupSlugs) {
  const selected = (groupSlugs || []).map((slug) => slug.trim()).filter(Boolean);
  if (!selected.length) return measureList;
  const allowed = new Set(selected);
  return (measureList || []).filter((measure) => allowed.has((measure.measure_group_slug || '').trim()));
}

export function measureListTitle({
  query = '',
  tagNames = [],
  groupNames = [],
  showArchived = 'off',
  groupName = '',
  previewMode = false,
  includeInDevelopment = false,
} = {}) {
  const q = (query || '').trim();
  const tags = (tagNames || []).map((name) => name.trim()).filter(Boolean);
  const groups = (groupNames || []).map((name) => name.trim()).filter(Boolean);
  const tagLabel = tags.join(', ');
  const groupLabel = groups.join(', ');
  const archived = showArchived === 'only' || showArchived === 'include' ? showArchived : 'off';
  const pageGroup = (groupName || '').trim();
  const previewLabel = includeInDevelopment
    ? 'All preview/in development measures'
    : 'All preview measures';
  const defaultAllLabel = previewMode ? previewLabel : 'All measures';
  const allLabel = pageGroup || defaultAllLabel;

  if (!q && !tagLabel && !groupLabel) {
    if (archived === 'include') return `${allLabel}, including archived`;
    if (archived === 'only') return 'Archived measures';
    return allLabel;
  }

  let title = 'Measures';
  if (q && archived === 'only') title = `Archived results for "${q}"`;
  else if (q) title = `Results for "${q}"`;
  else if (archived === 'only') title = 'Archived measures';

  if (tagLabel) title += ` tagged ${tagLabel}`;
  if (groupLabel) title += ` in ${groupLabel}`;
  if (archived === 'include') title += ', including archived';
  return title;
}

export function sortMeasureCards(cards, sort) {
  const copy = [...(cards || [])];
  if (sort === 'newest') {
    copy.sort((a, b) => cardNewest(b) - cardNewest(a));
    return copy;
  }
  copy.sort((a, b) => compareNames(cardName(a), cardName(b)));
  return copy;
}
