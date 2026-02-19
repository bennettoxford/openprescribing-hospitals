import { writable, derived } from 'svelte/store';

export const mode = writable('trust');
export const selectedCode = writable('');
export const sort = writable('name');
export const selectedTags = writable([]);
export const chartData = writable({});
const modesBySlug = writable({});
export const isLoadingCharts = writable(false);

export const detailLinkQuery = derived(
  [mode, selectedCode],
  ([$mode, $code]) => {
    const params = new URLSearchParams();
    if ($mode === 'trust') {
      params.set('mode', 'trust');
      if ($code) params.set('trusts', $code);
    } else if ($mode === 'region') {
      params.set('mode', 'region');
      if ($code) params.set('regions', $code);
    } else if ($mode === 'national') {
      params.set('mode', 'national');
    }
    const qs = params.toString();
    return qs ? `?${qs}` : '';
  }
);

export function setMode(m) { mode.set(m); selectedCode.set(''); }
export function setSelectedCode(c) { selectedCode.set(c); }
export function setSort(s) { sort.set(s); }
export function setSelectedTags(t) { selectedTags.set(Array.isArray(t) ? t : []); }
export function setChartData(data, modes) {
  chartData.set(data || {});
  modesBySlug.set(modes || {});
}
export function setLoadingCharts(v) { isLoadingCharts.set(v); }

export function getChartForSlug(slug) {
  return derived([chartData, modesBySlug, mode], ([$d, $m, $mode]) => ({
    data: $d[slug],
    mode: $m[slug] ?? $mode
  }));
}
