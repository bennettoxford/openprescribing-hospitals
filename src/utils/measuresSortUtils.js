
/**
 * Map a raw value to its percentile rank using percentile quantities.
 */
function valueToPercentileRank(value, percentileQuantities) {
  if (value == null || !percentileQuantities || Object.keys(percentileQuantities).length === 0) {
    return null;
  }
  const sorted = Object.entries(percentileQuantities)
    .map(([p, q]) => [Number(p), q])
    .sort((a, b) => a[1] - b[1] || a[0] - b[0]);
  const quantities = sorted.map(([, q]) => q);
  if (value <= quantities[0]) return sorted[0][0];
  if (value >= quantities[quantities.length - 1]) return sorted[sorted.length - 1][0];
  for (let i = 0; i < quantities.length - 1; i++) {
    if (quantities[i] <= value && value <= quantities[i + 1]) {
      const [pLo, qLo] = sorted[i];
      const [pHi, qHi] = sorted[i + 1];
      if (qHi === qLo) return pLo;
      return pLo + (pHi - pLo) * (value - qLo) / (qHi - qLo);
    }
  }
  return null;
}

/**
 * Build percentile quantities for a given month from chart percentiles.
 */
function getPercentileQuantitiesForMonth(percentiles, monthKey) {
  if (!percentiles || typeof percentiles !== 'object') return null;
  const result = {};
  for (const [pctStr, series] of Object.entries(percentiles)) {
    if (!Array.isArray(series)) continue;
    const entry = series.find(([d]) => String(d).startsWith(monthKey) || d === monthKey);
    if (entry) result[Number(pctStr)] = entry[1];
  }
  return Object.keys(result).length > 0 ? result : null;
}

/**
 * Get trust value for a month from trustData
 */
function getTrustValueForMonth(trustData, monthKey) {
  if (!Array.isArray(trustData)) return null;
  const entry = trustData.find(([d]) => String(d).startsWith(monthKey) || d === monthKey);
  return entry != null ? entry[1] : null;
}

/**
 * Extract sorted month keys (newest first) from chart data. Returns up to 12 months.
 */
function getLast12MonthsFromChartData(chartDataBySlug) {
  const months = new Set();
  for (const data of Object.values(chartDataBySlug || {})) {
    if (data?.trustData && Array.isArray(data.trustData)) {
      for (const [d] of data.trustData) {
        const key = String(d).slice(0, 7);
        months.add(key);
      }
    }
  }
  return [...months].sort().reverse().slice(0, 12);
}

/**
 * Compute average of non-null values in an array.
 */
function average(values) {
  const valid = values.filter((v) => v != null);
  if (valid.length === 0) return null;
  return valid.reduce((a, b) => a + b, 0) / valid.length;
}

/**
 * Derive sort metrics from chart data.
 */
export function deriveSortMetricsFromChartData(chartDataBySlug, measures) {
  if (!chartDataBySlug || typeof chartDataBySlug !== 'object') return {};
  const measuresBySlug = Array.isArray(measures)
    ? Object.fromEntries(measures.map((m) => [m.slug, m]))
    : measures;

  const monthKeys = getLast12MonthsFromChartData(chartDataBySlug);
  if (monthKeys.length < 6) return {};

  const result = {};
  for (const slug of Object.keys(chartDataBySlug)) {
    const data = chartDataBySlug[slug];
    if (!data?.percentiles || !data?.trustData) continue;

    const measure = measuresBySlug?.[slug];
    const lowerIsBetter = measure?.lower_is_better;
    if (lowerIsBetter == null) {
      result[slug] = { potential_improvement: null, most_improved: null };
      continue;
    }

    const ranks = [];
    for (const monthKey of monthKeys) {
      const pctQuantities = getPercentileQuantitiesForMonth(data.percentiles, monthKey);
      const value = getTrustValueForMonth(data.trustData, monthKey);
      const rank = valueToPercentileRank(value, pctQuantities);
      ranks.push(rank);
    }

    const validRanks = ranks.filter((r) => r != null);
    if (validRanks.length === 0) {
      result[slug] = { potential_improvement: null, most_improved: null };
      continue;
    }

    const avgLast3 = average(ranks.slice(0, 3));
    const avgFirst3 = average(ranks.slice(-3));

    let potential;
    let mostImproved;
    if (lowerIsBetter) {
      potential = Math.max(...validRanks);
      mostImproved = avgFirst3 != null && avgLast3 != null ? avgFirst3 - avgLast3 : null;
    } else {
      potential = -Math.min(...validRanks);
      mostImproved = avgFirst3 != null && avgLast3 != null ? avgLast3 - avgFirst3 : null;
    }

    result[slug] = { potential_improvement: potential, most_improved: mostImproved };
  }
  return result;
}
