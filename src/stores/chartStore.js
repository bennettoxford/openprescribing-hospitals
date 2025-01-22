import { writable } from 'svelte/store';

export function createChartStore(initialConfig = {}) {
    const defaultConfig = {
        data: { labels: [], datasets: [] },
        zoomState: {
            xDomain: null,
            yDomain: null,
            isZoomedIn: false
        },
        dimensions: {
            height: 400,
            margin: { top: 10, right: 20, bottom: 30, left: 50 }
        },
        config: {
            mode: 'percentiles',
            yAxisLabel: '%',
            yAxisLimits: [0, 100],
            percentileConfig: {
                medianColor: '#DC3220',
                rangeColor: 'rgb(0, 90, 181)'
            },
            visibleItems: new Set()
        }
    };

    const mergedConfig = {
        ...defaultConfig,
        config: {
            ...defaultConfig.config,
            ...initialConfig
        }
    };

    const { subscribe, set, update } = writable(mergedConfig);

    const store = {
        subscribe,
        setData: (newData) => {
            update(state => ({ ...state, data: newData }));
        },
        updateZoom: (xDomain, yDomain) => {
            update(state => ({
                ...state,
                zoomState: {
                    xDomain,
                    yDomain,
                    isZoomedIn: true
                }
            }));
        },
        resetZoom: () => {
            update(state => ({
                ...state,
                zoomState: {
                    xDomain: null,
                    yDomain: null,
                    isZoomedIn: false
                }
            }));
        },
        setDimensions: (dimensions) => {
            update(state => ({ ...state, dimensions }));
        },
        setConfig: (config) => {
            update(state => ({ ...state, config }));
        },
        updateVisibleItems: (items) => {
            update(state => {
                const updatedData = {
                    ...state.data,
                    datasets: state.data.datasets.map(dataset => ({
                        ...dataset,
                        visible: items.has(dataset.label)
                    }))
                };
                return {
                    ...state,
                    data: updatedData
                };
            });
        }
    };

    return store;
}

export const chartStore = createChartStore({ mode: 'national' });
