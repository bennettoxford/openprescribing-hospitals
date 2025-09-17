<script>
    import { onMount } from 'svelte';
    import * as Highcharts from 'highcharts';
    import { Chart } from '@highcharts/svelte';
    import Accessibility from 'highcharts/modules/accessibility';
    import { organisationSearchStore } from '../../stores/organisationSearchStore.js';

    export let org;
    export let latestDates;
    export let months;

    const DATA_SUBMITTED_COLOR = "#005AB5";
    const DATA_NOT_SUBMITTED_COLOR = "#DC3220";

    let showPredecessors = false;

    function parseDate(dateStr) {
        if (!dateStr) return null;
        const [month, year] = dateStr.split(' ');
        const date = new Date(`${month} 1, ${year}`);
        date.setMonth(date.getMonth() + 1);
        date.setDate(0);
        return date;
    }

    function getChartOptions(org) {
        if (!org?.data || !months?.length) {
            console.error('Invalid org data or months:', { org, months });
            return null;
        }

        const maxVmpCount = Math.max(...months.map(month => {
            const monthData = org.data[month];
            return monthData?.vmp_count || 0;
        }));
        
        const submittedData = [];
        const missingData = [];
        
        months.forEach(month => {
            const timestamp = new Date(month).getTime();
            const monthData = org.data[month] || { has_submitted: false, vmp_count: 0 };
            
            if (monthData.has_submitted) {
                submittedData.push({
                    x: timestamp,
                    y: monthData.vmp_count || 0,
                    hasSubmitted: true,
                    date: month
                });
                missingData.push({ x: timestamp, y: null });
            } else {
                submittedData.push({ x: timestamp, y: null });
                missingData.push({ 
                    x: timestamp, 
                    y: maxVmpCount * 1.1,
                    hasSubmitted: false,
                    date: month 
                });
            }
        });

        const finalDate = parseDate(latestDates.final);

        return {
            chart: {
                type: 'column',
                height: 200,
                spacingBottom: 0,
                spacingTop: 10,
                marginTop: 30,
                animation: false
            },
            accessibility: {
                enabled: true,
                description: `Bar chart showing submission history for ${organisationSearchStore.getDisplayName(org.name)}`
            },
            title: {
                text: organisationSearchStore.getDisplayName(org.name),
                align: 'left',
                style: {
                    fontSize: '14px',
                    fontWeight: '600',
                    color: '#4B5563'
                },
                margin: 5
            },
            xAxis: {
                type: 'datetime',
                labels: {
                    format: '{value:%b %Y}',
                    style: { fontSize: '12px' }
                }
            },
            yAxis: {
                title: { text: 'Unique Products' },
                min: 0,
                max: maxVmpCount * 1.1,
                labels: { style: { fontSize: '12px' } },
                endOnTick: false,
                gridLineWidth: 1
            },
            tooltip: {
                useHTML: true,
                formatter: function() {
                    const point = this.point;
                    const isProvisional = finalDate && new Date(point.date) >= finalDate;
                    
                    if (!point.hasSubmitted && this.series.name === 'No Submission') {
                        return `
                            ${isProvisional ? '<div class="tooltip-row text-amber-600 font-bold">⚠️ Provisional Data</div>' : ''}
                            <div class="font-medium">${Highcharts.dateFormat('%b %Y', point.x)}</div>
                            <div class="tooltip-row">
                                <span class="text-red-600">No submission</span>
                            </div>
                        `;
                    }
                    
                    return `
                        ${isProvisional ? '<div class="tooltip-row text-amber-600 font-bold">⚠️ Provisional Data</div>' : ''}
                        <div class="font-medium">${Highcharts.dateFormat('%b %Y', point.x)}</div>
                        <div class="tooltip-row">Products: ${point.y}</div>
                    `;
                }
            },
            responsive: {
                rules: [{
                    condition: { maxWidth: 500 },
                    chartOptions: {
                        chart: {
                            spacingRight: 10,
                            spacingLeft: 10
                        },
                        plotOptions: {
                            column: { pointWidth: 2 }
                        },
                        title: {
                            style: { fontSize: '12px' }
                        },
                        xAxis: {
                            labels: {
                                style: { fontSize: '10px' }
                            }
                        },
                        yAxis: {
                            labels: {
                                style: { fontSize: '10px' }
                            }
                        }
                    }
                }, {
                    condition: {
                        minWidth: 501,
                        maxWidth: 768
                    },
                    chartOptions: {
                        plotOptions: {
                            column: { pointWidth: 3 }
                        },
                        title: {
                            style: { fontSize: '13px' }
                        }
                    }
                }, {
                    condition: { minWidth: 769 },
                    chartOptions: {
                        plotOptions: {
                            column: { pointWidth: 8 }
                        }
                    }
                }]
            },
            plotOptions: {
                column: {
                    animation: false,
                    grouping: false,
                    pointPadding: 0,
                    groupPadding: 0,
                    borderWidth: 0,
                    pointWidth: 8
                },
                series: {
                    animation: false,
                    states: {
                        inactive: {
                            opacity: 1
                        }
                    }
                }
            },
            series: [
                {
                    name: 'Products',
                    data: submittedData.map(point => ({
                        ...point,
                        color: finalDate && new Date(point.date) >= finalDate 
                            ? '#88A4BC'  // Lighter blue for provisional
                            : DATA_SUBMITTED_COLOR
                    })),
                    zIndex: 2
                },
                {
                    name: 'No Submission',
                    data: missingData.map(point => ({
                        ...point,
                        color: finalDate && new Date(point.date) >= finalDate
                            ? 'rgba(220, 50, 32, 0.15)'  // Lighter red for provisional
                            : DATA_NOT_SUBMITTED_COLOR
                    })),
                    zIndex: 1
                }
            ],
            credits: { enabled: false },
            legend: { enabled: false }
        };
    }

    let chartInitialized = false;

    onMount(() => {
        if (Highcharts.Accessibility) {
            Highcharts.Accessibility(Highcharts);
        }
        chartInitialized = true;
    });
</script>

<div class="bg-white rounded-lg shadow-sm p-4 mb-6">
    {#if chartInitialized && org?.data && months?.length}
        <Chart options={getChartOptions(org)} />
        
        {#if org.predecessors?.length}
            <button 
                on:click={() => showPredecessors = !showPredecessors}
                class="mt-2 text-sm font-medium text-gray-600 hover:text-gray-900 flex items-center gap-1"
            >
                <svg 
                    class="w-4 h-4 transform transition-transform {showPredecessors ? 'rotate-90' : ''}" 
                    fill="none" 
                    stroke="currentColor" 
                    viewBox="0 0 24 24"
                >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
                {showPredecessors ? 'Hide' : 'Show'} predecessor organisations ({org.predecessors.length})
            </button>

            {#if showPredecessors}
                {#each org.predecessors as predecessor}
                    {#if predecessor?.data}
                        <div class="ml-6 mt-4">
                            <Chart options={getChartOptions(predecessor)} />
                        </div>
                    {/if}
                {/each}
            {/if}
        {/if}
    {:else if !chartInitialized}
        <div class="h-[200px] flex items-center justify-center">
            <div class="text-gray-500">Initializing chart...</div>
        </div>
    {:else}
        <div class="p-4 text-gray-500">
            No data available for this organisation
        </div>
    {/if}
</div>

<style>
    :global(.tooltip-row) {
        line-height: 1.1;
        margin-bottom: 1px;
    }
</style>