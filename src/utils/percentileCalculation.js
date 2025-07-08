/**
 * Calculate percentiles from filtered data
 */
export function calculatePercentiles(filteredData, predecessorMap = null) {
    if (!Array.isArray(filteredData) || filteredData.length === 0) {
        return { percentiles: [], trustCount: 0, excludedTrusts: [] };
    }

    const orgTotalUsage = {};
    const orgNames = {};
    const allMonths = new Set();

    filteredData.forEach(item => {
        const orgId = item.organisation__ods_code;
        const orgName = item.organisation__ods_name;
        
        if (!orgId) {
            return;
        }

        if (orgName) {
            orgNames[orgId] = orgName;
        }
        
        if (!orgTotalUsage[orgId]) {
            orgTotalUsage[orgId] = 0;
        }

        if (item.data && Array.isArray(item.data)) {
            item.data.forEach(entry => {
                if (entry && entry.length >= 2 && entry[1] !== null && entry[1] !== undefined) {
                    const month = entry[0];
                    const quantity = parseFloat(entry[1]);
                    
                    if (!isNaN(quantity)) {
                        orgTotalUsage[orgId] += quantity;
                        allMonths.add(month);
                    }
                }
            });
        }
    });

    const includedOrgs = Object.keys(orgTotalUsage);
    
    // Count additional trusts that should be included due to predecessor relationships
    let additionalIncludedCount = 0;
    
    if (predecessorMap instanceof Map) {
        for (const [successor, predecessors] of predecessorMap.entries()) {
            const successorHasData = Object.keys(orgTotalUsage).some(orgId => 
                orgNames[orgId] === successor
            );
            
            if (successorHasData && Array.isArray(predecessors)) {
                additionalIncludedCount += predecessors.length;
            }
        }
    }

    const totalIncludedCount = includedOrgs.length + additionalIncludedCount;

    if (includedOrgs.length === 0) {
        return { 
            percentiles: [], 
            trustCount: totalIncludedCount, 
            excludedTrusts: []
        };
    }

    const orgMonthlyValues = {};
    const sortedMonths = Array.from(allMonths).sort();

    includedOrgs.forEach(orgId => {
        orgMonthlyValues[orgId] = {};
        sortedMonths.forEach(month => {
            orgMonthlyValues[orgId][month] = 0; // Set missing months to 0 - this is in line with the percentiles calculation in measures
        });
    });

    filteredData.forEach(item => {
        const orgId = item.organisation__ods_code;
        
        if (orgId && includedOrgs.includes(orgId)) {
            if (item.data && Array.isArray(item.data)) {
                item.data.forEach(entry => {
                    if (entry && entry.length >= 2 && entry[1] !== null && entry[1] !== undefined) {
                        const month = entry[0];
                        const quantity = parseFloat(entry[1]);
                        
                        if (!isNaN(quantity)) {
                            orgMonthlyValues[orgId][month] = quantity;
                        }
                    }
                });
            }
        }
    });

    const percentilesData = [];
    
    sortedMonths.forEach(month => {
        const monthValues = includedOrgs.map(orgId => orgMonthlyValues[orgId][month]).sort((a, b) => a - b);
        const n = monthValues.length;

        if (n > 0) {
            [5, 15, 25, 35, 45, 50, 55, 65, 75, 85, 95].forEach(percentile => {
                const k = (n - 1) * (percentile / 100);
                const f = Math.floor(k);
                const c = Math.ceil(k);

                let value;
                if (f === c) {
                    value = monthValues[f];
                } else {
                    const d0 = monthValues[f] * (c - k);
                    const d1 = monthValues[c] * (k - f);
                    value = d0 + d1;
                }

                percentilesData.push({
                    month,
                    percentile,
                    quantity: value
                });
            });
        }
    });

    return {
        percentiles: percentilesData,
        trustCount: totalIncludedCount,
        excludedTrusts: []
    };
} 