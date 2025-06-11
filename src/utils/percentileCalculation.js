/**
 * Calculate percentiles from filtered data
 * Removes organisations with zero usage across all periods, then fills missing months with zeros
 */
export function calculatePercentiles(filteredData) {
    if (!Array.isArray(filteredData) || filteredData.length === 0) {
        return { percentiles: [], trustCount: 0, excludedTrusts: [] };
    }

    // First pass: Calculate total usage per organisation to identify those with any usage
    const orgTotalUsage = {};
    const orgNames = {};
    const allMonths = new Set();

    const uniqueOrgs = new Set();

    filteredData.forEach(item => {
        const orgId = item.organisation__ods_code;
        const orgName = item.organisation__ods_name;
        
        if (!orgId) {
            return;
        }

        uniqueOrgs.add(orgId);
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

    const includedOrgs = [];
    const excludedTrusts = [];
    
    Object.keys(orgTotalUsage).forEach(orgId => {
        if (orgTotalUsage[orgId] > 0) {
            includedOrgs.push(orgId);
        } else {
            excludedTrusts.push({
                code: orgId,
                name: orgNames[orgId] || `Unknown (${orgId})`
            });
        }
    });

    if (includedOrgs.length === 0) {
        return { percentiles: [], trustCount: 0, excludedTrusts };
    }

    // Second pass: Collect monthly values for included organisations only
    const orgMonthlyValues = {};
    const sortedMonths = Array.from(allMonths).sort();

    includedOrgs.forEach(orgId => {
        orgMonthlyValues[orgId] = {};
        sortedMonths.forEach(month => {
            orgMonthlyValues[orgId][month] = 0; // Set missing months to 0
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
        trustCount: includedOrgs.length,
        excludedTrusts: excludedTrusts.sort((a, b) => a.name.localeCompare(b.name))
    };
} 