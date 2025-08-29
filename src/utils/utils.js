export function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

export function formatNumber(value, options = {}) {
    const { 
        addCommas = false, 
        showUnit = false, 
        unit = '', 
        maxDecimals
    } = options;
    
    if (value === 0) return '0' + (showUnit ? ` ${unit}` : '');
    if (value == null || isNaN(value)) return 'N/A';

    if (value > 0 && value < 0.001) {
        return value.toExponential(2) + (showUnit ? ` ${unit}` : '');
    }
    
    let result;
    if (value < 0.1) {
        result = value.toFixed(3);
    } else if (value < 1) {
        result = value.toFixed(2);
    } else if (value >= 1000000) {
        result = `${(value / 1000000).toFixed(maxDecimals !== undefined ? Math.min(maxDecimals, 1) : 1)}m`;
    } else if (value >= 1000) {
        result = `${(value / 1000).toFixed(maxDecimals !== undefined ? Math.min(maxDecimals, 1) : 1)}k`;
    } else if (value < 10) {
        result = value.toFixed(1);
    } else {
        result = Math.round(value).toString();
    }
    
    if (addCommas) {
        const parts = result.split('.');
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        result = parts.join('.');
    }
    
    if (showUnit && unit) {
        result += ` ${unit}`;
    }
    
    return result;
}

export function formatStrength(value) {
    if (value == null || isNaN(value)) return 'N/A';
    
    const num = Number(value);
    
    const str = num.toString();
    const decimalIndex = str.indexOf('.');
    
    // If no decimal point or 5 or fewer decimal places, return as is
    if (decimalIndex === -1 || str.length - decimalIndex - 1 <= 5) {
        return str;
    }
    
    // If more than 5 decimal places, round to 5 decimal places
    return num.toFixed(5);
}

export function normaliseDDDUnit(unit) {
    if (!unit || typeof unit !== 'string') {
        return unit;
    }
    
    if (unit.startsWith('DDD (') && unit.endsWith(')')) {
        return 'DDD';
    }

    return unit;
}