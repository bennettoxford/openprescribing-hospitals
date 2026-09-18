import { describe, it, expect } from 'vitest';
import {
    PRODUCT_URL_PARAMS,
    buildAnalysisUrlParams,
    buildProductValidationParams,
    buildValidationParams,
} from '../../components/analyse/lib/analyseUrlParams.js';

describe('analyse URL params', () => {
    it('encodes selected products with vmps, vtms, ingredients, and atcs', () => {
        const params = buildAnalysisUrlParams({
            products: [
                { code: '123', type: 'vmp' },
                { code: '456', type: 'vmp' },
                { code: '111', type: 'vtm' },
                { code: 'A10', type: 'atc' },
            ],
        });

        expect(params).toEqual({
            vmps: '123,456',
            vtms: '111',
            atcs: 'A10',
        });
        expect(PRODUCT_URL_PARAMS).toEqual(['vmps', 'vtms', 'ingredients', 'atcs']);
    });

    it('builds a product-only validation query for product lookup', () => {
        const urlParams = new URLSearchParams('vmps=123,456&ingredients=222&trusts=RGT');

        expect(buildProductValidationParams(urlParams).toString()).toBe(
            'vmps=123%2C456&ingredients=222'
        );
        expect(buildProductValidationParams(new URLSearchParams()).toString()).toBe('');
    });

    it('adds analysis fields after the shared product params', () => {
        const urlParams = new URLSearchParams('vmps=123&trusts=RGT&quantity=ddd');

        expect(buildValidationParams(urlParams).toString()).toBe(
            'vmps=123&trusts=RGT&quantity=ddd'
        );
    });
});
