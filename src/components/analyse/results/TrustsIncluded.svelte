<svelte:options runes={false} />

<script>
    export let trusts = [];
    export let filterDescription = '';

    $: trustCount = (trusts || []).length;
    $: trustLabel = trustCount === 1 ? 'trust' : 'trusts';
</script>

<div class="py-4">
    <h3 class="text-xl font-semibold mb-4">Trusts included in analysis</h3>
    <div class="mb-2 text-sm text-gray-700">
        <p>
            This analysis is scoped to a group of NHS trusts.
            You can see the trusts included in the <a href="#analysis-totals-table" class="text-blue-600 hover:text-blue-800 underline">table below</a>.
        </p>
    </div>
    <p class="text-sm text-gray-600">
        Included: <span class="font-semibold">{trustCount} {trustLabel}</span>{filterDescription ? ` (${filterDescription})` : ''}
    </p>
</div>
