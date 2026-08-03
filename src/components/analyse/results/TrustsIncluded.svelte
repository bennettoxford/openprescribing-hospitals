<svelte:options runes={false} />

<script>
    export let trusts = [];
    export let filterDescription = '';
    export let isSingleTrust = false;

    $: trustCount = (trusts || []).length;
    $: trustLabel = trustCount === 1 ? 'trust' : 'trusts';
    $: trustName = trustCount > 0 ? trusts[0] : '';
</script>

<div class="py-4">
    <h3 class="text-xl font-semibold mb-4">Trusts included in analysis</h3>
    <div class="mb-2 text-sm text-gray-700">
        <p>
            {#if isSingleTrust}
                This analysis is scoped to a single NHS trust.
            {:else}
                This analysis is scoped to a group of NHS trusts.
            {/if}
            You can see the trusts included in the <a href="#analysis-totals-table" class="text-blue-600 hover:text-blue-800 underline">table below</a>.
        </p>
    </div>
    <p class="text-sm text-gray-600">
        {#if isSingleTrust}
            Included: <span class="font-semibold">{trustName}</span>
        {:else}
            Included: <span class="font-semibold">{trustCount}</span> {trustLabel}{#if filterDescription}{' '}({filterDescription}){/if}
        {/if}
    </p>
</div>
