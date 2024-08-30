<script>
    export let vmps = [];
</script>

<div class="vmp-list bg-gray-100 p-4 rounded-lg mb-4">
    <h3 class="text-lg font-semibold mb-2">VMPs Included in Analysis</h3>
    {#if vmps.length > 0}
        <ul class="list-disc pl-5">
            {#each vmps as vmp}
                <li>{vmp}</li>
            {/each}
        </ul>
    {:else}
        <p>No VMPs selected</p>
    {/if}
</div>