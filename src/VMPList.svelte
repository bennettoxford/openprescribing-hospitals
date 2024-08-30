<script>
    export let vmps = [];
</script>

<div>
    <h3>VMPs:</h3>
    <ul>
        {#each vmps as vmp}
            <li>{vmp}</li>
        {/each}
    </ul>
</div>