<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    export let data = [];
    export let quantityType = 'Dose';
</script>

<div>
    <h3 class="text-lg font-semibold mb-2">Data Table for {quantityType}</h3>
    <table class="min-w-full bg-white">
        <thead>
            <tr>
                <th class="py-2 px-4 border-b">Date</th>
                <th class="py-2 px-4 border-b">VMP Name</th>
                <th class="py-2 px-4 border-b">ODS Code</th>
                <th class="py-2 px-4 border-b">Quantity</th>
            </tr>
        </thead>
        <tbody>
            {#each data as row}
                <tr>
                    <td class="py-2 px-4 border-b">{row.year_month}</td>
                    <td class="py-2 px-4 border-b">{row.vmp_name}</td>
                    <td class="py-2 px-4 border-b">{row.ods_code}</td>
                    <td class="py-2 px-4 border-b">{row.quantity}</td>
                </tr>
            {/each}
        </tbody>
    </table>
</div>