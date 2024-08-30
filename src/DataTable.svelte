<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import './styles/styles.css';

    let tableData = [];

    export function updateData(newData) {
        console.log("DataTable updateData called with:", newData);
        tableData = newData;
    }

    onMount(() => {
        console.log("DataTable onMount called");
    });
</script>

<div class="overflow-x-auto">
    <table class="min-w-full bg-white">
        <thead>
            <tr>
                {#if tableData.length > 0}
                    {#each Object.keys(tableData[0]) as header}
                        <th class="px-6 py-3 border-b-2 border-gray-300 text-left text-xs leading-4 font-medium text-gray-500 uppercase tracking-wider">
                            {header}
                        </th>
                    {/each}
                {/if}
            </tr>
        </thead>
        <tbody>
            {#each tableData as row}
                <tr>
                    {#each Object.values(row) as cell}
                        <td class="px-6 py-4 whitespace-no-wrap border-b border-gray-300">
                            {cell}
                        </td>
                    {/each}
                </tr>
            {/each}
        </tbody>
    </table>
</div>