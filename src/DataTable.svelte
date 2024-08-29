<svelte:options customElement={{
    tag: 'data-table',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import './styles/styles.css';

    // Define the prop for data
    export let data = [];

    let sortColumn = 'id';
    let sortDirection = 1; // 1 for ascending, -1 for descending

    $: columns = data.length > 0 ? Object.keys(data[0]) : [];

    $: sortedData = [...data].sort((a, b) => {
        if (a[sortColumn] < b[sortColumn]) return -1 * sortDirection;
        if (a[sortColumn] > b[sortColumn]) return 1 * sortDirection;
        return 0;
    });

    function sort(column) {
        if (column === sortColumn) {
            sortDirection *= -1;
        } else {
            sortColumn = column;
            sortDirection = 1;
        }
    }

    // Custom element API to update data
    onMount(() => {
        if (customElements.get('data-table')) {
            customElements.get('data-table').prototype.updateData = function(newData) {
                data = newData;
                this.dispatchEvent(new CustomEvent('dataUpdated'));
            };
        }
    });
</script>

<div class="overflow-x-auto w-full">
    <table class="min-w-full bg-white border border-gray-300">
        <thead class="bg-gray-100">
            <tr>
                {#each columns as column}
                    <th class="sticky top-0 px-4 py-2 border-b cursor-pointer" on:click={() => sort(column)}>
                        {column} {sortColumn === column ? (sortDirection === 1 ? '▲' : '▼') : ''}
                    </th>
                {/each}
            </tr>
        </thead>
        <tbody class="overflow-y-auto block h-48"> <!-- Changed from h-96 to h-48 -->
            {#each sortedData as row}
                <tr class="table w-full table-fixed">
                    {#each columns as column}
                        <td class="px-4 py-2 border-b truncate">{row[column]}</td>
                    {/each}
                </tr>
            {/each}
        </tbody>
    </table>
</div>

<style>
    .overflow-x-auto {
        overflow-x: auto;
        max-width: 100%;
    }

    tbody {
        overflow-y: auto;
        max-height: 400px;
        display: block;
    }

    tr {
        display: table;
        width: 100%;
        table-layout: fixed;
    }

    thead {
        display: table;
        width: calc(100% - 1em); /* Adjust for scrollbar width */
        table-layout: fixed;
    }

    th, td {
        word-wrap: break-word;
    }
</style>