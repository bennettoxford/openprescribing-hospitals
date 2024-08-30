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

    // Define the order of columns
    const columnOrder = [
        'year_month', 'vmp_code', 'vmp_name', 'ods_code', 'ods_name', 'quantity', 'unit'
    ];

    // Function to format column headers
    function formatHeader(header) {
        return header.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
    }
</script>

<div class="overflow-x-auto">
    <table class="min-w-full bg-white">
        <thead>
            <tr>
                {#each columnOrder as header}
                    <th class="px-6 py-3 border-b-2 border-gray-300 text-left text-xs leading-4 font-medium text-gray-500 uppercase tracking-wider">
                        {formatHeader(header)}
                    </th>
                {/each}
            </tr>
        </thead>
        <tbody>
            {#each tableData as row}
                <tr>
                    {#each columnOrder as key}
                        <td class="px-6 py-4 whitespace-no-wrap border-b border-gray-300">
                            {row[key]}
                        </td>
                    {/each}
                </tr>
            {/each}
        </tbody>
    </table>
</div>