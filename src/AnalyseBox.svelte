<svelte:options customElement={{
    tag: 'analyse-box',
    shadow: 'none'
  }} />

<script>
    import { onMount } from 'svelte';
    import './styles/styles.css';
    import Search from './Search.svelte';
    import SearchableDropdown from './SearchableDropdown.svelte';

    let analysisType = 'default';
    let isAnalysisRunning = false;

    function runAnalysis() {
        isAnalysisRunning = true;
        // Simulating an analysis process
        setTimeout(() => {
            isAnalysisRunning = false;
            alert(`Analysis of type "${analysisType}" completed!`);
        }, 2000);
    }

    onMount(() => {
        // Any initialization code if needed
    });
</script>

<div class="analyse-box p-4 bg-white rounded-lg shadow-md">
    <h2 class="text-xl font-bold mb-4">Analysis Tools</h2>
    <div class="mb-4">
        <h3 class="text-lg font-semibold mb-2">Search</h3>
        <Search />
    </div>
    <div class="mb-4">
        <h3 class="text-lg font-semibold mb-2">Filter</h3>
        <SearchableDropdown />
    </div>
    <div class="mt-6">
        <h3 class="text-lg font-semibold mb-2">Run Analysis</h3>
        <div class="mb-2">
            <label for="analysis-type" class="block text-sm font-medium text-gray-700">Analysis Type</label>
            <select
                id="analysis-type"
                bind:value={analysisType}
                class="mt-1 block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm rounded-md"
            >
                <option value="default">Default Analysis</option>
                <option value="advanced">Advanced Analysis</option>
                <option value="custom">Custom Analysis</option>
            </select>
        </div>
        <button
            on:click={runAnalysis}
            disabled={isAnalysisRunning}
            class="w-full bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded focus:outline-none focus:shadow-outline {isAnalysisRunning ? 'opacity-50 cursor-not-allowed' : ''}"
        >
            {isAnalysisRunning ? 'Running Analysis...' : 'Run Analysis'}
        </button>
    </div>
</div>

<style>
    .analyse-box {
        width: 100%;
        max-width: 300px;
    }
</style>