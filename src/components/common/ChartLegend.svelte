<script>
    import { legendStore } from '../../stores/legendStore.js';
  
    export let items = [];
    export let isPercentileMode = false;
    export let onChange = () => {};
  
    $: {
        legendStore.setItems(items);
        legendStore.setPercentileMode(isPercentileMode);
        const visibleItems = items
            .filter(item => item.visible)
            .map(item => item.label);
        legendStore.setVisibleItems(visibleItems);
    }
  
    function toggleItem(item) {
        legendStore.toggleItem(item.label);
        const visibleItems = Array.from($legendStore.visibleItems);
        onChange(visibleItems);
    }
</script>

<div class="h-full flex flex-col overflow-hidden">
    <ul class="space-y-1 overflow-y-auto flex-grow">
        {#each items as item (item.label)}
            <li 
                class="flex items-start gap-2 rounded transition-colors min-w-0 {item.selectable ? 'cursor-pointer hover:bg-gray-50' : 'cursor-default'}"
                class:opacity-50={!$legendStore.visibleItems.has(item.label)}
                on:click={() => item.selectable && toggleItem(item)}
            >
                <div 
                    class="w-4 h-4 rounded-sm flex-shrink-0 mt-1" 
                    style="background-color: {item.color}; opacity: {item.opacity || 1};"
                ></div>
                <span class="text-sm break-words leading-tight min-w-0 flex-shrink">{item.label}</span>
            </li>
        {/each}
    </ul>
</div>

<style>
    li {
        user-select: none;
    }
</style>