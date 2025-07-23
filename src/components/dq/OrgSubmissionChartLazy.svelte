<script>
    import { onMount } from 'svelte';

    let visible = false;
    let element;

    onMount(() => {
        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting) {
                    visible = true;
                    observer.disconnect();
                }
            },
            {
                rootMargin: '200px'
            }
        );

        observer.observe(element);

        return () => {
            observer.disconnect();
        };
    });
</script>

<div bind:this={element} class="chart-container">
    {#if visible}
        <slot />
    {:else}
        <div class="bg-white rounded-lg shadow-sm p-4 mb-6 h-[200px] flex items-center justify-center">
            <div class="flex space-x-4 w-full">
                <div class="flex-1 space-y-4">
                    <div class="h-4 bg-gray-200 rounded w-1/4"></div>
                    <div class="h-[150px] bg-gray-200 rounded"></div>
                </div>
            </div>
        </div>
    {/if}
</div> 