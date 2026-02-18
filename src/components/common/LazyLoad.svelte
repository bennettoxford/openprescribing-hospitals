<script>
    import { onMount } from 'svelte';

    let visible = false;
    let element;

    onMount(() => {
        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0]?.isIntersecting) {
                    visible = true;
                    observer.disconnect();
                }
            },
            { rootMargin: '200px' }
        );
        observer.observe(element);
        return () => observer.disconnect();
    });
</script>

<div bind:this={element}>
    {#if visible}
        <slot />
    {:else}
        <slot name="placeholder">
            <div class="bg-gray-200 rounded animate-pulse min-h-[200px]"></div>
        </slot>
    {/if}
</div>
