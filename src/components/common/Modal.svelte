<script>
  export let isOpen = false;
  export let title = '';
  export let size = 'md'; // sm, md, lg, xl

  const sizeClasses = {
    sm: 'max-w-md',
    md: 'max-w-lg',
    lg: 'max-w-2xl',
    xl: 'max-w-4xl'
  };

  function handleBackdropClick(event) {
    if (event.target === event.currentTarget) {
      isOpen = false;
    }
  }

  function handleEscapeKey(event) {
    if (event.key === 'Escape' && isOpen) {
      isOpen = false;
    }
  }
</script>

<svelte:window on:keydown={handleEscapeKey} />

{#if isOpen}
  <div
    class="fixed inset-0 z-50 overflow-y-auto"
    on:click={handleBackdropClick}
    on:keydown={(e) => e.key === 'Enter' && handleBackdropClick(e)}
    role="button"
    tabindex="0"
  >
    <div class="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
      <div class="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75"></div>

      <span class="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>

      <div
        class="inline-block overflow-hidden text-left align-bottom transition-all transform bg-white rounded-lg shadow-xl sm:my-8 sm:align-middle {sizeClasses[size]} w-full"
        role="dialog"
        aria-modal="true"
        aria-labelledby="modal-headline"
      >
        {#if title}
          <div class="px-4 pt-5 pb-4 bg-white border-b border-gray-200 rounded-t-lg sm:p-6">
            <div class="flex items-center justify-between">
              <h3 class="text-lg font-medium leading-6 text-gray-900" id="modal-headline">
                {title}
              </h3>
              <button
                type="button"
                class="text-gray-400 hover:text-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
                on:click={() => isOpen = false}
              >
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>
        {/if}

        <div class="px-4 py-5 bg-white sm:p-6">
          <slot />
        </div>

        {#if $$slots.actions}
          <div class="px-4 py-3 bg-gray-50 border-t border-gray-200 rounded-b-lg sm:px-6 sm:flex sm:flex-row-reverse">
            <slot name="actions" />
          </div>
        {/if}
      </div>
    </div>
  </div>
{/if}
