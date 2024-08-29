<svelte:options customElement={{
    tag: 'my-component',
    shadow: 'none'
  }} />
<script>
    import './styles/styles.css';
  let count = 0;

  function increment() {
    count += 1;
  }
</script>

<div class="p-4 border border-gray-300 rounded-md text-green-500">
  <p>Count: {count}</p>
  <button on:click={increment}>Increment</button>
</div>

