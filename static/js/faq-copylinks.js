// FAQ Copy Links functionality
document.addEventListener('DOMContentLoaded', function() {
    // Initialize clipboard.js for existing buttons
    const clipboard = new ClipboardJS('.copy-link-btn');

    clipboard.on('success', function(e) {
        showToast();
        e.clearSelection();
    });

    clipboard.on('error', function(e) {
        console.error('Failed to copy:', e);
    });

    // Add copyable links to all h3 headings in the markdown content
    addCopyLinksToSubheadings();
    
    // Reinitialise clipboard.js to include the new buttons
    const newClipboard = new ClipboardJS('.copy-link-btn');
    
    newClipboard.on('success', function(e) {
        showToast();
        e.clearSelection();
    });
    
    newClipboard.on('error', function(e) {
        console.error('Failed to copy:', e);
    });
});

function addCopyLinksToSubheadings() {
    // Find all h3 elements within the prose content
    const h3Elements = document.querySelectorAll('.prose h3[id]');
    
    h3Elements.forEach(function(h3) {
        // Create a wrapper div to handle hover states
        const wrapper = document.createElement('div');
        wrapper.className = 'group flex items-center gap-2';
        
        // Move the h3 content to a new element inside the wrapper
        const h3Content = h3.innerHTML;
        h3.innerHTML = '';
        h3.appendChild(wrapper);
        
        // Create the title span
        const titleSpan = document.createElement('span');
        titleSpan.innerHTML = h3Content;
        wrapper.appendChild(titleSpan);
        
        // Create the copy button
        const copyButton = document.createElement('button');
        copyButton.className = 'copy-link-btn opacity-0 group-hover:opacity-100 transition-opacity duration-200 p-1 rounded hover:bg-gray-100 text-gray-500 hover:text-oxford-800';
        copyButton.setAttribute('data-clipboard-text', window.location.href.split('#')[0] + '#' + h3.id);
        copyButton.setAttribute('title', 'Copy link to this question');
        copyButton.setAttribute('aria-label', 'Copy link to ' + titleSpan.textContent);
        
        copyButton.innerHTML = `
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-4 h-4">
                <path stroke-linecap="round" stroke-linejoin="round" d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244" />
            </svg>
        `;
        
        wrapper.appendChild(copyButton);
    });
}

function showToast() {
    const toast = document.getElementById('copy-toast');
    toast.classList.remove('translate-y-full', 'opacity-0');
    toast.classList.add('translate-y-0', 'opacity-100');
    
    setTimeout(() => {
        toast.classList.add('translate-y-full', 'opacity-0');
        toast.classList.remove('translate-y-0', 'opacity-100');
    }, 2000);
} 