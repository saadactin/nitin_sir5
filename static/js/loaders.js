/**
 * Reusable Loading Indicators
 * Provides various loader components for different use cases
 */

// Spinner component
function createSpinner(size = 'md', color = 'primary') {
    const sizes = {
        'sm': 'w-4 h-4',
        'md': 'w-6 h-6',
        'lg': 'w-8 h-8',
        'xl': 'w-12 h-12'
    };
    
    const colors = {
        'primary': 'border-blue-600',
        'white': 'border-white',
        'gray': 'border-gray-600',
        'green': 'border-green-600',
        'red': 'border-red-600'
    };
    
    const spinner = document.createElement('div');
    spinner.className = `spinner ${sizes[size] || sizes.md} ${colors[color] || colors.primary}`;
    return spinner;
}

// Full page loader overlay
function showFullPageLoader(message = 'Loading...') {
    const overlay = document.createElement('div');
    overlay.id = 'full-page-loader';
    overlay.className = 'fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center';
    overlay.innerHTML = `
        <div class="bg-white dark:bg-gray-800 rounded-lg p-6 flex flex-col items-center gap-4">
            <div class="spinner w-12 h-12 border-blue-600"></div>
            <p class="text-gray-700 dark:text-gray-300 font-medium">${message}</p>
        </div>
    `;
    document.body.appendChild(overlay);
    return overlay;
}

function hideFullPageLoader() {
    const loader = document.getElementById('full-page-loader');
    if (loader) {
        loader.remove();
    }
}

// Inline loader for replacing content
function createInlineLoader(message = 'Loading...', size = 'md') {
    const loader = document.createElement('div');
    loader.className = 'flex items-center justify-center gap-3 py-8';
    loader.innerHTML = `
        <div class="spinner ${size === 'sm' ? 'w-4 h-4' : size === 'lg' ? 'w-8 h-8' : 'w-6 h-6'} border-blue-600"></div>
        <span class="text-gray-600 dark:text-gray-400">${message}</span>
    `;
    return loader;
}

// Button loader - shows spinner in button
function setButtonLoading(button, loading = true, originalText = null) {
    if (!button) return;
    
    if (loading) {
        button.dataset.originalHtml = button.innerHTML;
        button.disabled = true;
        button.classList.add('opacity-75', 'cursor-not-allowed');
        
        const spinner = createSpinner('sm', 'white');
        button.innerHTML = '';
        button.appendChild(spinner);
        if (originalText) {
            const text = document.createTextNode(` ${originalText}`);
            button.appendChild(text);
        }
    } else {
        button.disabled = false;
        button.classList.remove('opacity-75', 'cursor-not-allowed');
        if (button.dataset.originalHtml) {
            button.innerHTML = button.dataset.originalHtml;
            delete button.dataset.originalHtml;
        }
    }
}

// Table row skeleton loader
function createTableSkeleton(columns = 4, rows = 5) {
    const skeleton = document.createElement('div');
    skeleton.className = 'space-y-2';
    
    for (let i = 0; i < rows; i++) {
        const row = document.createElement('div');
        row.className = 'flex gap-4';
        for (let j = 0; j < columns; j++) {
            const cell = document.createElement('div');
            cell.className = 'h-4 bg-gray-200 dark:bg-gray-700 rounded loading-skeleton flex-1';
            row.appendChild(cell);
        }
        skeleton.appendChild(row);
    }
    
    return skeleton;
}

// Card skeleton loader
function createCardSkeleton(count = 3) {
    const container = document.createElement('div');
    container.className = 'grid grid-cols-1 lg:grid-cols-2 gap-6';
    
    for (let i = 0; i < count; i++) {
        const card = document.createElement('div');
        card.className = 'bg-surface-light dark:bg-surface-dark rounded-lg shadow border border-border-light dark:border-border-dark p-6';
        card.innerHTML = `
            <div class="h-6 bg-gray-200 dark:bg-gray-700 rounded loading-skeleton mb-4 w-3/4"></div>
            <div class="h-4 bg-gray-200 dark:bg-gray-700 rounded loading-skeleton mb-2 w-full"></div>
            <div class="h-4 bg-gray-200 dark:bg-gray-700 rounded loading-skeleton mb-2 w-5/6"></div>
            <div class="h-4 bg-gray-200 dark:bg-gray-700 rounded loading-skeleton w-4/6"></div>
        `;
        container.appendChild(card);
    }
    
    return container;
}

// Show loading state in element
function showLoading(element, message = 'Loading...', replaceContent = true) {
    if (!element) return;
    
    const loader = createInlineLoader(message);
    if (replaceContent) {
        element.innerHTML = '';
        element.appendChild(loader);
    } else {
        element.appendChild(loader);
    }
    return loader;
}

// Hide loading state
function hideLoading(element, originalContent = null) {
    if (!element) return;
    
    const loader = element.querySelector('.spinner')?.closest('.flex.items-center.justify-center');
    if (loader) {
        loader.remove();
    }
    
    if (originalContent && element.innerHTML.trim() === '') {
        element.innerHTML = originalContent;
    }
}

// Export for use in other scripts
window.LoaderUtils = {
    createSpinner,
    showFullPageLoader,
    hideFullPageLoader,
    createInlineLoader,
    setButtonLoading,
    createTableSkeleton,
    createCardSkeleton,
    showLoading,
    hideLoading
};

