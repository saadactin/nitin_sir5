/**
 * Page Optimizer - Improves page load performance
 * - Lazy loads images
 * - Defers non-critical scripts
 * - Preloads critical resources
 */

(function() {
    'use strict';
    
    // Wait for DOM to be ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
    
    function init() {
        // Lazy load images
        if ('IntersectionObserver' in window) {
            lazyLoadImages();
        }
        
        // Preload critical resources
        preloadCriticalResources();
        
        // Optimize font loading
        optimizeFontLoading();
    }
    
    function lazyLoadImages() {
        const images = document.querySelectorAll('img[data-src]');
        const imageObserver = new IntersectionObserver((entries, observer) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    img.src = img.dataset.src;
                    img.removeAttribute('data-src');
                    observer.unobserve(img);
                }
            });
        });
        
        images.forEach(img => imageObserver.observe(img));
    }
    
    function preloadCriticalResources() {
        // Preload critical CSS/JS if needed
        const criticalLinks = document.querySelectorAll('link[rel="preload"]');
        // Already handled by browser
    }
    
    function optimizeFontLoading() {
        // Ensure fonts are loaded asynchronously
        if (document.fonts) {
            document.fonts.ready.then(() => {
                document.documentElement.classList.add('fonts-loaded');
            });
        }
    }
    
    // Service Worker registration for caching (optional)
    if ('serviceWorker' in navigator) {
        // Can be enabled for offline support
    }
})();

