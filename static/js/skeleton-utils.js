/**
 * Skeleton Loader Utilities
 * Professional loading states for better UX
 */

// Show skeleton loader for table
function showTableSkeleton(containerId, rows = 5, columns = 4) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  let skeletonHTML = '<table class="enhanced-table"><thead><tr>';
  for (let i = 0; i < columns; i++) {
    skeletonHTML += '<th><div class="skeleton skeleton-text-sm"></div></th>';
  }
  skeletonHTML += '</tr></thead><tbody>';
  
  for (let i = 0; i < rows; i++) {
    skeletonHTML += '<tr>';
    for (let j = 0; j < columns; j++) {
      skeletonHTML += '<td><div class="skeleton skeleton-text"></div></td>';
    }
    skeletonHTML += '</tr>';
  }
  skeletonHTML += '</tbody></table>';
  
  container.innerHTML = skeletonHTML;
}

// Show skeleton loader for card grid
function showCardSkeleton(containerId, count = 4) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  let skeletonHTML = '<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">';
  for (let i = 0; i < count; i++) {
    skeletonHTML += `
      <div class="skeleton-card">
        <div class="flex items-center justify-between mb-4">
          <div class="skeleton skeleton-avatar"></div>
          <div class="skeleton skeleton-text-sm" style="width: 60px"></div>
        </div>
        <div class="skeleton skeleton-heading mb-2"></div>
        <div class="skeleton skeleton-text" style="width: 50%"></div>
      </div>
    `;
  }
  skeletonHTML += '</div>';
  
  container.innerHTML = skeletonHTML;
}

// Show skeleton loader for list
function showListSkeleton(containerId, items = 5) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  let skeletonHTML = '<div class="space-y-3">';
  for (let i = 0; i < items; i++) {
    skeletonHTML += `
      <div class="skeleton-list-item">
        <div class="skeleton skeleton-avatar"></div>
        <div class="flex-1">
          <div class="skeleton skeleton-text mb-2"></div>
          <div class="skeleton skeleton-text-sm" style="width: 70%"></div>
        </div>
      </div>
    `;
  }
  skeletonHTML += '</div>';
  
  container.innerHTML = skeletonHTML;
}

// Show skeleton loader for chart
function showChartSkeleton(containerId) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  container.innerHTML = '<div class="skeleton skeleton-chart"></div>';
}

// Hide skeleton and show content
function hideSkeleton(containerId, contentHTML) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  container.innerHTML = contentHTML;
}

// Replace spinner with skeleton in button
function setButtonLoading(button, text = 'Loading...') {
  if (!button) return;
  
  button.dataset.origHtml = button.innerHTML;
  button.classList.add('btn-loading');
  button.innerHTML = `<span>${text}</span>`;
  button.disabled = true;
}

// Remove loading state from button
function removeButtonLoading(button, restoreText = true) {
  if (!button) return;
  
  button.classList.remove('btn-loading');
  button.disabled = false;
  
  if (restoreText && button.dataset.origHtml) {
    button.innerHTML = button.dataset.origHtml;
  }
}

// Show page-level skeleton
function showPageSkeleton() {
  const mainContent = document.querySelector('main');
  if (!mainContent) return;
  
  mainContent.innerHTML = `
    <div class="space-y-6">
      <!-- Header Skeleton -->
      <div class="flex items-center justify-between">
        <div>
          <div class="skeleton skeleton-heading mb-2"></div>
          <div class="skeleton skeleton-text" style="width: 40%"></div>
        </div>
        <div class="skeleton skeleton-button"></div>
      </div>
      
      <!-- Cards Grid Skeleton -->
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        ${Array(4).fill(0).map(() => `
          <div class="skeleton-card">
            <div class="flex items-center justify-between mb-4">
              <div class="skeleton skeleton-avatar"></div>
              <div class="skeleton skeleton-text-sm" style="width: 60px"></div>
            </div>
            <div class="skeleton skeleton-heading mb-2"></div>
            <div class="skeleton skeleton-text" style="width: 50%"></div>
          </div>
        `).join('')}
      </div>
      
      <!-- Table Skeleton -->
      <div class="bg-surface-light dark:bg-surface-dark rounded-xl shadow-lg p-6">
        <div class="skeleton skeleton-title mb-4"></div>
        <table class="enhanced-table">
          <thead>
            <tr>
              ${Array(5).fill(0).map(() => '<th><div class="skeleton skeleton-text-sm"></div></th>').join('')}
            </tr>
          </thead>
          <tbody>
            ${Array(5).fill(0).map(() => `
              <tr>
                ${Array(5).fill(0).map(() => '<td><div class="skeleton skeleton-text"></div></td>').join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

