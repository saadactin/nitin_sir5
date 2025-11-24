# Loading Indicators Implementation

This document outlines all loading indicators (loaders) added throughout the application to improve user experience.

## Overview

Loading indicators have been added to provide visual feedback during async operations, form submissions, and data fetching. This prevents user confusion and provides clear feedback about ongoing operations.

## Components Created

### 1. Loader Utilities (`static/js/loaders.js`)

A reusable JavaScript module providing various loader components:

- **`createSpinner(size, color)`** - Creates a spinner with customizable size and color
- **`showFullPageLoader(message)`** - Shows a full-page overlay loader
- **`hideFullPageLoader()`** - Hides the full-page loader
- **`createInlineLoader(message, size)`** - Creates an inline loader for content areas
- **`setButtonLoading(button, loading, text)`** - Shows/hides loading state in buttons
- **`createTableSkeleton(columns, rows)`** - Creates skeleton loaders for tables
- **`createCardSkeleton(count)`** - Creates skeleton loaders for cards
- **`showLoading(element, message, replaceContent)`** - Shows loading in any element
- **`hideLoading(element, originalContent)`** - Hides loading and restores content

### 2. CSS Enhancements (`static/css/performance.css`)

Added spinner styles with:
- Multiple size variants (w-4, w-6, w-8, w-12)
- Color variants (blue, white, gray, green, red)
- Smooth animations
- Loading skeleton animations

## Implementation Locations

### Dashboard (`templates/dashboard.html`)

**Added loaders for:**
- Auto-refresh functionality
  - Shows spinner when refreshing last sync details
  - Shows loading indicator in sync history table
  - Restores content on error

**Features:**
- Visual feedback during 10-second auto-refresh
- Only refreshes when page is visible
- Graceful error handling with content restoration

### Sync Summary (`templates/sync_summary.html`)

**Added loaders for:**
- Initial page load
  - Large spinner with "Loading server summaries..." message
  - Replaced with server cards when data loads
- Refresh button
  - Button shows "Refreshing..." state
  - Disabled during refresh
  - Restores original state on completion

**Features:**
- Immediate visual feedback on page load
- Clear loading state during data fetch
- Error handling with user-friendly messages

### HANA Source Form (`templates/add_hana_source.html`)

**Added loaders for:**
1. **Test Connection Button**
   - Shows spinner and "Testing..." text
   - Disabled during test
   - Restores on completion/error

2. **Load Target Databases**
   - Dropdown shows "Loading databases..." with spinner
   - Disabled during fetch
   - Populated on success

3. **Migration Button**
   - Shows spinner and "Migrating..." text
   - Status div shows progress message
   - Updates with success/error messages

4. **Form Submission**
   - Submit button shows "Submitting..." state
   - Prevents double submission
   - Visual feedback during POST request

**Features:**
- All async operations have loading states
- Clear error messages
- Prevents user from clicking multiple times

### API Source Form (`templates/add_api_source.html`)

**Added loaders for:**
1. **Test Connection Button**
   - Uses loader utilities
   - Shows "Testing..." state
   - Restores on completion

2. **Load Target Databases**
   - Dropdown shows spinner during load
   - Disabled during fetch
   - Error handling

**Features:**
- Consistent with HANA form
- Uses shared loader utilities

## Usage Examples

### Button Loading
```javascript
// Show loading
window.LoaderUtils.setButtonLoading(button, true, 'Processing...');

// Hide loading
window.LoaderUtils.setButtonLoading(button, false);
```

### Inline Loading
```javascript
// Show loading in element
window.LoaderUtils.showLoading(element, 'Loading data...', true);

// Hide loading
window.LoaderUtils.hideLoading(element, originalContent);
```

### Full Page Loader
```javascript
// Show full page loader
const overlay = window.LoaderUtils.showFullPageLoader('Processing request...');

// Hide full page loader
window.LoaderUtils.hideFullPageLoader();
```

## Benefits

1. **Improved UX**: Users always know when something is happening
2. **Prevents Double Submissions**: Buttons are disabled during operations
3. **Clear Feedback**: Visual indicators show progress
4. **Error Handling**: Graceful degradation with error messages
5. **Consistent Design**: All loaders use the same styling
6. **Performance**: Loaders don't block UI, operations run async

## Browser Compatibility

- Modern browsers (Chrome, Firefox, Safari, Edge)
- Uses CSS animations (fallback to static spinner if animations disabled)
- Works with JavaScript disabled (forms still submit, just no loading indicators)

## Future Enhancements

1. **Progress Bars**: For long-running operations (migrations)
2. **Toast Notifications**: For success/error messages
3. **Skeleton Screens**: More detailed loading states for complex pages
4. **Lazy Loading**: For images and heavy content
5. **Virtual Scrolling**: For large data tables

## Files Modified

1. `static/js/loaders.js` - New loader utilities module
2. `static/css/performance.css` - Spinner and loader styles
3. `templates/base.html` - Added loader script reference
4. `templates/dashboard.html` - Added loaders to refresh function
5. `templates/sync_summary.html` - Added loaders to server cards loading
6. `templates/add_hana_source.html` - Added loaders to all async operations
7. `templates/add_api_source.html` - Added loaders to test connection and database loading

## Testing

To test loaders:
1. Navigate to Dashboard - should see spinner during auto-refresh
2. Go to Sync Summary - should see loading spinner on page load
3. Add HANA Source - test connection button should show loading
4. Add API Source - test connection should show loading
5. Submit any form - submit button should show loading state

All loaders should:
- Appear immediately when action is triggered
- Disable buttons/inputs during operation
- Show appropriate messages
- Restore original state on completion/error

