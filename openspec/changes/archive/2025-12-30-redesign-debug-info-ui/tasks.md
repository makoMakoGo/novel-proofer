# Tasks: Redesign Debug Info UI

## 1. UI Structure Changes
- [x] 1.1 Add Tab navigation component (进度 / 调试信息)
- [x] 1.2 Create "进度" Tab content (form, progress bar, status)
- [x] 1.3 Create "调试信息" Tab content (chunk list)
- [x] 1.4 Implement Tab switching logic

## 2. Visual Improvements
- [x] 2.1 Design status indicators (colors) for chunk states: pending, processing, done, error, retrying
- [x] 2.2 Add CSS styles for different chunk states
- [x] 2.3 Improve chunk info layout (grid/table instead of plain text)
- [x] 2.4 Add summary stats header (total/done/error counts)
- [x] 2.5 Unify spacing and margins across all UI elements for consistent layout

## 3. Filtering
- [x] 3.1 Add filter buttons for chunk states (All/Errors/Retrying)
- [x] 3.2 Implement client-side filtering logic

## 4. Responsive & Polish
- [x] 4.1 Ensure mobile-friendly layout
- [x] 4.2 Persist active tab state in localStorage

## 5. Testing
- [x] 5.1 Manual test with large file (many chunks)
- [x] 5.2 Verify error states display correctly
- [x] 5.3 Test on different screen sizes
