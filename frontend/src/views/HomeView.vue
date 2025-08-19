<script lang="ts" setup>
import { onMounted, ref, onBeforeUnmount } from "vue";
import L from "leaflet";
import type { ImageMetadata } from "@/types/api";

// Components
import MapComponent from "@/components/MapComponent.vue";
import FloatingDashboard from "@/components/FloatingDashboard.vue";
import ImageSidebar from "@/components/ImageSidebar.vue";
import ImageComparisonModal from "@/components/ImageComparisonModal.vue";

// Composables
import { useMapImages } from "@/composables/useMapImages";
import { useTimeFilter } from "@/composables/useTimeFilter";
import { useBoundingBoxSelection } from "@/composables/useBoundingBoxSelection";
import { useImageComparison } from "@/composables/useImageComparison";

// State
const showSidebar = ref(false);
const showComparisonModal = ref(false);
const visibleYears = ref(new Set<number>());

let map: L.Map | null = null;

// Composables
const mapImages = useMapImages();
const timeFilter = useTimeFilter();
const boundingBoxSelection = useBoundingBoxSelection();
const imageComparison = useImageComparison();

// Event handlers
onMounted(async () => {
  await timeFilter.initializeDateRanges();
});

onBeforeUnmount(() => {
  boundingBoxSelection.cleanup();
});

// Handle image overlay clicks for comparison modal
async function onImageOverlayClick(clickedImage: ImageMetadata) {
  // Get all available years from loaded images if visibleYears is not set
  let yearsToUse = visibleYears.value;
  if (yearsToUse.size === 0) {
    const allYears = new Set<number>();
    mapImages.images.value.forEach((image) => {
      const year = new Date(image.time).getFullYear();
      allYears.add(year);
    });
    yearsToUse = allYears;
  }

  if (yearsToUse.size < 2) {
    // If less than 2 years are available, fall back to sidebar behavior
    onImageSelected(clickedImage);
    return;
  }

  try {
    // Load comparison data and show modal
    await imageComparison.loadComparisonData(
      clickedImage,
      mapImages.images.value,
      mapImages.masks.value,
      yearsToUse
    );

    // Only show modal if comparison data was successfully loaded
    if (
      !imageComparison.hasError.value &&
      imageComparison.comparisonData.value
    ) {
      showComparisonModal.value = true;
    } else {
      // Fall back to sidebar behavior if comparison fails or has insufficient data
      onImageSelected(clickedImage);
    }
  } catch (error) {
    console.warn(
      "Failed to load comparison data, falling back to sidebar:",
      error
    );
    // Fall back to sidebar behavior if comparison fails
    onImageSelected(clickedImage);
  }
}

async function onMapReady(mapInstance: L.Map) {
  map = mapInstance;
  mapImages.initializeLayers(map);
  boundingBoxSelection.initializeBoundingBox(map);

  // Set up image overlay click handler for comparison modal
  mapImages.setImageOverlayClickHandler(onImageOverlayClick);
}

function onImageSelected(image: ImageMetadata) {
  mapImages.selectImage(image);
  showSidebar.value = true;
}

function closeSidebar() {
  showSidebar.value = false;
  mapImages.selectImage(null as any);
}

function closeComparisonModal() {
  showComparisonModal.value = false;
  imageComparison.clearComparisonData();
}

// Dashboard handlers
async function onExecuteQuery() {
  if (!map) return;

  const selectedBounds = boundingBoxSelection.getSelectedBounds();
  const timeRange = timeFilter.timeRange.value;

  if (selectedBounds.length > 0 && timeRange) {
    // Execute a single grouped query for all selected grid squares
    await mapImages.loadImagesForMultipleBounds(selectedBounds, timeRange);
    await mapImages.loadMasksForMultipleBounds(selectedBounds, timeRange);

    // Initialize all years as visible
    const allYears = new Set<number>();
    mapImages.images.value.forEach((image) => {
      const year = new Date(image.time).getFullYear();
      allYears.add(year);
    });
    visibleYears.value = allYears;

    // Switch back to navigation mode if currently in drawing mode
    if (boundingBoxSelection.drawingMode.value) {
      boundingBoxSelection.toggleDrawingMode();
    }

    // Clear the bounding box overlay after successful execution
    boundingBoxSelection.clearSelection();
  }
}

function onClearSelection() {
  boundingBoxSelection.clearSelection();
}

function onToggleDrawingMode() {
  boundingBoxSelection.toggleDrawingMode();
}

function onUpdateStartDate(value: string) {
  timeFilter.selectedStartDate.value = value;
}

function onUpdateEndDate(value: string) {
  timeFilter.selectedEndDate.value = value;
}

// Year filtering handler
function onToggleYearVisibility(year: number) {
  const newVisibleYears = new Set(visibleYears.value);

  if (newVisibleYears.has(year)) {
    newVisibleYears.delete(year);
  } else {
    newVisibleYears.add(year);
  }

  visibleYears.value = newVisibleYears;

  // Apply visibility to the map layers
  mapImages.toggleYearVisibility(year, newVisibleYears.has(year));
}

// Download handler
function onDownloadImage(image: ImageMetadata) {
  mapImages.downloadOriginalImage(image);
}
</script>

<template>
  <div
    class="relative"
    :class="{ 'drawing-mode': boundingBoxSelection.isDrawing.value }"
  >
    <!-- Map Container -->
    <div class="map-container">
      <MapComponent @map-ready="onMapReady" />
    </div>

    <!-- Floating Dashboard -->
    <FloatingDashboard
      :selected-count="boundingBoxSelection.hasSelection.value ? 1 : 0"
      :total-area="boundingBoxSelection.totalArea.value"
      :drawing-mode="boundingBoxSelection.drawingMode.value"
      :is-loading="mapImages.isLoading.value"
      :min-date="timeFilter.minDate.value"
      :max-date="timeFilter.maxDate.value"
      :selected-start-date="timeFilter.selectedStartDate.value"
      :selected-end-date="timeFilter.selectedEndDate.value"
      :has-selection="boundingBoxSelection.hasSelection.value"
      :available-images="mapImages.images.value"
      :visible-years="visibleYears"
      @toggle-drawing-mode="onToggleDrawingMode"
      @execute-query="onExecuteQuery"
      @clear-selection="onClearSelection"
      @update:start-date="onUpdateStartDate"
      @update:end-date="onUpdateEndDate"
      @toggle-year-visibility="onToggleYearVisibility"
    />

    <!-- Image Details Sidebar -->
    <ImageSidebar
      :visible="showSidebar"
      :image="mapImages.selectedImage.value"
      :image-preview-url="mapImages.imagePreviewUrl.value"
      @close="closeSidebar"
      @download="onDownloadImage"
    />

    <!-- Image Comparison Modal -->
    <ImageComparisonModal
      :visible="showComparisonModal"
      :comparison-data="imageComparison.comparisonData.value"
      :is-loading="imageComparison.isLoading.value"
      :has-error="imageComparison.hasError.value"
      :error-message="imageComparison.errorMessage.value"
      @close="closeComparisonModal"
    />
  </div>
</template>
<style scoped>
.map-container {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  z-index: 0;
}

.map {
  width: 100%;
  height: 100%;
}

/* Custom Leaflet popup styles */
:global(.leaflet-popup-content-wrapper) {
  border-radius: 8px;
}

:global(.leaflet-popup-content) {
  margin: 0;
}

/* Drawing mode cursor */
.drawing-mode .map-container {
  cursor: crosshair;
}

/* Disable text selection while drawing */
.drawing-mode {
  user-select: none;
  -webkit-user-select: none;
  -moz-user-select: none;
  -ms-user-select: none;
}
</style>
