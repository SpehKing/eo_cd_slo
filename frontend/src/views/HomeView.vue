<script lang="ts" setup>
import { onMounted, ref, onBeforeUnmount } from "vue";
import L from "leaflet";
import type { ImageMetadata } from "@/types/api";

// Components
import MapComponent from "@/components/MapComponent.vue";
import FloatingDashboard from "@/components/FloatingDashboard.vue";
import ImageSidebar from "@/components/ImageSidebar.vue";

// Composables
import { useMapImages } from "@/composables/useMapImages";
import { useTimeFilter } from "@/composables/useTimeFilter";
import { useBoundingBoxSelection } from "@/composables/useBoundingBoxSelection";

// State
const showSidebar = ref(false);
const imageLayerVisible = ref(true);

let map: L.Map | null = null;

// Composables
const mapImages = useMapImages();
const timeFilter = useTimeFilter();
const boundingBoxSelection = useBoundingBoxSelection();

// Event handlers
onMounted(async () => {
  await timeFilter.initializeDateRanges();
});

onBeforeUnmount(() => {
  boundingBoxSelection.cleanup();
});

async function onMapReady(mapInstance: L.Map) {
  map = mapInstance;
  mapImages.initializeLayers(map);
  boundingBoxSelection.initializeBoundingBox(map);
}

function onImageSelected(image: ImageMetadata) {
  mapImages.selectImage(image);
  showSidebar.value = true;
}

function closeSidebar() {
  showSidebar.value = false;
  mapImages.selectImage(null as any);
}

// Layer controls
function onToggleImageLayer(event: Event) {
  const target = event.target as HTMLInputElement;
  imageLayerVisible.value = target.checked;
  if (map) {
    mapImages.toggleImageLayer(map, target.checked);
  }
}

// Dashboard handlers
async function onExecuteQuery() {
  if (!map) return;

  const selectedBounds = boundingBoxSelection.getSelectedBounds();
  const timeRange = timeFilter.timeRange.value;

  if (selectedBounds.length > 0 && timeRange) {
    // Execute a single grouped query for all selected grid squares
    await mapImages.loadImagesForMultipleBounds(selectedBounds, timeRange);
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
      :image-layer-visible="imageLayerVisible"
      :is-loading="mapImages.isLoading.value"
      :min-date="timeFilter.minDate.value"
      :max-date="timeFilter.maxDate.value"
      :selected-start-date="timeFilter.selectedStartDate.value"
      :selected-end-date="timeFilter.selectedEndDate.value"
      @toggle-image-layer="onToggleImageLayer"
      @toggle-drawing-mode="onToggleDrawingMode"
      @execute-query="onExecuteQuery"
      @clear-selection="onClearSelection"
      @update:start-date="onUpdateStartDate"
      @update:end-date="onUpdateEndDate"
    />

    <!-- Image Details Sidebar -->
    <ImageSidebar
      :visible="showSidebar"
      :image="mapImages.selectedImage.value"
      :image-preview-url="mapImages.imagePreviewUrl.value"
      @close="closeSidebar"
      @download="onDownloadImage"
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
