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
import { useGridSelection } from "@/composables/useGridSelection";

// State
const showSidebar = ref(false);
const imageLayerVisible = ref(true);
const boundaryLayerVisible = ref(true);

let map: L.Map | null = null;

// Composables
const mapImages = useMapImages();
const timeFilter = useTimeFilter();
const gridSelection = useGridSelection();

// Event handlers
onMounted(async () => {
  await timeFilter.initializeDateRanges();
});

onBeforeUnmount(() => {
  gridSelection.cleanup();
});

async function onMapReady(mapInstance: L.Map) {
  map = mapInstance;
  mapImages.initializeLayers(map);
  gridSelection.initializeGrid(map);
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

function onToggleBoundaryLayer(event: Event) {
  const target = event.target as HTMLInputElement;
  boundaryLayerVisible.value = target.checked;
  if (map) {
    mapImages.toggleBoundaryLayer(map, target.checked);
  }
}

// Dashboard handlers
async function onExecuteQuery() {
  if (!map) return;

  const selectedBounds = gridSelection.getSelectedBounds();
  const timeRange = timeFilter.timeRange.value;

  if (selectedBounds.length > 0 && timeRange) {
    // Execute query for each selected grid square
    for (const bounds of selectedBounds) {
      await mapImages.loadImagesForBounds(bounds, timeRange);
    }
  }
}

function onClearSelection() {
  gridSelection.clearSelection();
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
  <div class="relative">
    <!-- Map Container -->
    <div class="map-container">
      <MapComponent @map-ready="onMapReady" />
    </div>

    <!-- Floating Dashboard -->
    <FloatingDashboard
      :selected-count="gridSelection.selectedCount.value"
      :total-area="gridSelection.totalArea.value"
      :image-layer-visible="imageLayerVisible"
      :boundary-layer-visible="boundaryLayerVisible"
      :is-loading="mapImages.isLoading.value"
      :min-date="timeFilter.minDate.value"
      :max-date="timeFilter.maxDate.value"
      :selected-start-date="timeFilter.selectedStartDate.value"
      :selected-end-date="timeFilter.selectedEndDate.value"
      @toggle-image-layer="onToggleImageLayer"
      @toggle-boundary-layer="onToggleBoundaryLayer"
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
</style>
