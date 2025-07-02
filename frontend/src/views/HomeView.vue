<script lang="ts" setup>
import { onMounted, ref, computed } from "vue";
import L from "leaflet";
import { cacheService } from "@/services/cache";
import type { ImageMetadata } from "@/types/api";

// Components
import MapComponent from "@/components/MapComponent.vue";
import MapOverlays from "@/components/MapOverlays.vue";
import LayerControls from "@/components/LayerControls.vue";
import TimeFilter from "@/components/TimeFilter.vue";
import ImageSidebar from "@/components/ImageSidebar.vue";

// Composables
import { useMapImages } from "@/composables/useMapImages";
import { useTimeFilter } from "@/composables/useTimeFilter";

// State
const showSidebar = ref(false);
const showCacheStats = ref(false);
const imageLayerVisible = ref(true);
const boundaryLayerVisible = ref(true);

let map: L.Map | null = null;

// Composables
const mapImages = useMapImages();
const timeFilter = useTimeFilter();

// Computed properties
const cacheStats = computed(() => {
  if (!showCacheStats.value) return null;
  const stats = cacheService.getCacheStatistics();
  return {
    ...stats,
    totalSizeMB: (stats.totalSize / (1024 * 1024)).toFixed(2),
    maxSizeMB: (stats.maxSize / (1024 * 1024)).toFixed(0),
    usagePercent: ((stats.totalSize / stats.maxSize) * 100).toFixed(1),
  };
});

// Event handlers
onMounted(async () => {
  await timeFilter.initializeDateRanges();
  mapImages.exposeGlobalSelectImage();
});

async function onMapReady(mapInstance: L.Map) {
  map = mapInstance;
  mapImages.initializeLayers(map);
  await loadImagesForCurrentView();
}

async function onBoundsChanged() {
  // Debounce map updates to avoid too many API calls
  await new Promise((resolve) => setTimeout(resolve, 500));
  await loadImagesForCurrentView();
}

async function loadImagesForCurrentView() {
  if (!map) return;
  await mapImages.loadImagesForBounds(
    map.getBounds(),
    timeFilter.timeRange.value || undefined
  );
}

function onImageSelected(image: ImageMetadata) {
  mapImages.selectImage(image);
  showSidebar.value = true;
}

function closeSidebar() {
  showSidebar.value = false;
  mapImages.selectImage(null as any);
}

function toggleSidebar() {
  showSidebar.value = !showSidebar.value;
}

function toggleCacheStats() {
  showCacheStats.value = !showCacheStats.value;
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

// Time filter handlers
function onApplyTimeFilter() {
  loadImagesForCurrentView();
}

function onClearTimeFilter() {
  timeFilter.clearTimeFilter();
  loadImagesForCurrentView();
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
      <MapComponent @map-ready="onMapReady" @bounds-changed="onBoundsChanged" />
    </div>

    <!-- Overlays Component -->
    <MapOverlays
      :is-loading="mapImages.isLoading.value"
      :has-error="mapImages.hasError.value"
      :error="mapImages.error.value"
      :image-count="mapImages.imageCount.value"
      :show-cache-stats="showCacheStats"
      :cache-stats="cacheStats"
      :show-sidebar="showSidebar"
      :show-time-filter="timeFilter.showTimeFilter.value"
      @toggle-cache-stats="toggleCacheStats"
      @toggle-sidebar="toggleSidebar"
      @close-sidebar="closeSidebar"
    />

    <!-- Layer Controls -->
    <LayerControls
      :image-layer-visible="imageLayerVisible"
      :boundary-layer-visible="boundaryLayerVisible"
      :show-cache-stats="showCacheStats"
      @toggle-image-layer="onToggleImageLayer"
      @toggle-boundary-layer="onToggleBoundaryLayer"
    />

    <!-- Time Filter -->
    <TimeFilter
      :visible="timeFilter.showTimeFilter.value"
      :start-date="timeFilter.selectedStartDate.value"
      :end-date="timeFilter.selectedEndDate.value"
      :min-date="timeFilter.minDate.value"
      :max-date="timeFilter.maxDate.value"
      :show-cache-stats="showCacheStats"
      @toggle-visibility="timeFilter.toggleTimeFilter"
      @update:start-date="
        (value) => (timeFilter.selectedStartDate.value = value)
      "
      @update:end-date="(value) => (timeFilter.selectedEndDate.value = value)"
      @apply-filter="onApplyTimeFilter"
      @clear-filter="onClearTimeFilter"
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
