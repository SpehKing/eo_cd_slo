<script lang="ts" setup>
import { onMounted, onBeforeUnmount, ref, computed } from "vue";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useImageStore } from "@/stores/counter";
import {
  parseWktPolygon,
  formatDate,
  formatFileSize,
  getImageColor,
} from "@/utils/helpers";
import { apiService } from "@/services/api";
import type { ImageMetadata } from "@/types/api";

const mapContainer = ref<HTMLElement>();
const selectedImage = ref<ImageMetadata | null>(null);
const showSidebar = ref(false);
const imageLayer = ref<L.LayerGroup | null>(null);

let map: L.Map | null = null;

const imageStore = useImageStore();

// Computed properties
const sidebarClass = computed(() => ({
  "translate-x-0": showSidebar.value,
  "translate-x-full": !showSidebar.value,
}));

const imagePreviewUrl = computed(() =>
  selectedImage.value
    ? apiService.getImagePreviewUrl(selectedImage.value.id)
    : null
);

onMounted(async () => {
  if (mapContainer.value) {
    // Initialize the map centered on Slovenia
    map = L.map(mapContainer.value, {
      center: [46.0569, 14.5058], // Ljubljana, Slovenia
      zoom: 8,
      zoomControl: true,
      attributionControl: true,
    });

    // Add OpenStreetMap tile layer
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 19,
    }).addTo(map);

    // Fix for default markers not showing up
    delete (L.Icon.Default.prototype as any)._getIconUrl;
    L.Icon.Default.mergeOptions({
      iconRetinaUrl:
        "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png",
      iconUrl:
        "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png",
      shadowUrl:
        "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
    });

    // Create a layer group for image overlays
    imageLayer.value = L.layerGroup().addTo(map);

    // Load images for current map bounds
    await loadImagesForCurrentView();

    // Add map event listeners
    map.on("moveend", onMapMoveEnd);
    map.on("zoomend", onMapMoveEnd);
  }
});

onBeforeUnmount(() => {
  if (map) {
    map.off("moveend", onMapMoveEnd);
    map.off("zoomend", onMapMoveEnd);
  }
});

async function loadImagesForCurrentView() {
  if (!map) return;

  const bounds = map.getBounds();
  const boundsObj = {
    minLon: bounds.getWest(),
    minLat: bounds.getSouth(),
    maxLon: bounds.getEast(),
    maxLat: bounds.getNorth(),
  };

  try {
    await imageStore.fetchImagesByBounds(boundsObj);
    displayImagesOnMap();
  } catch (error) {
    console.error("Failed to load images:", error);
  }
}

function displayImagesOnMap() {
  if (!map || !imageLayer.value) return;

  // Clear existing image overlays
  imageLayer.value.clearLayers();

  // Add each image as a polygon overlay
  imageStore.images.forEach((image) => {
    const polygonData = parseWktPolygon(image.bbox_wkt);
    if (!polygonData) return;

    const color = getImageColor(image.time);

    // Create polygon overlay
    const polygon = L.polygon(polygonData.bounds, {
      color: color,
      weight: 2,
      opacity: 0.8,
      fillOpacity: 0.3,
      fillColor: color,
    });

    // Add popup with image info
    const popupContent = `
      <div class="p-2 min-w-[200px]">
        <h3 class="font-bold text-sm mb-2">Satellite Image</h3>
        <p class="text-xs mb-1"><strong>Date:</strong> ${formatDate(
          image.time
        )}</p>
        <p class="text-xs mb-1"><strong>Size:</strong> ${formatFileSize(
          image.size_bytes
        )}</p>
        <button 
          onclick="window.selectImage(${image.id})" 
          class="mt-2 px-3 py-1 bg-blue-500 text-white text-xs rounded hover:bg-blue-600"
        >
          View Details
        </button>
      </div>
    `;

    polygon.bindPopup(popupContent);

    // Add click handler
    polygon.on("click", () => {
      selectImageHandler(image);
    });

    // Add to layer group
    imageLayer.value?.addLayer(polygon);
  });
}

function selectImageHandler(image: ImageMetadata) {
  selectedImage.value = image;
  showSidebar.value = true;
}

// Make selectImage function globally available for popup buttons
(window as any).selectImage = (imageId: number) => {
  const image = imageStore.getImageById(imageId);
  if (image) {
    selectImageHandler(image);
  }
};

async function onMapMoveEnd() {
  // Debounce map updates to avoid too many API calls
  await new Promise((resolve) => setTimeout(resolve, 500));
  await loadImagesForCurrentView();
}

function closeSidebar() {
  showSidebar.value = false;
  selectedImage.value = null;
}

function toggleSidebar() {
  showSidebar.value = !showSidebar.value;
}

// Download original image
async function downloadOriginalImage() {
  if (!selectedImage.value) return;

  try {
    const url = `${apiService["baseUrl"]}/images/${selectedImage.value.id}?format=original`;
    const link = document.createElement("a");
    link.href = url;
    link.download = `sentinel2_${selectedImage.value.id}_${formatDate(
      selectedImage.value.time
    ).replace(/\s+/g, "_")}.tif`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  } catch (error) {
    console.error("Failed to download image:", error);
  }
}
</script>

<template>
  <div class="relative">
    <!-- Map Container -->
    <div class="map-container">
      <div ref="mapContainer" class="map"></div>
    </div>

    <!-- Loading Overlay -->
    <div
      v-if="imageStore.isLoading"
      class="absolute top-4 left-4 bg-white bg-opacity-90 px-4 py-2 rounded-lg shadow-lg z-[1000]"
    >
      <div class="flex items-center space-x-2">
        <div
          class="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"
        ></div>
        <span class="text-sm">Loading images...</span>
      </div>
    </div>

    <!-- Error Message -->
    <div
      v-if="imageStore.hasError"
      class="absolute top-4 left-4 bg-red-100 border border-red-400 text-red-700 px-4 py-2 rounded-lg shadow-lg z-[1000]"
    >
      <p class="text-sm">{{ imageStore.error }}</p>
    </div>

    <!-- Image Count Display -->
    <div
      class="absolute top-4 right-4 bg-white bg-opacity-90 px-4 py-2 rounded-lg shadow-lg z-[1000]"
    >
      <p class="text-sm font-medium">
        {{ imageStore.imageCount }} images found
      </p>
    </div>

    <!-- Sidebar Toggle Button -->
    <button
      @click="toggleSidebar"
      class="absolute top-20 right-4 bg-blue-500 hover:bg-blue-600 text-white p-2 rounded-lg shadow-lg z-[1000] transition-colors"
    >
      <svg
        class="w-5 h-5"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
      >
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M4 6h16M4 12h16M4 18h16"
        ></path>
      </svg>
    </button>

    <!-- Image Details Sidebar -->
    <div
      class="fixed top-0 right-0 h-full w-96 bg-white shadow-xl z-[1000] transform transition-transform duration-300 ease-in-out"
      :class="sidebarClass"
    >
      <div class="flex flex-col h-full">
        <!-- Header -->
        <div class="flex items-center justify-between p-4 border-b">
          <h2 class="text-lg font-semibold">Image Details</h2>
          <button @click="closeSidebar" class="p-1 hover:bg-gray-100 rounded">
            <svg
              class="w-5 h-5"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M6 18L18 6M6 6l12 12"
              ></path>
            </svg>
          </button>
        </div>

        <!-- Content -->
        <div v-if="selectedImage" class="flex-1 overflow-y-auto p-4">
          <!-- Image Preview -->
          <div class="mb-4">
            <img
              v-if="imagePreviewUrl"
              :src="imagePreviewUrl"
              :alt="`Satellite image from ${formatDate(selectedImage.time)}`"
              class="w-full h-48 object-cover rounded-lg border"
              @error="(event) => { const target = event.target as HTMLImageElement; if (target) target.src = '/placeholder-image.jpg'; }"
            />
          </div>

          <!-- Image Information -->
          <div class="space-y-3">
            <div>
              <label class="text-sm font-medium text-gray-600">Date</label>
              <p class="text-sm">{{ formatDate(selectedImage.time) }}</p>
            </div>

            <div>
              <label class="text-sm font-medium text-gray-600">Image ID</label>
              <p class="text-sm">{{ selectedImage.id }}</p>
            </div>

            <div>
              <label class="text-sm font-medium text-gray-600">File Size</label>
              <p class="text-sm">
                {{ formatFileSize(selectedImage.size_bytes) }}
              </p>
            </div>

            <div v-if="selectedImage.width && selectedImage.height">
              <label class="text-sm font-medium text-gray-600"
                >Dimensions</label
              >
              <p class="text-sm">
                {{ selectedImage.width }} × {{ selectedImage.height }} pixels
              </p>
            </div>

            <div v-if="selectedImage.data_type">
              <label class="text-sm font-medium text-gray-600">Data Type</label>
              <p class="text-sm">{{ selectedImage.data_type }}</p>
            </div>

            <div>
              <label class="text-sm font-medium text-gray-600"
                >Bounding Box</label
              >
              <p class="text-xs text-gray-500 font-mono break-all">
                {{ selectedImage.bbox_wkt }}
              </p>
            </div>
          </div>

          <!-- Actions -->
          <div class="mt-6 space-y-2">
            <button
              @click="downloadOriginalImage"
              class="w-full bg-blue-500 hover:bg-blue-600 text-white py-2 px-4 rounded-lg transition-colors"
            >
              Download Original Image
            </button>
          </div>
        </div>

        <!-- No image selected state -->
        <div v-else class="flex-1 flex items-center justify-center p-4">
          <div class="text-center text-gray-500">
            <svg
              class="w-12 h-12 mx-auto mb-2"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"
              ></path>
            </svg>
            <p>Click on an image overlay to view details</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Sidebar Overlay (for mobile) -->
    <div
      v-if="showSidebar"
      @click="closeSidebar"
      class="fixed inset-0 bg-black bg-opacity-50 z-[999] lg:hidden"
    ></div>
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
