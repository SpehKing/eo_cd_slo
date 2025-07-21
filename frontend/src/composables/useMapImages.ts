import { ref, computed } from "vue";
import L from "leaflet";
import type { ImageMetadata, BoundingBox } from "@/types/api";
import { useImageStore } from "@/stores/counter";
import { apiService } from "@/services/api";
import { parseWktPolygon, formatDate, formatFileSize } from "@/utils/helpers";

export function useMapImages() {
  const imageStore = useImageStore();
  const imageLayer = ref<L.LayerGroup | null>(null);
  const selectedImage = ref<ImageMetadata | null>(null);

  // Initialize layers
  function initializeLayers(map: L.Map) {
    imageLayer.value = L.layerGroup();
    
    imageLayer.value.addTo(map);
  }

  // Load images for current bounds and time range
  async function loadImagesForBounds(bounds: L.LatLngBounds, timeRange?: { start?: string; end?: string }) {
    const boundsObj: BoundingBox = {
      minLon: bounds.getWest(),
      minLat: bounds.getSouth(),
      maxLon: bounds.getEast(),
      maxLat: bounds.getNorth(),
    };

    try {
      await imageStore.fetchImagesByBounds(boundsObj, timeRange);
      await displayImagesOnMap();
      imageStore.preloadImagePreviews();
    } catch (error) {
      console.error("Failed to load images:", error);
    }
  }

  // Load images for multiple bounds at once by combining them into a single query
  async function loadImagesForMultipleBounds(boundsList: L.LatLngBounds[], timeRange?: { start?: string; end?: string }) {
    if (boundsList.length === 0) return;

    // Combine all bounds into a single bounding box that encompasses all selected areas
    let minLon = Infinity;
    let minLat = Infinity;
    let maxLon = -Infinity;
    let maxLat = -Infinity;

    boundsList.forEach(bounds => {
      minLon = Math.min(minLon, bounds.getWest());
      minLat = Math.min(minLat, bounds.getSouth());
      maxLon = Math.max(maxLon, bounds.getEast());
      maxLat = Math.max(maxLat, bounds.getNorth());
    });

    const combinedBounds: BoundingBox = {
      minLon,
      minLat,
      maxLon,
      maxLat,
    };

    try {
      await imageStore.fetchImagesByBounds(combinedBounds, timeRange);
      await displayImagesOnMap();
      imageStore.preloadImagePreviews();
    } catch (error) {
      console.error("Failed to load images for multiple bounds:", error);
    }
  }

  // Display images on map
  async function displayImagesOnMap() {
    if (!imageLayer.value) return;

    // Clear existing overlays
    imageLayer.value.clearLayers();

    // Add each image as image overlay only
    for (const image of imageStore.images) {
      const polygonData = parseWktPolygon(image.bbox_wkt);
      if (!polygonData) continue;

      try {
        // Get image URL (async, but might be from cache)
        const imageUrl = await apiService.getImagePreviewUrl(image.id);

        // Create image overlay
        const imageOverlay = L.imageOverlay(imageUrl, polygonData.bounds, {
          opacity: 0.7,
          interactive: true,
          crossOrigin: true,
        });

        // Add click handler to image overlay
        imageOverlay.on("click", () => {
          selectImage(image);
        });

        // Add image overlay to image layer
        imageLayer.value?.addLayer(imageOverlay);
      } catch (error) {
        console.warn(`Failed to load image overlay for image ${image.id}:`, error);
      }
    }
  }

  // Create popup content for image
  function createPopupContent(image: ImageMetadata): string {
    return `
      <div class="p-2 min-w-[200px]">
        <h3 class="font-bold text-sm mb-2">Satellite Image</h3>
        <p class="text-xs mb-1"><strong>Date:</strong> ${formatDate(image.time)}</p>
        <p class="text-xs mb-1"><strong>Size:</strong> ${formatFileSize(image.size_bytes)}</p>
        <p class="text-xs mb-1"><strong>ID:</strong> ${image.id}</p>
        <button 
          onclick="window.selectImage(${image.id})" 
          class="mt-2 px-3 py-1 bg-blue-500 text-white text-xs rounded hover:bg-blue-600"
        >
          View Details
        </button>
      </div>
    `;
  }

  // Select an image
  function selectImage(image: ImageMetadata) {
    selectedImage.value = image;
  }

  // Get image by ID
  function getImageById(imageId: number): ImageMetadata | undefined {
    return imageStore.getImageById(imageId);
  }

  // Toggle layer visibility
  function toggleImageLayer(map: L.Map, visible: boolean) {
    if (imageLayer.value) {
      if (visible) {
        imageLayer.value.addTo(map);
      } else {
        map.removeLayer(imageLayer.value as unknown as L.Layer);
      }
    }
  }

  // Download original image
  async function downloadOriginalImage(image: ImageMetadata) {
    try {
      const url = `${apiService["baseUrl"]}/images/${image.id}?format=original`;
      const link = document.createElement("a");
      link.href = url;
      link.download = `sentinel2_${image.id}_${formatDate(image.time).replace(/\s+/g, "_")}.tif`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (error) {
      console.error("Failed to download image:", error);
    }
  }

  // Get image preview URL
  const imagePreviewUrl = computed(() =>
    selectedImage.value
      ? apiService.getImagePreviewUrlSync(selectedImage.value.id)
      : null
  );

  // Expose global selectImage function for popup buttons
  function exposeGlobalSelectImage() {
    (window as any).selectImage = (imageId: number) => {
      const image = getImageById(imageId);
      if (image) {
        selectImage(image);
      }
    };
  }

  return {
    // State
    selectedImage,
    imagePreviewUrl,
    
    // Store getters
    images: computed(() => imageStore.images),
    imageCount: computed(() => imageStore.imageCount),
    isLoading: computed(() => imageStore.isLoading),
    hasError: computed(() => imageStore.hasError),
    error: computed(() => imageStore.error),
    
    // Methods
    initializeLayers,
    loadImagesForBounds,
    loadImagesForMultipleBounds,
    displayImagesOnMap,
    selectImage,
    getImageById,
    toggleImageLayer,
    downloadOriginalImage,
    exposeGlobalSelectImage,
  };
}
