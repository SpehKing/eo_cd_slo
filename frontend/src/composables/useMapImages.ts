import { ref, computed } from "vue";
import L from "leaflet";
import type { ImageMetadata, BoundingBox, ChangeMaskMetadata } from "@/types/api";
import { useImageStore } from "@/stores/counter";
import { apiService } from "@/services/api";
import { parseWktPolygon, formatDate, formatFileSize } from "@/utils/helpers";

export function useMapImages() {
  const imageStore = useImageStore();
  const imageLayer = ref<L.LayerGroup | null>(null);
  const maskLayer = ref<L.LayerGroup | null>(null);
  const compositeMaskLayer = ref<L.LayerGroup | null>(null);
  const selectedImage = ref<ImageMetadata | null>(null);
  
  // Track image overlays by year for visibility toggling
  const imageOverlaysByYear = ref<Map<number, L.ImageOverlay[]>>(new Map());

  // Event handlers for image overlay clicks
  const onImageOverlayClick = ref<((image: ImageMetadata) => void) | null>(null);
  
  // Drawing mode state to disable mask clicks
  const isDrawingMode = ref(false);

  // Helper function to set pointer events on overlay
  function setOverlayPointerEvents(overlay: L.ImageOverlay, enabled: boolean) {
    // Try to set immediately
    const element = overlay.getElement();
    if (element) {
      element.style.pointerEvents = enabled ? 'auto' : 'none';
    } else {
      // If element not ready, wait for it to be added
      overlay.once('add', () => {
        const element = overlay.getElement();
        if (element) {
          element.style.pointerEvents = enabled ? 'auto' : 'none';
        }
      });
    }
  }

  // Initialize layers
  function initializeLayers(map: L.Map) {
    imageLayer.value = L.layerGroup();
    maskLayer.value = L.layerGroup();
    compositeMaskLayer.value = L.layerGroup();
    
    // Always add layers to map in order: images (bottom), masks (middle), composite masks (top)
    imageLayer.value.addTo(map);
    maskLayer.value.addTo(map);
    compositeMaskLayer.value.addTo(map);
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

  async function loadMasksForMultipleBounds(boundsList: L.LatLngBounds[], timeRange?: { start?: string; end?: string }) {
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
      await imageStore.fetchMasksByBounds(combinedBounds, timeRange);
      await displayMasksOnMap();
    } catch (error) {
      console.error("Failed to load masks for multiple bounds:", error);
    }
  }

  // Display images on map
  async function displayImagesOnMap() {
    if (!imageLayer.value) return;

    // Clear existing overlays
    imageLayer.value.clearLayers();
    imageOverlaysByYear.value.clear();

    // Add each image as image overlay only
    for (const image of imageStore.images) {
      const polygonData = parseWktPolygon(image.bbox_wkt);
      if (!polygonData) continue;

      try {
        // Get image URL (async, but might be from cache)
        const imageUrl = await apiService.getImagePreviewUrl(image.id);

        // Create image overlay
        const imageOverlay = L.imageOverlay(imageUrl, polygonData.bounds, {
          interactive: true,
          crossOrigin: true,
        });

        // Set pointer events based on drawing mode
        setOverlayPointerEvents(imageOverlay, !isDrawingMode.value);

        // Add click handler to image overlay
        imageOverlay.on("click", () => {
          // Don't handle image clicks when in drawing mode
          if (isDrawingMode.value) {
            return;
          }
          
          if (onImageOverlayClick.value) {
            onImageOverlayClick.value(image);
          } else {
            // Fallback to old behavior if no handler is set
            selectImage(image);
          }
        });

        // Add image overlay to image layer
        imageLayer.value?.addLayer(imageOverlay);
        
        // Track overlay by year
        const year = new Date(image.time).getFullYear();
        if (!imageOverlaysByYear.value.has(year)) {
          imageOverlaysByYear.value.set(year, []);
        }
        imageOverlaysByYear.value.get(year)!.push(imageOverlay);
      } catch (error) {
        console.warn(`Failed to load image overlay for image ${image.id}:`, error);
      }
    }
  }

  // Display masks on map (always on top of images)
  async function displayMasksOnMap() {
    if (!maskLayer.value) return;

    // Clear existing mask overlays
    maskLayer.value.clearLayers();

    // Add each mask as image overlay
    for (const mask of imageStore.masks) {
      const polygonData = parseWktPolygon(mask.bbox_wkt);
      if (!polygonData) continue;

      try {
        // Get mask preview URL
        const maskUrl = await apiService.getMaskPreviewUrl(mask.img_a_id, mask.img_b_id);

        // Create mask overlay with higher opacity and ensure it's on top
        const maskOverlay = L.imageOverlay(maskUrl, polygonData.bounds, {
          opacity: 0.8, // Higher opacity for visibility
          interactive: true,
          crossOrigin: true,
          zIndex: 1000, // Ensure masks are above images
        });

        // Set pointer events based on drawing mode
        setOverlayPointerEvents(maskOverlay, !isDrawingMode.value);

        // Add click handler to mask overlay
        maskOverlay.on("click", () => {
          // Don't handle mask clicks when in drawing mode
          if (isDrawingMode.value) {
            return;
          }

          if (onImageOverlayClick.value) {
            const previousImage = imageStore.getImageById(mask.img_a_id);
            const currentImage = imageStore.getImageById(mask.img_b_id);
            if (previousImage && currentImage) {
              onImageOverlayClick.value(previousImage); // Trigger click on previous image
            }
          }
        });

        // Add mask overlay to mask layer
        maskLayer.value?.addLayer(maskOverlay);
      } catch (error) {
        console.warn(`Failed to load mask overlay for images ${mask.img_a_id}-${mask.img_b_id}:`, error);
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

  // Toggle visibility of images for a specific year
  function toggleYearVisibility(year: number, visible: boolean) {
    const overlays = imageOverlaysByYear.value.get(year);
    if (!overlays) return;

    overlays.forEach(overlay => {
      if (visible) {
        overlay.setOpacity(1);
        // Make sure it's part of the DOM
        if (overlay.getElement()) {
          overlay.getElement()!.style.display = 'block';
        }
      } else {
        overlay.setOpacity(0);
        // Hide from DOM
        if (overlay.getElement()) {
          overlay.getElement()!.style.display = 'none';
        }
      }
    });
  }

  // Set the handler for image overlay clicks
  function setImageOverlayClickHandler(handler: (image: ImageMetadata) => void) {
    onImageOverlayClick.value = handler;
  }

  // Display composite masks on map
  function displayCompositeMasks(compositeMasks: Map<string, { canvas: HTMLCanvasElement, bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number } }>, isVisible: boolean = true) {
    if (!compositeMaskLayer.value) return;

    // Clear existing composite overlays
    compositeMaskLayer.value.clearLayers();

    if (!isVisible || compositeMasks.size === 0) return;

    // Add each composite mask as image overlay
    compositeMasks.forEach((maskData, boundsKey) => {
      try {
        // Convert canvas to blob URL
        maskData.canvas.toBlob((blob) => {
          if (!blob) return;

          const url = URL.createObjectURL(blob);
          
          // Use the actual bounds from the mask data
          const bounds = L.latLngBounds(
            [maskData.bounds.minLat, maskData.bounds.minLon],
            [maskData.bounds.maxLat, maskData.bounds.maxLon]
          );

          // Create composite mask overlay with high opacity and ensure it's on top
          const maskOverlay = L.imageOverlay(url, bounds, {
            opacity: 0.7,
            interactive: true,
            crossOrigin: true,
            zIndex: 2000, // Higher than regular masks
          });

          // Set pointer events based on drawing mode
          setOverlayPointerEvents(maskOverlay, !isDrawingMode.value);

          // Add click handler for composite mask
          maskOverlay.on("click", () => {
            // Don't handle composite mask clicks when in drawing mode
            if (isDrawingMode.value) {
              return;
            }
            
          });

          // Add to composite mask layer
          compositeMaskLayer.value?.addLayer(maskOverlay);
        }, 'image/png');

      } catch (error) {
        console.warn(`Failed to display composite mask for bounds ${boundsKey}:`, error);
      }
    });
  }

  // Toggle composite mask visibility
  function toggleCompositeMaskVisibility(visible: boolean) {
    if (!compositeMaskLayer.value) return;

    compositeMaskLayer.value.eachLayer((layer) => {
      if (layer instanceof L.ImageOverlay) {
        layer.setOpacity(visible ? 0.7 : 0);
      }
    });
  }

  // Clear composite masks
  function clearCompositeMasks() {
    if (compositeMaskLayer.value) {
      compositeMaskLayer.value.clearLayers();
    }
  }

  // Set drawing mode to disable/enable mask clicks
  function setDrawingMode(drawingMode: boolean) {
    isDrawingMode.value = drawingMode;
    
    // Update pointer events on all existing overlays
    if (imageLayer.value) {
      imageLayer.value.eachLayer((layer) => {
        if (layer instanceof L.ImageOverlay) {
          setOverlayPointerEvents(layer, !drawingMode);
        }
      });
    }
    
    if (maskLayer.value) {
      maskLayer.value.eachLayer((layer) => {
        if (layer instanceof L.ImageOverlay) {
          setOverlayPointerEvents(layer, !drawingMode);
        }
      });
    }
    
    if (compositeMaskLayer.value) {
      compositeMaskLayer.value.eachLayer((layer) => {
        if (layer instanceof L.ImageOverlay) {
          setOverlayPointerEvents(layer, !drawingMode);
        }
      });
    }
  }

  return {
    // State
    selectedImage,
    imagePreviewUrl,
    
    // Store getters
    images: computed(() => imageStore.images),
    masks: computed(() => imageStore.masks),
    imageCount: computed(() => imageStore.imageCount),
    maskCount: computed(() => imageStore.maskCount),
    isLoading: computed(() => imageStore.isLoading),
    isMasksLoading: computed(() => imageStore.isMasksLoading),
    hasError: computed(() => imageStore.hasError),
    error: computed(() => imageStore.error),
    
    // Methods
    initializeLayers,
    loadImagesForBounds,
    loadImagesForMultipleBounds,
    loadMasksForMultipleBounds,
    displayImagesOnMap,
    displayMasksOnMap,
    displayCompositeMasks,
    toggleCompositeMaskVisibility,
    clearCompositeMasks,
    selectImage,
    getImageById,
    downloadOriginalImage,
    exposeGlobalSelectImage,
    toggleYearVisibility,
    setImageOverlayClickHandler,
    setDrawingMode,
  };
}
