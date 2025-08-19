import { ref, computed } from "vue";
import { apiService } from "@/services/api";
import type { ChangeMaskMetadata, DateRangeResponse } from "@/types/api";

export function useAutoMaskLoader() {
  // Loading state
  const isLoading = ref(false);
  const currentStep = ref(0);
  const progress = ref(0);
  const hasError = ref(false);
  const errorMessage = ref("");

  // Mask loading statistics
  const totalMasks = ref(0);
  const loadedMasks = ref(0);
  const maskProgress = ref(0);
  const maskLoadSpeed = ref(0);
  const dateRange = ref<DateRangeResponse | null>(null);

  // Composite mask data
  const allMasks = ref<ChangeMaskMetadata[]>([]);
  const compositeMaskOverlays = ref<Map<string, { canvas: HTMLCanvasElement, bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number } }>>(new Map());
  const isCompositeVisible = ref(true);

  // Performance tracking
  const loadStartTime = ref(0);
  const lastProgressTime = ref(0);
  const lastLoadedCount = ref(0);

  // Computed properties
  const isComplete = computed(() => currentStep.value >= 4 && !isLoading.value);

  // Slovenia bounds - covering the entire country
  const SLOVENIA_BOUNDS = {
    minLon: 13.3736,  // Western border
    minLat: 45.4214,  // Southern border  
    maxLon: 16.6106,  // Eastern border
    maxLat: 46.8766,  // Northern border
  };

  // Initialize date ranges
  async function initializeDateRanges(): Promise<void> {
    currentStep.value = 1;
    updateProgress();

    try {
      const response = await apiService.fetchDateRange();
      dateRange.value = response;
      
      if (!response.min_date || !response.max_date) {
        throw new Error("No date range available");
      }
    } catch (error) {
      hasError.value = true;
      errorMessage.value = `Failed to initialize date ranges: ${error instanceof Error ? error.message : 'Unknown error'}`;
      throw error;
    }
  }

    // Load all change masks for Slovenia
  async function loadAllChangeMasks(): Promise<void> {
    currentStep.value = 2;
    updateProgress();

    if (!dateRange.value?.min_date || !dateRange.value?.max_date) {
      throw new Error("Date range not initialized");
    }

    loadStartTime.value = Date.now();
    lastProgressTime.value = loadStartTime.value;
    lastLoadedCount.value = 0;

    try {
      // Format dates properly for API (convert to Z format to avoid backend formatting issues)
      const startTime = new Date(dateRange.value.min_date).toISOString();
      const endTime = new Date(dateRange.value.max_date).toISOString();


      // First, get total count by making a request with limit=1
      const initialResponse = await apiService.fetchMasks({
        start_time: startTime,
        end_time: endTime,
        min_lon: SLOVENIA_BOUNDS.minLon,
        min_lat: SLOVENIA_BOUNDS.minLat,
        max_lon: SLOVENIA_BOUNDS.maxLon,
        max_lat: SLOVENIA_BOUNDS.maxLat,
        limit: 1,
        offset: 0,
      });

      totalMasks.value = initialResponse.total;
      
      if (totalMasks.value === 0) {
        console.warn("No change masks found for Slovenia");
        hasError.value = true;
        errorMessage.value = "No change detection masks found in the database. The system may not have processed any satellite image pairs yet.";
        return;
      }

      // Load all masks in batches
      const batchSize = 100; // API limit per request
      const allLoadedMasks: ChangeMaskMetadata[] = [];

      for (let offset = 0; offset < totalMasks.value; offset += batchSize) {
        const batchResponse = await apiService.fetchMasks({
          start_time: startTime,
          end_time: endTime,
          min_lon: SLOVENIA_BOUNDS.minLon,
          min_lat: SLOVENIA_BOUNDS.minLat,
          max_lon: SLOVENIA_BOUNDS.maxLon,
          max_lat: SLOVENIA_BOUNDS.maxLat,
          limit: batchSize,
          offset: offset,
        });

        allLoadedMasks.push(...batchResponse.masks);
        loadedMasks.value = allLoadedMasks.length;

        // Update progress and speed
        maskProgress.value = (loadedMasks.value / totalMasks.value) * 100;
        updateLoadSpeed();
        updateProgress();

        // Small delay to prevent overwhelming the API
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      allMasks.value = allLoadedMasks;

    } catch (error) {
      hasError.value = true;
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      
      if (errorMsg.includes('Internal Server Error') || errorMsg.includes('500')) {
        errorMessage.value = "Database error: The change detection service may not be properly configured or no satellite data has been processed yet.";
      } else if (errorMsg.includes('Failed to fetch')) {
        errorMessage.value = "Network error: Unable to connect to the change detection service. Please check your connection.";
      } else {
        errorMessage.value = `Failed to load change masks: ${errorMsg}`;
      }
      
      console.error("Change mask loading error:", error);
      throw error;
    }
  }

  // Create composite visualizations
  async function createCompositeVisualization(): Promise<void> {
    currentStep.value = 3;
    updateProgress();

    try {
      // Group masks by their exact spatial bounds
      const spatialGroups = groupMasksByExactBounds(allMasks.value);
      

      // Create composite mask for each spatial group
      for (const [boundsKey, maskGroup] of spatialGroups) {
        if (maskGroup.masks.length > 1) {
          // Only create composite if there are multiple masks in the same area
          await createCompositeMaskForGroup(boundsKey, maskGroup);
        }
      }


    } catch (error) {
      hasError.value = true;
      errorMessage.value = `Failed to create composite visualizations: ${error instanceof Error ? error.message : 'Unknown error'}`;
      throw error;
    }
  }

  // Group masks by their exact spatial bounds
  function groupMasksByExactBounds(masks: ChangeMaskMetadata[]): Map<string, { masks: ChangeMaskMetadata[], bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number } }> {
    const groups = new Map<string, { masks: ChangeMaskMetadata[], bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number } }>();

    masks.forEach(mask => {
      // Parse WKT to get exact bounds
      const bounds = parseWktToBounds(mask.bbox_wkt);
      if (!bounds) return;

      // Create a key based on the exact bounds (rounded to avoid floating point precision issues)
      const boundsKey = `${bounds.minLon.toFixed(6)}_${bounds.minLat.toFixed(6)}_${bounds.maxLon.toFixed(6)}_${bounds.maxLat.toFixed(6)}`;

      if (!groups.has(boundsKey)) {
        groups.set(boundsKey, { masks: [], bounds });
      }
      groups.get(boundsKey)!.masks.push(mask);
    });

    return groups;
  }

  // Parse WKT polygon to bounding box (simplified)
  function parseWktToBounds(wkt: string): { minLon: number; minLat: number; maxLon: number; maxLat: number } | null {
    try {
      // Extract coordinates from POLYGON WKT
      const coordsMatch = wkt.match(/POLYGON\s*\(\s*\(([\d\s\.,\-]+)\)\s*\)/);
      if (!coordsMatch) return null;

      const coordPairs = coordsMatch[1].split(',').map(pair => {
        const [lon, lat] = pair.trim().split(/\s+/).map(Number);
        return { lon, lat };
      });

      if (coordPairs.length === 0) return null;

      const lons = coordPairs.map(p => p.lon);
      const lats = coordPairs.map(p => p.lat);

      return {
        minLon: Math.min(...lons),
        minLat: Math.min(...lats),
        maxLon: Math.max(...lons),
        maxLat: Math.max(...lats),
      };
    } catch (error) {
      console.warn("Failed to parse WKT bounds:", error);
      return null;
    }
  }

  // Create composite mask for a group of overlapping masks
  async function createCompositeMaskForGroup(boundsKey: string, maskGroup: { masks: ChangeMaskMetadata[], bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number } }): Promise<void> {
    try {
      // Create a canvas for the composite
      const canvas = document.createElement('canvas');
      const ctx = canvas.getContext('2d');
      if (!ctx) return;

      // Set canvas size (you may want to adjust based on actual mask dimensions)
      canvas.width = 512;
      canvas.height = 512;

      // Load and composite all masks in this group
      const maskImages = await Promise.all(
        maskGroup.masks.map(async (mask) => {
          try {
            const maskUrl = await apiService.getMaskPreviewUrl(mask.img_a_id, mask.img_b_id);
            return loadImageFromUrl(maskUrl);
          } catch (error) {
            console.warn(`Failed to load mask ${mask.img_a_id}-${mask.img_b_id}:`, error);
            return null;
          }
        })
      );

      // Composite the masks using OR operation
      maskImages.forEach((img, index) => {
        if (!img) return;

        // Set blend mode for OR operation
        ctx.globalCompositeOperation = index === 0 ? 'source-over' : 'lighter';
        ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      });

      // Store the composite canvas with the bounds information
      compositeMaskOverlays.value.set(boundsKey, {
        canvas,
        bounds: maskGroup.bounds
      });

    } catch (error) {
      console.warn(`Failed to create composite for bounds ${boundsKey}:`, error);
    }
  }

  // Load image from URL
  function loadImageFromUrl(url: string): Promise<HTMLImageElement> {
    return new Promise((resolve, reject) => {
      const img = new Image();
      img.crossOrigin = 'anonymous';
      img.onload = () => resolve(img);
      img.onerror = reject;
      img.src = url;
    });
  }

  // Finalize preparation
  async function finalizePreparation(): Promise<void> {
    currentStep.value = 4;
    updateProgress();

    // Small delay to show completion
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  // Update loading progress
  function updateProgress(): void {
    const stepProgress = (currentStep.value / 4) * 100;
    const maskProgressContribution = currentStep.value === 2 ? (maskProgress.value * 0.6) : 0;
    progress.value = Math.min(100, stepProgress + maskProgressContribution);
  }

  // Update mask loading speed
  function updateLoadSpeed(): void {
    const now = Date.now();
    const timeDiff = (now - lastProgressTime.value) / 1000; // seconds
    const countDiff = loadedMasks.value - lastLoadedCount.value;

    if (timeDiff > 1) { // Update every second
      maskLoadSpeed.value = countDiff / timeDiff;
      lastProgressTime.value = now;
      lastLoadedCount.value = loadedMasks.value;
    }
  }

  // Main loading function
  async function loadAllData(): Promise<void> {
    isLoading.value = true;
    hasError.value = false;
    errorMessage.value = "";
    currentStep.value = 0;
    progress.value = 0;

    try {
      await initializeDateRanges();
      await loadAllChangeMasks();
      
      // Only create composite visualization if we have masks
      if (allMasks.value.length > 0) {
        await createCompositeVisualization();
      } else {
        // Skip composite creation if no masks
        currentStep.value = 3;
        updateProgress();
      }
      
      await finalizePreparation();
    } catch (error) {
      console.error("Failed to load auto mask data:", error);
      // Don't rethrow - let the error state be handled by the UI
    } finally {
      isLoading.value = false;
    }
  }

  // Continue without masks (graceful degradation)
  async function continueWithoutMasks(): Promise<void> {
    hasError.value = false;
    errorMessage.value = "";
    totalMasks.value = 0;
    allMasks.value = [];
    currentStep.value = 3;
    updateProgress();
    await finalizePreparation();
    isLoading.value = false;
  }

  // Retry loading
  async function retryLoading(): Promise<void> {
    // Reset state
    allMasks.value = [];
    compositeMaskOverlays.value.clear();
    totalMasks.value = 0;
    loadedMasks.value = 0;
    maskProgress.value = 0;
    maskLoadSpeed.value = 0;

    await loadAllData();
  }

  // Toggle composite visibility
  function toggleCompositeVisibility(): void {
    isCompositeVisible.value = !isCompositeVisible.value;
  }

  return {
    // State
    isLoading,
    currentStep,
    progress,
    hasError,
    errorMessage,
    totalMasks,
    loadedMasks,
    maskProgress,
    maskLoadSpeed,
    dateRange,
    allMasks,
    compositeMaskOverlays,
    isCompositeVisible,
    isComplete,

    // Methods
    loadAllData,
    retryLoading,
    continueWithoutMasks,
    toggleCompositeVisibility,
  };
}
