import { ref, computed } from 'vue'
import { defineStore } from 'pinia'
import { apiService } from '@/services/api'
import { cacheService } from '@/services/cache'
import type { ImageMetadata, ImageQueryParams, BoundingBox, ChangeMaskMetadata } from '@/types/api'

export const useImageStore = defineStore('images', () => {
  // State
  const images = ref<ImageMetadata[]>([])
  const masks = ref<ChangeMaskMetadata[]>([])
  const loading = ref(false)
  const masksLoading = ref(false)
  const error = ref<string | null>(null)
  const total = ref(0)
  const totalMasks = ref(0)
  const hasMore = ref(false)
  const hasMoreMasks = ref(false)
  const currentOffset = ref(0)
  const currentMasksOffset = ref(0)
  const limit = ref(50)

  // Computed
  const isLoading = computed(() => loading.value)
  const isMasksLoading = computed(() => masksLoading.value)
  const hasError = computed(() => error.value !== null)
  const imageCount = computed(() => images.value.length)
  const maskCount = computed(() => masks.value.length)

  // Actions
  async function fetchImages(params: ImageQueryParams = {}, append = false) {
    try {
      loading.value = true
      error.value = null

      const queryParams = {
        ...params,
        limit: limit.value,
        offset: append ? currentOffset.value : 0
      }

      const response = await apiService.fetchImages(queryParams)
      
      if (append) {
        images.value.push(...response.images)
      } else {
        images.value = response.images
        currentOffset.value = 0
      }

      total.value = response.total
      hasMore.value = response.has_more
      currentOffset.value += response.images.length

    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch images'
      console.error('Error fetching images:', err)
    } finally {
      loading.value = false
    }
  }

  async function fetchImagesByBounds(bounds: BoundingBox, timeRange?: { start?: string, end?: string }) {
    const boundsKey = cacheService.generateBoundsKey(bounds);
    
    // Generate cache key that includes time range for proper caching
    const cacheKey = timeRange ? `${boundsKey}_${timeRange.start || ''}_${timeRange.end || ''}` : boundsKey;
    
    // Check cache first
    const cachedImages = cacheService.getCachedImagesByBounds(cacheKey);
    if (cachedImages) {
      images.value = cachedImages;
      total.value = cachedImages.length;
      hasMore.value = false;
      currentOffset.value = cachedImages.length;
      return;
    }

    // Fetch from API if not cached
    const params: ImageQueryParams = {
      min_lon: bounds.minLon,
      min_lat: bounds.minLat,
      max_lon: bounds.maxLon,
      max_lat: bounds.maxLat,
      start_time: timeRange?.start,
      end_time: timeRange?.end
    }

    try {
      loading.value = true;
      error.value = null;

      const response = await apiService.fetchImages(params);
      
      images.value = response.images;
      total.value = response.total;
      hasMore.value = response.has_more;
      currentOffset.value = response.images.length;

      // Cache the results
      cacheService.cacheImageMetadata(response.images, cacheKey);

    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch images'
      console.error('Error fetching images:', err)
    } finally {
      loading.value = false
    }
  }

  async function fetchMasksByBounds(bounds: BoundingBox, timeRange?: { start?: string, end?: string }) {
    const boundsKey = cacheService.generateBoundsKey(bounds);
    //fetch from api
    const params: ImageQueryParams = {
      min_lon: bounds.minLon,
      min_lat: bounds.minLat,
      max_lon: bounds.maxLon,
      max_lat: bounds.maxLat,
      start_time: timeRange?.start,
      end_time: timeRange?.end
    }

    try{
      masksLoading.value = true;
      error.value = null;

      const response = await apiService.fetchMasks(params);
      
      masks.value = response.masks;
      totalMasks.value = response.total;
      hasMoreMasks.value = response.has_more;
      currentMasksOffset.value = response.masks.length;


    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch masks'
      console.error('Error fetching masks:', err)
    } finally {
      masksLoading.value = false
    }
  }

  async function loadMoreImages(params: ImageQueryParams = {}) {
    if (!hasMore.value || loading.value) return
    await fetchImages(params, true)
  }

  function clearImages() {
    images.value = []
    total.value = 0
    hasMore.value = false
    currentOffset.value = 0
    error.value = null
  }

  function clearMasks() {
    masks.value = []
    totalMasks.value = 0
    hasMoreMasks.value = false
    currentMasksOffset.value = 0
  }

  function getImageById(id: number): ImageMetadata | undefined {
    return images.value.find(img => img.id === id)
  }

  // Preload image previews for current images
  async function preloadImagePreviews() {
    const imageIds = images.value
      .filter(img => !cacheService.hasImagePreview(img.id))
      .map(img => img.id);

    if (imageIds.length === 0) return;

    
    // Preload in batches to avoid overwhelming the server
    const batchSize = 5;
    for (let i = 0; i < imageIds.length; i += batchSize) {
      const batch = imageIds.slice(i, i + batchSize);
      
      await Promise.allSettled(
        batch.map(async (imageId) => {
          try {
            await apiService.getImagePreviewUrl(imageId);
          } catch (error) {
            console.warn(`Failed to preload preview for image ${imageId}:`, error);
          }
        })
      );
      
      // Small delay between batches
      if (i + batchSize < imageIds.length) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
  }

  return {
    // State
    images,
    masks,
    loading,
    masksLoading,
    error,
    total,
    totalMasks,
    hasMore,
    hasMoreMasks,
    limit,
    // Computed
    isLoading,
    isMasksLoading,
    hasError,
    imageCount,
    maskCount,
    // Actions
    fetchImages,
    fetchImagesByBounds,
    fetchMasksByBounds,
    loadMoreImages,
    clearImages,
    clearMasks,
    getImageById,
    preloadImagePreviews
  }
})
