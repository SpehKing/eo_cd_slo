import { ref, computed } from 'vue'
import { defineStore } from 'pinia'
import { apiService } from '@/services/api'
import type { ImageMetadata, ImageQueryParams, BoundingBox } from '@/types/api'

export const useImageStore = defineStore('images', () => {
  // State
  const images = ref<ImageMetadata[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)
  const total = ref(0)
  const hasMore = ref(false)
  const currentOffset = ref(0)
  const limit = ref(50)

  // Computed
  const isLoading = computed(() => loading.value)
  const hasError = computed(() => error.value !== null)
  const imageCount = computed(() => images.value.length)

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
    const params: ImageQueryParams = {
      min_lon: bounds.minLon,
      min_lat: bounds.minLat,
      max_lon: bounds.maxLon,
      max_lat: bounds.maxLat,
      ...timeRange
    }

    await fetchImages(params)
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

  function getImageById(id: number): ImageMetadata | undefined {
    return images.value.find(img => img.id === id)
  }

  return {
    // State
    images,
    loading,
    error,
    total,
    hasMore,
    limit,
    // Computed
    isLoading,
    hasError,
    imageCount,
    // Actions
    fetchImages,
    fetchImagesByBounds,
    loadMoreImages,
    clearImages,
    getImageById
  }
})
