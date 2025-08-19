<template>
  <div
    v-if="visible"
    class="fixed inset-0 bg-black bg-opacity-50 z-[9999] flex items-center justify-center p-4"
    @click="handleBackdropClick"
  >
    <div
      class="bg-white rounded-lg shadow-xl max-w-6xl w-full max-h-[90vh] overflow-hidden"
      @click.stop
    >
      <!-- Modal Header -->
      <div class="flex items-center justify-between p-4 border-b">
        <h2 class="text-xl font-semibold text-gray-800">
          Image Comparison: {{ formatYearRange() }}
        </h2>
        <button
          @click="$emit('close')"
          class="text-gray-400 hover:text-gray-600 p-1 rounded-full hover:bg-gray-100"
        >
          <svg
            class="w-6 h-6"
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

      <!-- Modal Content -->
      <div class="p-4">
        <div v-if="isLoading" class="flex items-center justify-center h-64">
          <div class="text-center">
            <div
              class="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto mb-2"
            ></div>
            <p class="text-gray-600">Loading comparison images...</p>
          </div>
        </div>

        <div v-else-if="hasError" class="flex items-center justify-center h-64">
          <div class="text-center text-red-600">
            <p class="mb-2">Failed to load comparison data</p>
            <p class="text-sm">{{ errorMessage }}</p>
          </div>
        </div>

        <div v-else-if="comparisonData" class="space-y-4">
          <!-- Image Panels -->
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <!-- Previous Year Panel -->
            <div class="space-y-2">
              <div class="flex items-center justify-between">
                <h3 class="text-lg font-medium text-gray-700">
                  Previous Year:
                  {{ formatDate(comparisonData.previousImage.time) }}
                </h3>
                <span class="text-sm text-gray-500">
                  ID: {{ comparisonData.previousImage.id }}
                </span>
              </div>
              <div
                class="relative bg-gray-100 rounded-lg overflow-hidden"
                style="aspect-ratio: 1"
              >
                <img
                  v-if="comparisonData.previousImageUrl"
                  :src="comparisonData.previousImageUrl"
                  :alt="`Image from ${formatDate(
                    comparisonData.previousImage.time
                  )}`"
                  class="w-full h-full object-cover"
                  @error="handleImageError"
                />
                <div
                  v-else
                  class="flex items-center justify-center h-full text-gray-500"
                >
                  Image not available
                </div>
              </div>
            </div>

            <!-- Current Year Panel -->
            <div class="space-y-2">
              <div class="flex items-center justify-between">
                <h3 class="text-lg font-medium text-gray-700">
                  Current Year:
                  {{ formatDate(comparisonData.currentImage.time) }}
                </h3>
                <span class="text-sm text-gray-500">
                  ID: {{ comparisonData.currentImage.id }}
                </span>
              </div>
              <div
                class="relative bg-gray-100 rounded-lg overflow-hidden"
                style="aspect-ratio: 1"
              >
                <!-- Base Current Image -->
                <img
                  v-if="comparisonData.currentImageUrl"
                  :src="comparisonData.currentImageUrl"
                  :alt="`Image from ${formatDate(
                    comparisonData.currentImage.time
                  )}`"
                  class="w-full h-full object-cover"
                  @error="handleImageError"
                />

                <!-- Change Mask Overlay -->
                <div
                  v-if="showMaskOverlay && comparisonData.changeMaskUrl"
                  class="absolute inset-0"
                  :style="{ opacity: maskOpacity }"
                >
                  <img
                    :src="comparisonData.changeMaskUrl"
                    alt="Change detection mask"
                    class="w-full h-full object-cover"
                    style="mix-blend-mode: multiply"
                  />
                </div>

                <!-- No Image Fallback -->
                <div
                  v-if="!comparisonData.currentImageUrl"
                  class="flex items-center justify-center h-full text-gray-500"
                >
                  Image not available
                </div>
              </div>
            </div>
          </div>

          <!-- Change Mask Controls -->
          <div
            v-if="comparisonData.changeMask"
            class="bg-gray-50 rounded-lg p-4"
          >
            <div class="flex items-center justify-between mb-3">
              <h4 class="text-md font-medium text-gray-700">
                Change Detection Overlay
              </h4>
              <div class="flex items-center space-x-2">
                <span class="text-sm text-gray-600">
                  Period:
                  {{ formatDate(comparisonData.changeMask.period_start) }} -
                  {{ formatDate(comparisonData.changeMask.period_end) }}
                </span>
              </div>
            </div>

            <div class="flex items-center space-x-4">
              <!-- Toggle Switch -->
              <label class="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  v-model="showMaskOverlay"
                  class="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                />
                <span class="text-sm text-gray-700"
                  >Show change mask overlay</span
                >
              </label>

              <!-- Opacity Slider -->
              <div
                v-if="showMaskOverlay"
                class="flex items-center space-x-2 flex-1 max-w-xs"
              >
                <span class="text-sm text-gray-600">Opacity:</span>
                <input
                  type="range"
                  v-model="maskOpacity"
                  min="0.1"
                  max="1"
                  step="0.1"
                  class="flex-1"
                />
                <span class="text-sm text-gray-600 w-8"
                  >{{ Math.round(maskOpacity * 100) }}%</span
                >
              </div>
            </div>
          </div>

          <!-- No Change Mask Available -->
          <div
            v-else
            class="bg-yellow-50 border border-yellow-200 rounded-lg p-4"
          >
            <div class="flex items-center">
              <svg
                class="w-5 h-5 text-yellow-400 mr-2"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fill-rule="evenodd"
                  d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z"
                  clip-rule="evenodd"
                ></path>
              </svg>
              <p class="text-sm text-yellow-700">
                No change detection mask available for the selected year
                comparison.
              </p>
            </div>
          </div>

          <!-- Metadata -->
          <div
            class="grid grid-cols-1 lg:grid-cols-2 gap-4 text-sm text-gray-600"
          >
            <div>
              <h5 class="font-medium text-gray-700 mb-1">
                Previous Image Details
              </h5>
              <p>
                Size:
                {{ formatFileSize(comparisonData.previousImage.size_bytes) }}
              </p>
              <p
                v-if="
                  comparisonData.previousImage.width &&
                  comparisonData.previousImage.height
                "
              >
                Dimensions: {{ comparisonData.previousImage.width }} ×
                {{ comparisonData.previousImage.height }}
              </p>
            </div>
            <div>
              <h5 class="font-medium text-gray-700 mb-1">
                Current Image Details
              </h5>
              <p>
                Size:
                {{ formatFileSize(comparisonData.currentImage.size_bytes) }}
              </p>
              <p
                v-if="
                  comparisonData.currentImage.width &&
                  comparisonData.currentImage.height
                "
              >
                Dimensions: {{ comparisonData.currentImage.width }} ×
                {{ comparisonData.currentImage.height }}
              </p>
            </div>
          </div>
        </div>
      </div>

      <!-- Modal Footer -->
      <div class="flex justify-end items-center p-4 border-t bg-gray-50">
        <button
          @click="$emit('close')"
          class="px-4 py-2 text-gray-600 hover:text-gray-800 transition-colors"
        >
          Close
        </button>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, watch } from "vue";
import type { ImageMetadata, ChangeMaskMetadata } from "@/types/api";
import { formatDate, formatFileSize } from "@/utils/helpers";

interface ComparisonData {
  previousImage: ImageMetadata;
  currentImage: ImageMetadata;
  changeMask: ChangeMaskMetadata | null;
  previousImageUrl: string;
  currentImageUrl: string;
  changeMaskUrl: string | null;
}

interface ImageComparisonModalProps {
  visible?: boolean;
  comparisonData?: ComparisonData | null;
  isLoading?: boolean;
  hasError?: boolean;
  errorMessage?: string;
}

interface ImageComparisonModalEmits {
  (e: "close"): void;
}

const props = withDefaults(defineProps<ImageComparisonModalProps>(), {
  visible: false,
  comparisonData: null,
  isLoading: false,
  hasError: false,
  errorMessage: "",
});

defineEmits<ImageComparisonModalEmits>();

// Local state for overlay controls
const showMaskOverlay = ref(true);
const maskOpacity = ref(0.7);

// Reset overlay state when modal opens/closes
watch(
  () => props.visible,
  (newVisible) => {
    if (newVisible) {
      showMaskOverlay.value = true;
      maskOpacity.value = 0.7;
    }
  }
);

function handleBackdropClick(event: Event) {
  if (event.target === event.currentTarget) {
    // Only close if clicking the backdrop, not the modal content
    // This is handled by the @click.stop on the modal content
  }
}

function handleImageError(event: Event) {
  const target = event.target as HTMLImageElement;
  if (target) {
    target.src = "/placeholder-image.jpg";
  }
}

function formatYearRange(): string {
  if (!props.comparisonData) return "";

  const prevYear = new Date(
    props.comparisonData.previousImage.time
  ).getFullYear();
  const currYear = new Date(
    props.comparisonData.currentImage.time
  ).getFullYear();

  return `${prevYear} vs ${currYear}`;
}
</script>

<style scoped>
/* Custom scrollbar for modal content */
.overflow-hidden:hover {
  overflow-y: auto;
}

/* Ensure modal appears above everything */
.z-\[9999\] {
  z-index: 9999;
}
</style>
