<template>
  <div
    class="fixed top-0 right-0 h-full w-96 bg-white shadow-xl z-[1000] transform transition-transform duration-300 ease-in-out"
    :class="sidebarClass"
  >
    <div class="flex flex-col h-full">
      <!-- Header -->
      <div class="flex items-center justify-between p-4 border-b">
        <h2 class="text-lg font-semibold">Image Details</h2>
        <button @click="$emit('close')" class="p-1 hover:bg-gray-100 rounded">
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
      <div v-if="image" class="flex-1 overflow-y-auto p-4">
        <!-- Image Preview -->
        <div class="mb-4">
          <img
            v-if="imagePreviewUrl"
            :src="imagePreviewUrl"
            :alt="`Satellite image from ${formatDate(image.time)}`"
            class="w-full h-48 object-cover rounded-lg border"
            @error="handleImageError"
          />
        </div>

        <!-- Image Information -->
        <div class="space-y-3">
          <div>
            <label class="text-sm font-medium text-gray-600">Date</label>
            <p class="text-sm">{{ formatDate(image.time) }}</p>
          </div>

          <div>
            <label class="text-sm font-medium text-gray-600">Image ID</label>
            <p class="text-sm">{{ image.id }}</p>
          </div>

          <div>
            <label class="text-sm font-medium text-gray-600">File Size</label>
            <p class="text-sm">{{ formatFileSize(image.size_bytes) }}</p>
          </div>

          <div v-if="image.width && image.height">
            <label class="text-sm font-medium text-gray-600">Dimensions</label>
            <p class="text-sm">{{ image.width }} × {{ image.height }} pixels</p>
          </div>

          <div v-if="image.data_type">
            <label class="text-sm font-medium text-gray-600">Data Type</label>
            <p class="text-sm">{{ image.data_type }}</p>
          </div>

          <div>
            <label class="text-sm font-medium text-gray-600"
              >Bounding Box</label
            >
            <p class="text-xs text-gray-500 font-mono break-all">
              {{ image.bbox_wkt }}
            </p>
          </div>
        </div>

        <!-- Actions -->
        <div class="mt-6 space-y-2">
          <button
            @click="$emit('download', image)"
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
</template>

<script lang="ts" setup>
import { computed } from "vue";
import type { ImageMetadata } from "@/types/api";
import { formatDate, formatFileSize } from "@/utils/helpers";

interface ImageSidebarProps {
  visible?: boolean;
  image?: ImageMetadata | null;
  imagePreviewUrl?: string | null;
}

interface ImageSidebarEmits {
  (e: "close"): void;
  (e: "download", image: ImageMetadata): void;
}

const props = withDefaults(defineProps<ImageSidebarProps>(), {
  visible: false,
  image: null,
  imagePreviewUrl: null,
});

defineEmits<ImageSidebarEmits>();

const sidebarClass = computed(() => ({
  "translate-x-0": props.visible,
  "translate-x-full": !props.visible,
}));

function handleImageError(event: Event) {
  const target = event.target as HTMLImageElement;
  if (target) {
    target.src = "/placeholder-image.jpg";
  }
}
</script>
