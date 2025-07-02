<template>
  <div>
    <!-- Loading Overlay -->
    <div
      v-if="isLoading"
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
      v-if="hasError"
      class="absolute top-4 left-4 bg-red-100 border border-red-400 text-red-700 px-4 py-2 rounded-lg shadow-lg z-[1000]"
    >
      <p class="text-sm">{{ error }}</p>
    </div>

    <!-- Image Count and Cache Stats -->
    <div
      class="absolute top-4 right-4 bg-white bg-opacity-90 px-4 py-2 rounded-lg shadow-lg z-[1000]"
    >
      <p class="text-sm font-medium">{{ imageCount }} images found</p>
      <button
        @click="$emit('toggle-cache-stats')"
        class="text-xs text-blue-600 hover:text-blue-800 mt-1"
      >
        {{ showCacheStats ? "Hide" : "Show" }} Cache Stats
      </button>
    </div>

    <!-- Cache Stats Display -->
    <div
      v-if="showCacheStats && cacheStats"
      class="absolute top-20 right-4 bg-white bg-opacity-95 px-3 py-2 rounded-lg shadow-lg z-[1000] text-xs"
    >
      <div class="space-y-1">
        <div class="font-medium text-gray-700">Cache Statistics</div>
        <div class="text-gray-600">Items: {{ cacheStats.itemCount }}</div>
        <div class="text-gray-600">
          Size: {{ cacheStats.totalSizeMB }}MB / {{ cacheStats.maxSizeMB }}MB
        </div>
        <div class="text-gray-600">Usage: {{ cacheStats.usagePercent }}%</div>
      </div>
    </div>

    <!-- Sidebar Toggle Button -->
    <button
      @click="$emit('toggle-sidebar')"
      class="absolute right-4 bg-blue-500 hover:bg-blue-600 text-white p-2 rounded-lg shadow-lg z-[1000] transition-colors"
      :class="sidebarTogglePosition"
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

    <!-- Sidebar Overlay (for mobile) -->
    <div
      v-if="showSidebar"
      @click="$emit('close-sidebar')"
      class="fixed inset-0 bg-black bg-opacity-50 z-[999] lg:hidden"
    ></div>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";

interface CacheStats {
  itemCount: number;
  totalSizeMB: string;
  maxSizeMB: string;
  usagePercent: string;
}

interface MapOverlaysProps {
  isLoading?: boolean;
  hasError?: boolean;
  error?: string | null;
  imageCount?: number;
  showCacheStats?: boolean;
  cacheStats?: CacheStats | null;
  showSidebar?: boolean;
  showTimeFilter?: boolean;
}

interface MapOverlaysEmits {
  (e: "toggle-cache-stats"): void;
  (e: "toggle-sidebar"): void;
  (e: "close-sidebar"): void;
}

const props = withDefaults(defineProps<MapOverlaysProps>(), {
  isLoading: false,
  hasError: false,
  error: "",
  imageCount: 0,
  showCacheStats: false,
  cacheStats: null,
  showSidebar: false,
  showTimeFilter: false,
});

defineEmits<MapOverlaysEmits>();

const sidebarTogglePosition = computed(() => ({
  "top-64": props.showTimeFilter,
  "top-48": props.showCacheStats && !props.showTimeFilter,
  "top-32": !props.showCacheStats && !props.showTimeFilter,
}));
</script>
