<template>
  <div class="fixed bottom-6 left-1/2 transform -translate-x-1/2 z-[1000]">
    <div
      class="dashboard-container bg-white rounded-3xl shadow-2xl p-6 min-w-[1000px] max-w-[90vw]"
    >
      <!-- Date Range Slider Row -->
      <div class="!p-6">
        <label class="block text-sm font-medium text-gray-700 !mb-3">
          Select Time Range
        </label>
        <div class="relative">
          <!-- Date range slider -->
          <div class="flex items-center space-x-4">
            <span class="text-xs text-gray-500 min-w-[80px]">
              {{ formatDate(minDate) }}
            </span>
            <div class="flex-1 relative">
              <!-- Custom dual range slider -->
              <div class="relative h-2 bg-gray-200 rounded-full">
                <div
                  class="absolute h-2 bg-blue-500 rounded-full"
                  :style="rangeStyle"
                ></div>
                <input
                  type="range"
                  :min="0"
                  :max="dateRange"
                  :value="startValue"
                  @input="updateStartDate"
                  class="absolute w-full h-2 bg-transparent appearance-none cursor-pointer slider-thumb"
                />
                <input
                  type="range"
                  :min="0"
                  :max="dateRange"
                  :value="endValue"
                  @input="updateEndDate"
                  class="absolute w-full h-2 bg-transparent appearance-none cursor-pointer slider-thumb"
                />
              </div>
            </div>
            <span class="text-xs text-gray-500 min-w-[80px] !ml-2">
              {{ formatDate(maxDate) }}
            </span>
          </div>
          <!-- Selected range display -->
          <div class="flex justify-center !mt-2 space-x-4 text-sm">
            <span class="text-blue-600 font-medium">
              {{ formatDate(selectedStartDate) }}
            </span>
            <span class="text-gray-400 !ml-4 !mr-3">to</span>
            <span class="text-blue-600 font-medium">
              {{ formatDate(selectedEndDate) }}
            </span>
          </div>
        </div>
      </div>

      <!-- Controls Row -->
      <div class="controls-grid grid grid-cols-3 gap-6 !px-6 !pb-6">
        <!-- Selection Stats Column -->
        <div class="space-y-3">
          <h3 class="text-sm font-medium text-gray-700">Area Selection</h3>
          <div class="space-y-2 text-sm">
            <div class="flex justify-between">
              <span class="text-gray-600">Status:</span>
              <span class="font-medium text-gray-600">{{
                selectedCount > 0 ? "Selected" : "None"
              }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-600">Area:</span>
              <span class="font-medium text-gray-600">{{
                formatArea(totalArea)
              }}</span>
            </div>
            <div v-if="selectedCount === 0" class="text-xs text-gray-500 mt-2">
              <span v-if="drawingMode"
                >Click and drag on the map to draw a bounding box</span
              >
              <span v-else>Enable drawing mode to select an area</span>
            </div>
          </div>
        </div>

        <!-- Layer Controls Column -->
        <div class="space-y-3">
          <h3 class="text-sm font-medium text-gray-700">Map Layers</h3>
          <div class="space-y-2">
            <label class="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                :checked="imageLayerVisible"
                @change="$emit('toggle-image-layer', $event)"
                class="rounded text-blue-600 focus:ring-blue-500"
              />
              <span class="text-sm text-gray-600">Satellite Images</span>
            </label>
          </div>

          <h3 class="text-sm font-medium text-gray-700 pt-2">Selection Mode</h3>
          <div>
            <button
              @click="$emit('toggle-drawing-mode')"
              :class="[
                'w-full px-3 py-2 text-sm font-medium rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2',
                drawingMode
                  ? 'bg-green-600 text-white hover:bg-green-700 focus:ring-green-500'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200 focus:ring-gray-500',
              ]"
            >
              <div class="flex items-center justify-center space-x-2">
                <svg
                  class="w-4 h-4"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4"
                  />
                </svg>
                <span>{{
                  drawingMode ? "Drawing Mode" : "Navigate Mode"
                }}</span>
              </div>
            </button>
          </div>
        </div>

        <!-- Action Buttons Column -->
        <div class="space-y-3">
          <h3 class="text-sm font-medium text-gray-700">Actions</h3>
          <div class="space-y-2">
            <button
              @click="$emit('execute-query')"
              :disabled="!canExecuteQuery"
              class="w-full px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
            >
              <div class="flex items-center justify-center space-x-2">
                <svg
                  v-if="isLoading"
                  class="animate-spin h-4 w-4"
                  viewBox="0 0 24 24"
                >
                  <circle
                    cx="12"
                    cy="12"
                    r="10"
                    stroke="currentColor"
                    stroke-width="4"
                    fill="none"
                    opacity="0.25"
                  />
                  <path
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    opacity="0.75"
                  />
                </svg>
                <span>{{ isLoading ? "Loading..." : "Execute Query" }}</span>
              </div>
            </button>
            <button
              @click="$emit('clear-selection')"
              :disabled="selectedCount === 0"
              class="w-full px-4 py-2 bg-gray-100 text-gray-700 text-sm font-medium rounded-lg hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 disabled:bg-gray-50 disabled:text-gray-400 disabled:cursor-not-allowed transition-colors"
            >
              Clear Selection
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";

interface FloatingDashboardProps {
  selectedCount?: number;
  totalArea?: number;
  drawingMode?: boolean;
  imageLayerVisible?: boolean;
  isLoading?: boolean;
  minDate?: string;
  maxDate?: string;
  selectedStartDate?: string;
  selectedEndDate?: string;
}

interface FloatingDashboardEmits {
  (e: "toggle-image-layer", event: Event): void;
  (e: "toggle-drawing-mode"): void;
  (e: "execute-query"): void;
  (e: "clear-selection"): void;
  (e: "update:start-date", value: string): void;
  (e: "update:end-date", value: string): void;
}

const props = withDefaults(defineProps<FloatingDashboardProps>(), {
  selectedCount: 0,
  totalArea: 0,
  drawingMode: false,
  imageLayerVisible: true,
  isLoading: false,
  minDate: "",
  maxDate: "",
  selectedStartDate: "",
  selectedEndDate: "",
});

const emit = defineEmits<FloatingDashboardEmits>();

// Date range calculations
const dateRange = computed(() => {
  if (!props.minDate || !props.maxDate) return 100;
  const min = new Date(props.minDate).getTime();
  const max = new Date(props.maxDate).getTime();
  return max - min;
});

const startValue = computed(() => {
  if (!props.minDate || !props.selectedStartDate) return 0;
  const min = new Date(props.minDate).getTime();
  const start = new Date(props.selectedStartDate).getTime();
  return start - min;
});

const endValue = computed(() => {
  if (!props.minDate || !props.selectedEndDate) return dateRange.value;
  const min = new Date(props.minDate).getTime();
  const end = new Date(props.selectedEndDate).getTime();
  return end - min;
});

const rangeStyle = computed(() => {
  const leftPercent = (startValue.value / dateRange.value) * 100;
  const rightPercent = (endValue.value / dateRange.value) * 100;
  return {
    left: `${leftPercent}%`,
    width: `${rightPercent - leftPercent}%`,
  };
});

const canExecuteQuery = computed(() => {
  return (
    props.selectedCount > 0 &&
    props.selectedStartDate &&
    props.selectedEndDate &&
    !props.isLoading
  );
});

// Helper functions
function formatDate(dateString: string): string {
  if (!dateString) return "-";
  return new Date(dateString).toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatArea(area: number): string {
  if (area === 0) return "0 km²";
  if (area < 1) {
    return `${(area * 1000000).toFixed(0)} m²`;
  }
  return `${area.toFixed(2)} km²`;
}

function updateStartDate(event: Event) {
  const target = event.target as HTMLInputElement;
  const value = parseInt(target.value);
  const minTime = new Date(props.minDate).getTime();
  const newDate = new Date(minTime + value);
  emit("update:start-date", newDate.toISOString().split("T")[0]);
}

function updateEndDate(event: Event) {
  const target = event.target as HTMLInputElement;
  const value = parseInt(target.value);
  const minTime = new Date(props.minDate).getTime();
  const newDate = new Date(minTime + value);
  emit("update:end-date", newDate.toISOString().split("T")[0]);
}
</script>

<style scoped>
.dashboard-container {
  backdrop-filter: blur(10px);
  background: rgba(255, 255, 255, 0.95);
}

.controls-grid {
  border-top: 1px solid #e5e7eb;
}

/* Custom slider styles */
.slider-thumb {
  pointer-events: none;
}

.slider-thumb::-webkit-slider-thumb {
  appearance: none;
  height: 20px;
  width: 20px;
  border-radius: 50%;
  background: #3b82f6;
  border: 2px solid white;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  cursor: pointer;
  pointer-events: all;
}

.slider-thumb::-moz-range-thumb {
  height: 20px;
  width: 20px;
  border-radius: 50%;
  background: #3b82f6;
  border: 2px solid white;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
  cursor: pointer;
  pointer-events: all;
}
</style>
