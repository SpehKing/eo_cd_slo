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
        <!-- Grid Stats Column -->
        <div class="space-y-3">
          <h3 class="text-sm font-medium text-gray-700">Grid Selection</h3>
          <div class="space-y-2 text-sm">
            <div class="flex justify-between">
              <span class="text-gray-600">Squares:</span>
              <span class="font-medium text-gray-600">{{ selectedCount }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-600">Area:</span>
              <span class="font-medium text-gray-600">{{
                formatArea(totalArea)
              }}</span>
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
            <label class="flex items-center space-x-2 cursor-pointer">
              <input
                type="checkbox"
                :checked="boundaryLayerVisible"
                @change="$emit('toggle-boundary-layer', $event)"
                class="rounded text-blue-600 focus:ring-blue-500"
              />
              <span class="text-sm text-gray-600">Boundaries</span>
            </label>
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
  imageLayerVisible?: boolean;
  boundaryLayerVisible?: boolean;
  isLoading?: boolean;
  minDate?: string;
  maxDate?: string;
  selectedStartDate?: string;
  selectedEndDate?: string;
}

interface FloatingDashboardEmits {
  (e: "toggle-image-layer", event: Event): void;
  (e: "toggle-boundary-layer", event: Event): void;
  (e: "execute-query"): void;
  (e: "clear-selection"): void;
  (e: "update:start-date", value: string): void;
  (e: "update:end-date", value: string): void;
}

const props = withDefaults(defineProps<FloatingDashboardProps>(), {
  selectedCount: 0,
  totalArea: 0,
  imageLayerVisible: true,
  boundaryLayerVisible: true,
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
  if (area < 1) {
    return `${(area * 1000000).toFixed(0)} m²`;
  } else if (area < 100) {
    return `${area.toFixed(2)} km²`;
  } else {
    return `${area.toFixed(0)} km²`;
  }
}

function updateStartDate(event: Event) {
  const target = event.target as HTMLInputElement;
  const value = parseInt(target.value);
  if (!props.minDate) return;

  const min = new Date(props.minDate).getTime();
  const newDate = new Date(min + value);
  emit("update:start-date", newDate.toISOString().split("T")[0]);
}

function updateEndDate(event: Event) {
  const target = event.target as HTMLInputElement;
  const value = parseInt(target.value);
  if (!props.minDate) return;

  const min = new Date(props.minDate).getTime();
  const newDate = new Date(min + value);
  emit("update:end-date", newDate.toISOString().split("T")[0]);
}
</script>

<style scoped>
/* Custom slider styling */
.slider-thumb::-webkit-slider-thumb {
  appearance: none;
  height: 20px;
  width: 20px;
  border-radius: 50%;
  background: #3b82f6;
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
}

.slider-thumb::-moz-range-thumb {
  height: 20px;
  width: 20px;
  border-radius: 50%;
  background: #3b82f6;
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
}

.slider-thumb:focus::-webkit-slider-thumb {
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.3);
}

.slider-thumb:focus::-moz-range-thumb {
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.3);
}

/* Mobile responsiveness */
@media (max-width: 768px) {
  .dashboard-container {
    min-width: 350px;
  }

  .controls-grid {
    grid-template-columns: 1fr;
    gap: 1rem;
  }
}
</style>
