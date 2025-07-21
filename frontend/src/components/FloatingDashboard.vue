<template>
  <div class="fixed bottom-6 left-1/2 transform -translate-x-1/2 z-[1000]">
    <!-- Hidden dashboard trigger area -->
    <div
      v-if="shouldShowTrigger"
      @mouseenter="showDashboard"
      class="trigger-area fixed bottom-0 left-1/2 transform -translate-x-1/2 w-[1000px] h-8 z-[999] flex items-end justify-center cursor-pointer"
    >
      <!-- Arrow indicator -->
      <div
        class="arrow-indicator bg-white rounded-t-lg shadow-lg px-3 py-1 transition-transform duration-200 hover:scale-110"
      >
        <svg
          class="w-4 h-4 text-gray-600"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M5 15l7-7 7 7"
          />
        </svg>
      </div>
    </div>

    <!-- Main dashboard -->
    <div
      ref="dashboardRef"
      @mouseleave="handleMouseLeave"
      :class="[
        'dashboard-container bg-white rounded-3xl shadow-2xl min-w-[1000px] max-w-[90vw] transition-all duration-500 ease-out',
        {
          'translate-y-[calc(100%+2rem)]': isHidden,
          'translate-y-0': !isHidden,
        },
      ]"
    >
      <!-- Single Row with Date Range Slider and Controls -->
      <div class="flex items-center !p-6 !space-x-6">
        <!-- Date Range Slider Section (5/6 of the width) -->
        <div class="flex-1 w-5/6">
          <div class="relative">
            <!-- Date range slider -->
            <div class="flex items-center !space-x-4">
              <span class="text-base text-gray-800 min-w-[80px]">
                {{ formatDate(minDate) }}
              </span>
              <div class="flex-1 relative">
                <!-- Custom dual range slider -->
                <div class="relative !h-2 bg-gray-200 rounded-full">
                  <div
                    class="absolute !h-2 bg-blue-500 rounded-full"
                    :style="rangeStyle"
                  ></div>
                  <input
                    type="range"
                    :min="0"
                    :max="dateRange"
                    :value="startValue"
                    @input="updateStartDate"
                    @mousedown="handleInteractionStart"
                    @mouseup="handleInteractionEnd"
                    @touchstart="handleInteractionStart"
                    @touchend="handleInteractionEnd"
                    class="absolute w-full h-2 bg-transparent appearance-none cursor-pointer slider-thumb"
                  />
                  <input
                    type="range"
                    :min="0"
                    :max="dateRange"
                    :value="endValue"
                    @input="updateEndDate"
                    @mousedown="handleInteractionStart"
                    @mouseup="handleInteractionEnd"
                    @touchstart="handleInteractionStart"
                    @touchend="handleInteractionEnd"
                    class="absolute w-full h-2 bg-transparent appearance-none cursor-pointer slider-thumb"
                  />
                </div>
              </div>
              <span class="text-base text-gray-800 min-w-[80px]">
                {{ formatDate(maxDate) }}
              </span>
            </div>
            <!-- Selected range display -->
            <div class="flex justify-center !mt-3 !space-x-4 !text-sm">
              <span class="text-blue-800 font-medium text-base">
                {{ formatDate(selectedStartDate) }}
              </span>
              <span class="text-gray-800 !px-2 text-base">to</span>
              <span class="text-blue-800 font-medium text-base">
                {{ formatDate(selectedEndDate) }}
              </span>
            </div>
          </div>
        </div>

        <!-- Controls Section (1/6 of the width) -->
        <div class="w-1/6 min-w-[200px]">
          <!-- All buttons in one row -->
          <div class="h-14 flex !space-x-1">
            <!-- Selection Mode Button (takes 1/2 width) -->
            <button
              @click="$emit('toggle-drawing-mode')"
              @mousedown="handleInteractionStart"
              @mouseup="handleInteractionEnd"
              :class="[
                'w-1/2 h-full px-2 text-xs font-medium rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2',
                drawingMode
                  ? 'bg-green-600 text-white hover:bg-green-700 focus:ring-green-500'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200 focus:ring-gray-500',
              ]"
            >
              <div class="flex items-center justify-center space-x-1">
                <span class="text-base">{{ drawingMode ? "✏️" : "👆" }}</span>
                <span class="text-sm">{{
                  drawingMode ? "Drawing" : "Navigate"
                }}</span>
              </div>
            </button>

            <!-- Action Buttons Container (takes 1/2 width) -->
            <div class="w-1/2 flex flex-col !space-y-1">
              <button
                @click="$emit('execute-query')"
                @mousedown="handleInteractionStart"
                @mouseup="handleInteractionEnd"
                :disabled="!canExecuteQuery"
                class="w-full h-9 px-2 py-1 bg-blue-600 text-white text-sm font-medium rounded hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
              >
                <div class="flex items-center justify-center space-x-1">
                  <svg
                    v-if="isLoading"
                    class="animate-spin h-3 w-3"
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
                  <span class="text-sm">{{
                    isLoading ? "Loading..." : "Execute"
                  }}</span>
                </div>
              </button>
              <button
                @click="$emit('clear-selection')"
                @mousedown="handleInteractionStart"
                @mouseup="handleInteractionEnd"
                class="w-full h-9 px-2 bg-gray-100 text-gray-700 text-sm font-medium rounded hover:bg-gray-200 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 transition-colors"
              >
                Clear
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref, watch, nextTick } from "vue";

interface FloatingDashboardProps {
  drawingMode?: boolean;
  isLoading?: boolean;
  minDate?: string;
  maxDate?: string;
  selectedStartDate?: string;
  selectedEndDate?: string;
  hasSelection?: boolean;
}

interface FloatingDashboardEmits {
  (e: "toggle-drawing-mode"): void;
  (e: "execute-query"): void;
  (e: "clear-selection"): void;
  (e: "update:start-date", value: string): void;
  (e: "update:end-date", value: string): void;
}

const props = withDefaults(defineProps<FloatingDashboardProps>(), {
  drawingMode: false,
  isLoading: false,
  minDate: "",
  maxDate: "",
  selectedStartDate: "",
  selectedEndDate: "",
  hasSelection: false,
});

const emit = defineEmits<FloatingDashboardEmits>();

// Dashboard hiding state
const isHidden = ref(false);
const hasBeenHiddenOnce = ref(false);
const dashboardRef = ref<HTMLElement>();
const isInteracting = ref(false);

// Auto-hide logic
const shouldShowTrigger = computed(
  () => isHidden.value && hasBeenHiddenOnce.value
);

// Watch for first selection to trigger auto-hide
watch(
  () => props.hasSelection,
  (newHasSelection, oldHasSelection) => {
    if (newHasSelection && !oldHasSelection && !hasBeenHiddenOnce.value) {
      // First time selection is made - hide dashboard after a short delay
      setTimeout(() => {
        hideDashboard();
      }, 1500); // Give user time to see the dashboard before hiding
    }
  }
);

function hideDashboard() {
  isHidden.value = true;
  hasBeenHiddenOnce.value = true;
}

function showDashboard() {
  isHidden.value = false;
}

function handleMouseLeave() {
  if (hasBeenHiddenOnce.value && !isInteracting.value && !props.drawingMode) {
    // Small delay before hiding to prevent flickering
    setTimeout(() => {
      if (
        dashboardRef.value &&
        !dashboardRef.value.matches(":hover") &&
        !isInteracting.value &&
        !props.drawingMode
      ) {
        isHidden.value = true;
      }
    }, 300);
  }
}

// Prevent hiding during interactions
function handleInteractionStart() {
  isInteracting.value = true;
}

function handleInteractionEnd() {
  setTimeout(() => {
    isInteracting.value = false;
  }, 100);
}

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
    props.selectedStartDate &&
    props.selectedEndDate &&
    props.hasSelection &&
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
  backdrop-filter: blur(4px);
  background: rgba(255, 255, 255, 0.5);
}

/* Trigger area styles */
.trigger-area {
  /* Invisible background for hover detection */
  background: transparent;
}

.arrow-indicator {
  backdrop-filter: blur(8px);
  background: rgba(255, 255, 255, 0.95);
  border: 1px solid rgba(0, 0, 0, 0.1);
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

/* Animation improvements */
.dashboard-container {
  transform-origin: center bottom;
}

/* Enhance the arrow indicator hover effect */
.arrow-indicator:hover {
  background: rgba(255, 255, 255, 1);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}
</style>
