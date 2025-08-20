<template>
  <div class="fixed bottom-6 left-1/2 transform -translate-x-1/2 z-[1000]">
    <!-- Hidden dashboard trigger area -->
    <div
      v-if="shouldShowTrigger"
      @mouseenter="showDashboard"
      class="trigger-area fixed bottom-[-1.5rem] left-1/2 transform -translate-x-1/2 w-[1000px] h-8 z-[999] flex items-end justify-center cursor-pointer"
    >
      <!-- Arrow indicator -->
      <div
        class="arrow-indicator !bg-white !rounded-t-lg !shadow-lg !px-3 !py-1 transition-transform duration-200 hover:scale-110"
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
            <!-- Year Dots Row -->
            <div
              v-if="availableYears.length > 0"
              class="flex justify-center items-center !mb-4 !space-x-3"
            >
              <span class="text-xs text-gray-600 mr-2 font-medium">Years:</span>
              <div class="flex !space-x-2 items-center">
                <button
                  v-for="yearData in availableYears"
                  :key="yearData.year"
                  @click="selectYear(yearData.year)"
                  @mousedown="handleInteractionStart"
                  @mouseup="handleInteractionEnd"
                  :class="[
                    'relative rounded-full border-2 transition-all duration-200 hover:scale-110 focus:outline-none focus:ring-2 focus:ring-blue-300 focus:ring-opacity-50',
                    getYearDotColor(yearData.year, yearData.count),
                    getYearDotSize(yearData.count),
                  ]"
                  :title="`${yearData.year}: ${yearData.count} images`"
                >
                  <span class="sr-only"
                    >{{ yearData.year }} ({{ yearData.count }} images)</span
                  >
                </button>
              </div>
            </div>

            <!-- Date range slider -->
            <div class="flex items-center !space-x-4">
              <span class="text-base text-gray-800 min-w-[80px]">
                {{ minYear }}
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
                    :max="yearRange"
                    :step="1"
                    :value="startValue"
                    @input="updateStartYear"
                    @mousedown="handleInteractionStart"
                    @mouseup="handleInteractionEnd"
                    @touchstart="handleInteractionStart"
                    @touchend="handleInteractionEnd"
                    class="absolute w-full h-2 bg-transparent appearance-none cursor-pointer slider-thumb"
                  />
                  <input
                    type="range"
                    :min="0"
                    :max="yearRange"
                    :step="1"
                    :value="endValue"
                    @input="updateEndYear"
                    @mousedown="handleInteractionStart"
                    @mouseup="handleInteractionEnd"
                    @touchstart="handleInteractionStart"
                    @touchend="handleInteractionEnd"
                    class="absolute w-full h-2 bg-transparent appearance-none cursor-pointer slider-thumb"
                  />
                </div>
              </div>
              <span class="text-base text-gray-800 min-w-[80px]">
                {{ maxYear }}
              </span>
            </div>
            <!-- Selected range display -->
            <div class="flex justify-center !mt-3 !space-x-4 !text-sm">
              <span class="text-blue-800 font-medium text-base">
                {{ selectedStartYear }}
              </span>
              <span class="text-gray-800 !px-2 text-base">to</span>
              <span class="text-blue-800 font-medium text-base">
                {{ selectedEndYear }}
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
import { computed, ref, watch, nextTick, onMounted } from "vue";
import { apiService } from "@/services/api";
import type { DateRangeResponse } from "@/types/api";

interface FloatingDashboardProps {
  drawingMode?: boolean;
  isLoading?: boolean;
  minDate?: string;
  maxDate?: string;
  selectedStartDate?: string;
  selectedEndDate?: string;
  hasSelection?: boolean;
  availableImages?: any[];
  visibleYears?: Set<number>;
}

interface FloatingDashboardEmits {
  (e: "toggle-drawing-mode"): void;
  (e: "execute-query"): void;
  (e: "clear-selection"): void;
  (e: "update:start-date", value: string): void;
  (e: "update:end-date", value: string): void;
  (e: "toggle-year-visibility", year: number): void;
}

const props = withDefaults(defineProps<FloatingDashboardProps>(), {
  drawingMode: false,
  isLoading: false,
  minDate: "",
  maxDate: "",
  selectedStartDate: "",
  selectedEndDate: "",
  hasSelection: false,
  availableImages: () => [],
  visibleYears: () => new Set(),
});

const emit = defineEmits<FloatingDashboardEmits>();

// Date range state
const dateRangeData = ref<DateRangeResponse | null>(null);

// Dashboard hiding state
const isHidden = ref(false);
const hasBeenHiddenOnce = ref(false);
const dashboardRef = ref<HTMLElement>();
const isInteracting = ref(false);

// Auto-hide logic
const shouldShowTrigger = computed(
  () => isHidden.value && hasBeenHiddenOnce.value
);

// Year dots logic
const availableYears = computed(() => {
  if (!props.availableImages || props.availableImages.length === 0) return [];

  const yearCounts = new Map<number, number>();

  props.availableImages.forEach((image) => {
    const year = new Date(image.time).getFullYear();
    yearCounts.set(year, (yearCounts.get(year) || 0) + 1);
  });

  return Array.from(yearCounts.entries())
    .map(([year, count]) => ({ year, count }))
    .sort((a, b) => a.year - b.year);
});

const currentTimeRangeYear = computed(() => {
  if (!props.selectedStartDate) return null;
  return new Date(props.selectedStartDate).getFullYear();
});

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

// Year-based range calculations
const availableYearsArray = computed(() => {
  return availableYears.value.map((y) => y.year).sort((a, b) => a - b);
});

const minYear = computed(() => {
  // Use API data as primary source, fallback to available years
  if (dateRangeData.value?.min_date) {
    return new Date(dateRangeData.value.min_date).getFullYear();
  }
  return availableYearsArray.value.length > 0
    ? availableYearsArray.value[0]
    : new Date().getFullYear();
});

const maxYear = computed(() => {
  // Use API data as primary source, fallback to available years
  if (dateRangeData.value?.max_date) {
    return new Date(dateRangeData.value.max_date).getFullYear();
  }
  return availableYearsArray.value.length > 0
    ? availableYearsArray.value[availableYearsArray.value.length - 1]
    : new Date().getFullYear();
});

const allAvailableYears = computed(() => {
  // Generate array of all years between min and max from API data
  if (dateRangeData.value?.min_date && dateRangeData.value?.max_date) {
    const startYear = new Date(dateRangeData.value.min_date).getFullYear();
    const endYear = new Date(dateRangeData.value.max_date).getFullYear();
    const years = [];
    for (let year = startYear; year <= endYear; year++) {
      years.push(year);
    }
    return years;
  }
  // Fallback to available years array
  return availableYearsArray.value;
});

const yearRange = computed(() => {
  return allAvailableYears.value.length - 1;
});

const startValue = computed(() => {
  if (!props.selectedStartDate || allAvailableYears.value.length === 0)
    return 0;
  const selectedYear = new Date(props.selectedStartDate).getFullYear();
  const index = allAvailableYears.value.indexOf(selectedYear);
  return index >= 0 ? index : 0;
});

const endValue = computed(() => {
  if (!props.selectedEndDate || allAvailableYears.value.length === 0)
    return yearRange.value;
  const selectedYear = new Date(props.selectedEndDate).getFullYear();
  const index = allAvailableYears.value.indexOf(selectedYear);
  return index >= 0 ? index : yearRange.value;
});

const selectedStartYear = computed(() => {
  if (!props.selectedStartDate || allAvailableYears.value.length === 0)
    return minYear.value;
  return new Date(props.selectedStartDate).getFullYear();
});

const selectedEndYear = computed(() => {
  if (!props.selectedEndDate || allAvailableYears.value.length === 0)
    return maxYear.value;
  return new Date(props.selectedEndDate).getFullYear();
});

const rangeStyle = computed(() => {
  if (yearRange.value === 0) {
    return { left: "0%", width: "100%" };
  }
  const leftPercent = (startValue.value / yearRange.value) * 100;
  const rightPercent = (endValue.value / yearRange.value) * 100;
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

function updateStartYear(event: Event) {
  const target = event.target as HTMLInputElement;
  const yearIndex = parseInt(target.value);
  if (yearIndex >= 0 && yearIndex < allAvailableYears.value.length) {
    const selectedYear = allAvailableYears.value[yearIndex];
    // Always set to January 1st
    const newDate = `${selectedYear}-01-01`;
    emit("update:start-date", newDate);
  }
}

function updateEndYear(event: Event) {
  const target = event.target as HTMLInputElement;
  const yearIndex = parseInt(target.value);
  if (yearIndex >= 0 && yearIndex < allAvailableYears.value.length) {
    const selectedYear = allAvailableYears.value[yearIndex];
    // Always set to December 31st
    const newDate = `${selectedYear}-12-31`;
    emit("update:end-date", newDate);
  }
}

// Year selection functions
function selectYear(year: number) {
  // If the year is already visible and it's the only one visible, hide it
  const isCurrentlyVisible = props.visibleYears?.has(year);
  const visibleCount = props.visibleYears?.size || 0;

  if (isCurrentlyVisible && visibleCount === 1) {
    // If this is the only visible year, hide it
    emit("toggle-year-visibility", year);
  } else {
    // Otherwise, hide all other years and show only this one
    // First hide all visible years
    if (props.visibleYears) {
      for (const visibleYear of props.visibleYears) {
        if (visibleYear !== year) {
          emit("toggle-year-visibility", visibleYear);
        }
      }
    }

    // Then ensure the selected year is visible
    if (!isCurrentlyVisible) {
      emit("toggle-year-visibility", year);
    }
  }
}

function getYearDotColor(year: number, count: number): string {
  const isVisible = props.visibleYears?.has(year);
  const isCurrentRange = currentTimeRangeYear.value === year;

  if (isVisible) {
    if (count > 20) {
      return "bg-emerald-600 border-emerald-700 shadow-lg";
    } else if (count > 5) {
      return "bg-green-500 border-green-600 shadow-lg";
    } else if (count > 0) {
      return "bg-yellow-500 border-yellow-600 shadow-lg";
    }
  } else {
    if (count > 20) {
      return "bg-emerald-200 border-emerald-300 hover:bg-emerald-300";
    } else if (count > 5) {
      return "bg-green-200 border-green-300 hover:bg-green-300";
    } else if (count > 0) {
      return "bg-yellow-200 border-yellow-300 hover:bg-yellow-300";
    } else {
      return "bg-gray-200 border-gray-300 hover:bg-gray-300";
    }
  }

  return "bg-gray-300 border-gray-400";
}

// Get year dot size based on image count
function getYearDotSize(count: number): string {
  if (count > 20) return "w-5 h-5";
  if (count > 10) return "w-4.5 h-4.5";
  if (count > 5) return "w-4 h-4";
  return "w-3.5 h-3.5";
}

// Fetch date range from API
async function fetchDateRange() {
  try {
    const response = await apiService.fetchDateRange();
    dateRangeData.value = response;
  } catch (error) {
    console.warn("Failed to fetch date range from API:", error);
    // Component will fallback to using availableImages data
  }
}

// Initialize component
onMounted(() => {
  fetchDateRange();
});
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

/* Year dots styles */
.year-dot {
  transition: all 0.2s ease;
}

.year-dot:hover {
  transform: scale(1.2);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
}

.year-dot:active {
  transform: scale(1.1);
}
</style>
