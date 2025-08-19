import { ref, computed } from "vue";
import { apiService } from "@/services/api";

export function useTimeFilter() {
  const minDate = ref<string>("");
  const maxDate = ref<string>("");
  const selectedStartDate = ref<string>("");
  const selectedEndDate = ref<string>("");
  const showTimeFilter = ref(false);

  // Computed properties
  const timeRange = computed(() => {
    if (!selectedStartDate.value && !selectedEndDate.value) return null;
    return {
      start: selectedStartDate.value || undefined,
      end: selectedEndDate.value || undefined,
    };
  });

  const hasTimeFilter = computed(() => Boolean(timeRange.value));

  // Initialize date ranges
  async function initializeDateRanges() {
    try {
      // Fetch date range efficiently from dedicated endpoint
      const response = await apiService.fetchDateRange();

      if (response.min_date && response.max_date) {
        minDate.value = new Date(response.min_date).toISOString().split("T")[0];
        maxDate.value = new Date(response.max_date).toISOString().split("T")[0];

        // Set initial selection to full range
        selectedStartDate.value = minDate.value;
        selectedEndDate.value = maxDate.value;
      }
    } catch (error) {
      console.warn("Failed to initialize date ranges:", error);
    }
  }

  // Clear time filter
  function clearTimeFilter() {
    selectedStartDate.value = minDate.value;
    selectedEndDate.value = maxDate.value;
  }

  // Toggle time filter visibility
  function toggleTimeFilter() {
    showTimeFilter.value = !showTimeFilter.value;
  }

  return {
    // State
    minDate,
    maxDate,
    selectedStartDate,
    selectedEndDate,
    showTimeFilter,
    
    // Computed
    timeRange,
    hasTimeFilter,
    
    // Methods
    initializeDateRanges,
    clearTimeFilter,
    toggleTimeFilter,
  };
}
