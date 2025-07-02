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
      // Fetch a few images to determine the available date range
      const response = await apiService.fetchImages({ limit: 1000 });
      const images = response.images;

      if (images.length > 0) {
        const dates = images
          .map((img) => new Date(img.time))
          .sort((a, b) => a.getTime() - b.getTime());
        const earliest = dates[0];
        const latest = dates[dates.length - 1];

        minDate.value = earliest.toISOString().split("T")[0];
        maxDate.value = latest.toISOString().split("T")[0];

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
