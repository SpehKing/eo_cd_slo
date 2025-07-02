<template>
  <div
    class="absolute left-4 bg-white bg-opacity-90 px-3 py-2 rounded-lg shadow-lg z-[1000]"
    :class="dynamicPosition"
  >
    <div class="flex items-center space-x-2 mb-2">
      <button
        @click="$emit('toggle-visibility')"
        class="text-xs font-medium text-blue-600 hover:text-blue-800 flex items-center space-x-1"
      >
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
            d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
          ></path>
        </svg>
        <span>{{ visible ? "Hide" : "Show" }} Time Filter</span>
      </button>
      <span
        v-if="hasActiveFilter"
        class="px-2 py-1 bg-blue-100 text-blue-700 text-xs rounded-full"
      >
        Active
      </span>
    </div>

    <div v-if="visible" class="space-y-3 min-w-[280px]">
      <div class="grid grid-cols-2 gap-2">
        <div>
          <label class="block text-xs font-medium text-gray-700 mb-1">
            Start Date
          </label>
          <input
            :value="startDate"
            type="date"
            :min="minDate"
            :max="maxDate"
            @input="
              $emit(
                'update:start-date',
                ($event.target as HTMLInputElement).value
              )
            "
            @change="$emit('apply-filter')"
            class="w-full text-xs border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label class="block text-xs font-medium text-gray-700 mb-1">
            End Date
          </label>
          <input
            :value="endDate"
            type="date"
            :min="minDate"
            :max="maxDate"
            @input="
              $emit(
                'update:end-date',
                ($event.target as HTMLInputElement).value
              )
            "
            @change="$emit('apply-filter')"
            class="w-full text-xs border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      <div class="text-xs text-gray-600">
        <div>Available: {{ minDate }} to {{ maxDate }}</div>
        <div v-if="hasActiveFilter" class="mt-1">
          Showing: {{ startDate || "earliest" }} to {{ endDate || "latest" }}
        </div>
      </div>

      <div class="flex space-x-2">
        <button
          @click="$emit('clear-filter')"
          class="flex-1 px-2 py-1 bg-gray-100 hover:bg-gray-200 text-gray-700 text-xs rounded transition-colors"
        >
          Clear Filter
        </button>
        <button
          @click="$emit('apply-filter')"
          class="flex-1 px-2 py-1 bg-blue-500 hover:bg-blue-600 text-white text-xs rounded transition-colors"
        >
          Apply
        </button>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";

interface TimeFilterProps {
  visible?: boolean;
  startDate?: string;
  endDate?: string;
  minDate?: string;
  maxDate?: string;
  showCacheStats?: boolean;
}

interface TimeFilterEmits {
  (e: "toggle-visibility"): void;
  (e: "update:start-date", value: string): void;
  (e: "update:end-date", value: string): void;
  (e: "apply-filter"): void;
  (e: "clear-filter"): void;
}

const props = withDefaults(defineProps<TimeFilterProps>(), {
  visible: false,
  startDate: "",
  endDate: "",
  minDate: "",
  maxDate: "",
  showCacheStats: false,
});

defineEmits<TimeFilterEmits>();

const hasActiveFilter = computed(() =>
  Boolean(props.startDate || props.endDate)
);

const dynamicPosition = computed(() => ({
  "top-32": !props.showCacheStats,
  "top-16": props.showCacheStats,
}));
</script>
