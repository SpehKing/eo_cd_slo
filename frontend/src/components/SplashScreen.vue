<template>
  <div
    v-if="visible"
    class="fixed inset-0 bg-gradient-to-br from-blue-900 via-blue-800 to-blue-900 z-[9999] flex items-center justify-center"
  >
    <div class="text-center text-white max-w-lg px-8">
      <!-- Logo/Icon -->
      <div class="mb-8">
        <div
          class="inline-flex items-center justify-center w-24 h-24 bg-white bg-opacity-20 rounded-full mb-4"
        >
          <svg
            class="w-12 h-12 text-white"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
        </div>
        <h1 class="text-4xl font-bold mb-2">EO Change Detection</h1>
        <p class="text-blue-200 text-lg">Earth Observation for Slovenia</p>
      </div>

      <!-- Loading Section -->
      <div class="mb-8">
        <div class="mb-4">
          <div class="w-full bg-blue-800 bg-opacity-50 rounded-full h-3 mb-2">
            <div
              class="bg-gradient-to-r from-blue-400 to-blue-300 h-3 rounded-full transition-all duration-300 ease-out"
              :style="{ width: `${progress}%` }"
            ></div>
          </div>
          <p class="text-sm text-blue-200">
            {{ Math.round(progress) }}% Complete
          </p>
        </div>

        <div class="space-y-2">
          <div class="flex items-center justify-between text-sm">
            <span class="flex items-center">
              <div
                class="w-2 h-2 rounded-full mr-2"
                :class="
                  currentStep >= 1
                    ? 'bg-green-400'
                    : currentStep === 1
                    ? 'bg-blue-400 animate-pulse'
                    : 'bg-blue-600'
                "
              ></div>
              Initializing date ranges
            </span>
            <span v-if="currentStep >= 1" class="text-green-400">✓</span>
          </div>

          <div class="flex items-center justify-between text-sm">
            <span class="flex items-center">
              <div
                class="w-2 h-2 rounded-full mr-2"
                :class="
                  currentStep >= 2
                    ? 'bg-green-400'
                    : currentStep === 2
                    ? 'bg-blue-400 animate-pulse'
                    : 'bg-blue-600'
                "
              ></div>
              Loading change detection masks
            </span>
            <span v-if="currentStep >= 2" class="text-green-400">✓</span>
          </div>

          <div class="flex items-center justify-between text-sm">
            <span class="flex items-center">
              <div
                class="w-2 h-2 rounded-full mr-2"
                :class="
                  currentStep >= 3
                    ? 'bg-green-400'
                    : currentStep === 3
                    ? 'bg-blue-400 animate-pulse'
                    : 'bg-blue-600'
                "
              ></div>
              Creating composite visualization
            </span>
            <span v-if="currentStep >= 3" class="text-green-400">✓</span>
          </div>

          <div class="flex items-center justify-between text-sm">
            <span class="flex items-center">
              <div
                class="w-2 h-2 rounded-full mr-2"
                :class="
                  currentStep >= 4
                    ? 'bg-green-400'
                    : currentStep === 4
                    ? 'bg-blue-400 animate-pulse'
                    : 'bg-blue-600'
                "
              ></div>
              Preparing map interface
            </span>
            <span v-if="currentStep >= 4" class="text-green-400">✓</span>
          </div>
        </div>

        <!-- Details Section -->
        <div
          v-if="showDetails"
          class="mt-6 p-4 bg-blue-800 bg-opacity-30 rounded-lg text-left"
        >
          <h3 class="font-semibold mb-2">Loading Statistics</h3>
          <div class="text-sm space-y-1 text-blue-200">
            <div v-if="totalMasks > 0">
              Total change masks: {{ totalMasks.toLocaleString() }}
            </div>
            <div v-if="loadedMasks > 0">
              Loaded: {{ loadedMasks.toLocaleString() }}
            </div>
            <div v-if="dateRange">
              Time range: {{ formatDate(dateRange.min_date) }} -
              {{ formatDate(dateRange.max_date) }}
            </div>
            <div v-if="currentStep === 2 && maskProgress > 0">
              Progress: {{ Math.round(maskProgress) }}% ({{
                Math.round(maskLoadSpeed)
              }}
              masks/sec)
            </div>
          </div>
        </div>

        <!-- Toggle Details Button -->
        <button
          @click="showDetails = !showDetails"
          class="mt-4 text-xs text-blue-300 hover:text-white underline"
        >
          {{ showDetails ? "Hide" : "Show" }} Details
        </button>
      </div>

      <!-- Error handling -->
      <div
        v-if="hasError"
        class="mb-4 p-4 bg-red-900 bg-opacity-50 rounded-lg border border-red-500"
      >
        <h3 class="font-semibold text-red-300 mb-2">Loading Error</h3>
        <p class="text-sm text-red-200">{{ errorMessage }}</p>
        <div class="flex space-x-3 mt-3">
          <button
            @click="$emit('retry')"
            class="px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm font-medium transition-colors"
          >
            Retry Loading
          </button>
          <button
            @click="$emit('continue-without-masks')"
            class="px-4 py-2 bg-gray-600 hover:bg-gray-700 rounded-lg text-sm font-medium transition-colors"
          >
            Continue Without Masks
          </button>
        </div>
      </div>

      <!-- Footer -->
      <div class="text-xs text-blue-300">
        <p>Powered by Earth Observation technologies</p>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref } from "vue";
import type { DateRangeResponse } from "@/types/api";

interface SplashScreenProps {
  visible?: boolean;
  currentStep?: number;
  progress?: number;
  totalMasks?: number;
  loadedMasks?: number;
  maskProgress?: number;
  maskLoadSpeed?: number;
  dateRange?: DateRangeResponse | null;
  hasError?: boolean;
  errorMessage?: string;
}

interface SplashScreenEmits {
  (e: "retry"): void;
  (e: "continue-without-masks"): void;
}

const props = withDefaults(defineProps<SplashScreenProps>(), {
  visible: false,
  currentStep: 0,
  progress: 0,
  totalMasks: 0,
  loadedMasks: 0,
  maskProgress: 0,
  maskLoadSpeed: 0,
  dateRange: null,
  hasError: false,
  errorMessage: "",
});

defineEmits<SplashScreenEmits>();

const showDetails = ref(false);

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "N/A";
  return new Date(dateStr).toLocaleDateString();
}
</script>

<style scoped>
/* Custom animations */
@keyframes pulse {
  0%,
  100% {
    opacity: 1;
  }
  50% {
    opacity: 0.5;
  }
}

.animate-pulse {
  animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
}
</style>
