<template>
  <div
    class="absolute bg-white bg-opacity-90 px-3 py-2 rounded-lg shadow-lg z-[1000] space-y-1"
    :class="dynamicPosition"
  >
    <div class="flex items-center space-x-2">
      <input
        id="imageLayer"
        type="checkbox"
        :checked="imageLayerVisible"
        @change="$emit('toggle-image-layer', $event)"
        class="rounded"
      />
      <label for="imageLayer" class="text-xs font-medium"
        >Satellite Images</label
      >
    </div>
    <div class="flex items-center space-x-2">
      <input
        id="boundaryLayer"
        type="checkbox"
        :checked="boundaryLayerVisible"
        @change="$emit('toggle-boundary-layer', $event)"
        class="rounded"
      />
      <label for="boundaryLayer" class="text-xs font-medium">Boundaries</label>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";

interface LayerControlsProps {
  imageLayerVisible?: boolean;
  boundaryLayerVisible?: boolean;
  showCacheStats?: boolean;
}

interface LayerControlsEmits {
  (e: "toggle-image-layer", event: Event): void;
  (e: "toggle-boundary-layer", event: Event): void;
}

const props = withDefaults(defineProps<LayerControlsProps>(), {
  imageLayerVisible: true,
  boundaryLayerVisible: true,
  showCacheStats: false,
});

defineEmits<LayerControlsEmits>();

const dynamicPosition = computed(() => ({
  "top-32": !props.showCacheStats,
  "top-16": props.showCacheStats,
  right: "1rem",
}));
</script>
