<template>
  <div ref="mapContainer" class="map"></div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onBeforeUnmount } from "vue";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

interface MapComponentProps {
  center?: [number, number];
  zoom?: number;
}

interface MapComponentEmits {
  (e: "map-ready", map: L.Map): void;
  (e: "bounds-changed", bounds: L.LatLngBounds): void;
}

const props = withDefaults(defineProps<MapComponentProps>(), {
  center: () => [46.0569, 14.5058], // Ljubljana, Slovenia
  zoom: 8,
});

const emit = defineEmits<MapComponentEmits>();

const mapContainer = ref<HTMLElement>();
let map: L.Map | null = null;

onMounted(() => {
  if (mapContainer.value) {
    initializeMap();
  }
});

onBeforeUnmount(() => {
  if (map) {
    map.off("moveend", onMapMoveEnd);
    map.off("zoomend", onMapMoveEnd);
    map.remove();
  }
});

function initializeMap() {
  if (!mapContainer.value) return;

  // Initialize the map
  map = L.map(mapContainer.value, {
    center: props.center,
    zoom: props.zoom,
    zoomControl: true,
    attributionControl: true,
  });

  // Add OpenStreetMap tile layer
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  }).addTo(map);

  // Fix for default markers not showing up
  delete (L.Icon.Default.prototype as any)._getIconUrl;
  L.Icon.Default.mergeOptions({
    iconRetinaUrl:
      "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon-2x.png",
    iconUrl:
      "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-icon.png",
    shadowUrl:
      "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
  });

  // Add event listeners
  map.on("moveend", onMapMoveEnd);
  map.on("zoomend", onMapMoveEnd);

  // Emit map ready event
  emit("map-ready", map);
}

function onMapMoveEnd() {
  if (map) {
    emit("bounds-changed", map.getBounds());
  }
}

defineExpose({
  getMap: () => map,
});
</script>

<style scoped>
.map {
  width: 100%;
  height: 100%;
}
</style>
