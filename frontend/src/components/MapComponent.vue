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
  zoom: 13,
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

  // Define tile layers
  const streetLayer = L.tileLayer(
    "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 19,
    }
  );

  const satelliteLayer = L.tileLayer(
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    {
      attribution:
        "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community",
      maxZoom: 20,
      tileSize: 256,
    }
  );

  // Define Slovenia boundaries
  const sloveniaBounds = L.latLngBounds(
    [45.4, 13.4], // Southwest corner
    [46.9, 16.6] // Northeast corner
  );

  // Initialize the map with the street layer as default
  map = L.map(mapContainer.value, {
    center: props.center,
    zoom: props.zoom,
    minZoom: 10,
    maxZoom: 18,
    maxBounds: sloveniaBounds,
    maxBoundsViscosity: 1.0, // Makes the boundary restriction strong
    zoomControl: true,
    attributionControl: true,
    layers: [satelliteLayer], // Default layer
  });

  // Define base layers for the layer control
  const baseLayers = {
    Streets: streetLayer,
    Satellite: satelliteLayer,
  };

  // Add layer control
  L.control.layers(baseLayers).addTo(map);

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
