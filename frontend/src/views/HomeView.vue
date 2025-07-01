<script lang="ts" setup>
import { onMounted, ref } from "vue";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

const mapContainer = ref<HTMLElement>();
let map: L.Map | null = null;

onMounted(() => {
  if (mapContainer.value) {
    // Initialize the map
    map = L.map(mapContainer.value, {
      center: [46.0569, 14.5058], // Ljubljana, Slovenia coordinates
      zoom: 8,
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

    // Add a sample marker
    L.marker([46.0569, 14.5058])
      .addTo(map)
      .bindPopup("Ljubljana, Slovenia")
      .openPopup();
  }
});
</script>

<template>
  <div class="map-container">
    <div ref="mapContainer" class="map"></div>
  </div>
</template>

<style scoped>
.map-container {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  z-index: 0;
}

.map {
  width: 100%;
  height: 100%;
}
</style>
