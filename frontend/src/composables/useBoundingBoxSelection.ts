import { ref, computed } from 'vue'
import L from 'leaflet'

export interface BoundingBox {
  bounds: L.LatLngBounds
  selected: boolean
  rectangle?: L.Rectangle
}

export function useBoundingBoxSelection() {
  const boundingBox = ref<BoundingBox | null>(null)
  const isDrawing = ref(false)
  const drawingMode = ref(false) // New state for drawing mode
  
  let map: L.Map | null = null
  let boundingBoxLayer: L.LayerGroup | null = null
  let currentDrawingRectangle: L.Rectangle | null = null
  let drawStartLatLng: L.LatLng | null = null

  // Computed properties
  const hasSelection = computed(() => boundingBox.value?.selected ?? false)
  
  const totalArea = computed(() => {
    if (!boundingBox.value?.selected || !boundingBox.value.bounds) {
      return 0
    }
    
    const bounds = boundingBox.value.bounds
    const latDiff = Math.abs(bounds.getNorth() - bounds.getSouth())
    const lngDiff = Math.abs(bounds.getEast() - bounds.getWest())
    
    // Rough conversion to km² (approximate)
    const areaKm2 = latDiff * lngDiff * 111 * 111 * Math.cos(bounds.getCenter().lat * Math.PI / 180)
    return areaKm2
  })

  // Initialize bounding box drawing system
  function initializeBoundingBox(mapInstance: L.Map) {
    map = mapInstance
    boundingBoxLayer = L.layerGroup().addTo(map)
    
    // Add map event listeners for drawing (initially disabled)
    map.on('mousedown', handleMapMouseDown)
    map.on('mousemove', handleMapMouseMove)
    map.on('mouseup', handleMapMouseUp)
    
    // Add escape key listener to cancel drawing
    document.addEventListener('keydown', handleKeyDown)
  }

  // Toggle drawing mode
  function toggleDrawingMode() {
    drawingMode.value = !drawingMode.value
    
    if (drawingMode.value) {
      // Enable drawing, disable map dragging
      map?.dragging.disable()
      // Change cursor to crosshair
      if (map) {
        map.getContainer().style.cursor = 'crosshair'
      }
    } else {
      // Disable drawing, enable map dragging
      map?.dragging.enable()
      // Reset cursor
      if (map) {
        map.getContainer().style.cursor = ''
      }
      // Cancel any ongoing drawing
      if (isDrawing.value) {
        cancelDrawing()
      }
    }
  }

  // Handle map mouse down to start drawing
  function handleMapMouseDown(e: L.LeafletMouseEvent) {
    if (!drawingMode.value || e.originalEvent.button !== 0) return // Only in drawing mode and left mouse button
    
    // Clear existing selection
    clearSelection()
    
    // Start drawing
    isDrawing.value = true
    drawStartLatLng = e.latlng
    
    // Create initial rectangle
    const bounds = L.latLngBounds(e.latlng, e.latlng)
    currentDrawingRectangle = L.rectangle(bounds, {
      fillColor: '#3b82f6',
      fillOpacity: 0.2,
      color: '#3b82f6',
      weight: 2,
      opacity: 0.8,
      dashArray: '5, 5'
    })
    
    if (boundingBoxLayer) {
      currentDrawingRectangle.addTo(boundingBoxLayer)
    }
    
    L.DomEvent.stopPropagation(e)
  }

  // Handle map mouse move during drawing
  function handleMapMouseMove(e: L.LeafletMouseEvent) {
    if (!drawingMode.value || !isDrawing.value || !drawStartLatLng || !currentDrawingRectangle) return
    
    // Update rectangle bounds
    const bounds = L.latLngBounds(drawStartLatLng, e.latlng)
    currentDrawingRectangle.setBounds(bounds)
    
    L.DomEvent.stopPropagation(e)
  }

  // Handle map mouse up to finish drawing
  function handleMapMouseUp(e: L.LeafletMouseEvent) {
    if (!drawingMode.value || !isDrawing.value || !drawStartLatLng || !currentDrawingRectangle) return
    
    // Finish drawing
    isDrawing.value = false
    
    // Calculate final bounds
    const bounds = L.latLngBounds(drawStartLatLng, e.latlng)
    
    // Only create bounding box if there's a meaningful area
    const latDiff = Math.abs(bounds.getNorth() - bounds.getSouth())
    const lngDiff = Math.abs(bounds.getEast() - bounds.getWest())
    
    if (latDiff > 0.001 && lngDiff > 0.001) { // Minimum size threshold
      // Update rectangle style to show it's selected
      currentDrawingRectangle.setStyle({
        fillColor: '#22c55e',
        fillOpacity: 0.3,
        color: '#22c55e',
        weight: 2,
        opacity: 1,
        dashArray: undefined
      })
      
      // Add click handler to the rectangle for deselection
      currentDrawingRectangle.on('click', handleRectangleClick)
      
      // Store the bounding box
      boundingBox.value = {
        bounds,
        selected: true,
        rectangle: currentDrawingRectangle
      }
    } else {
      // Remove the rectangle if too small
      if (boundingBoxLayer) {
        boundingBoxLayer.removeLayer(currentDrawingRectangle)
      }
    }
    
    // Reset drawing state
    currentDrawingRectangle = null
    drawStartLatLng = null
    
    L.DomEvent.stopPropagation(e)
  }

  // Handle rectangle click for deselection
  function handleRectangleClick(e: L.LeafletMouseEvent) {
    L.DomEvent.stopPropagation(e)
    clearSelection()
  }

  // Handle escape key to cancel drawing
  function handleKeyDown(e: KeyboardEvent) {
    if (e.key === 'Escape' && isDrawing.value) {
      cancelDrawing()
    }
  }

  // Cancel current drawing operation
  function cancelDrawing() {
    if (!isDrawing.value) return
    
    isDrawing.value = false
    
    // Remove the current drawing rectangle
    if (currentDrawingRectangle) {
      currentDrawingRectangle.remove()
    }
    
    // Reset drawing state
    currentDrawingRectangle = null
    drawStartLatLng = null
  }

  // Clear all selections
  function clearSelection() {
    if (boundingBox.value?.rectangle && boundingBoxLayer) {
      boundingBox.value.rectangle.remove()
    }
    boundingBox.value = null
  }

  // Get selected bounds for API query
  function getSelectedBounds(): L.LatLngBounds[] {
    if (boundingBox.value?.selected && boundingBox.value.bounds) {
      return [boundingBox.value.bounds]
    }
    return []
  }

  // Cleanup
  function cleanup() {
    if (map) {
      map.off('mousedown', handleMapMouseDown)
      map.off('mousemove', handleMapMouseMove)
      map.off('mouseup', handleMapMouseUp)
      
      // Reset map state
      map.dragging.enable()
      map.getContainer().style.cursor = ''
    }
    
    document.removeEventListener('keydown', handleKeyDown)
    
    clearSelection()
    
    if (boundingBoxLayer) {
      boundingBoxLayer.remove()
    }
    
    // Reset states
    drawingMode.value = false
    isDrawing.value = false
  }

  return {
    // State
    hasSelection,
    totalArea,
    isDrawing,
    drawingMode,
    
    // Methods
    initializeBoundingBox,
    toggleDrawingMode,
    clearSelection,
    getSelectedBounds,
    cleanup
  }
}
