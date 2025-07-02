import { ref, computed } from 'vue'
import L from 'leaflet'

export interface GridSquare {
  id: string
  bounds: L.LatLngBounds
  selected: boolean
  polygon?: L.Polygon
}

export function useGridSelection() {
  const gridSquares = ref<Map<string, GridSquare>>(new Map())
  const isDragging = ref(false)
  const dragStartSquare = ref<string | null>(null)
  const dragMode = ref<'select' | 'deselect'>('select')
  const justFinishedDrag = ref(false)
  
  let map: L.Map | null = null
  let gridLayer: L.LayerGroup | null = null

  // Computed properties
  const selectedSquares = computed(() => 
    Array.from(gridSquares.value.values()).filter(square => square.selected)
  )

  const selectedCount = computed(() => selectedSquares.value.length)

  const totalArea = computed(() => {
    // Calculate total area in km²
    return selectedSquares.value.reduce((total, square) => {
      const bounds = square.bounds
      const latDiff = Math.abs(bounds.getNorth() - bounds.getSouth())
      const lngDiff = Math.abs(bounds.getEast() - bounds.getWest())
      
      // Rough conversion to km² (approximate)
      const areaKm2 = latDiff * lngDiff * 111 * 111 * Math.cos(bounds.getCenter().lat * Math.PI / 180)
      return total + areaKm2
    }, 0)
  })

  // Initialize grid system
  function initializeGrid(mapInstance: L.Map) {
    map = mapInstance
    gridLayer = L.layerGroup().addTo(map)
    
    // Update grid when map moves or zooms
    map.on('moveend', updateGrid)
    map.on('zoomend', updateGrid)
    
    // Initial grid creation
    updateGrid()
  }

  // Calculate grid size based on zoom level
  function getGridSize(zoom: number): number {
    // Adjust these values based on your needs
    if (zoom >= 15) return 0.001 // ~100m squares
    if (zoom >= 13) return 0.005 // ~500m squares
    if (zoom >= 11) return 0.01  // ~1km squares
    if (zoom >= 9) return 0.05   // ~5km squares
    return 0.1 // ~10km squares for lower zooms
  }

  // Update grid based on current map view
  function updateGrid() {
    if (!map || !gridLayer) return

    const bounds = map.getBounds()
    const zoom = map.getZoom()
    const gridSize = getGridSize(zoom)

    // Clear existing grid
    clearGrid()

    // Calculate grid bounds
    const north = Math.ceil(bounds.getNorth() / gridSize) * gridSize
    const south = Math.floor(bounds.getSouth() / gridSize) * gridSize
    const east = Math.ceil(bounds.getEast() / gridSize) * gridSize
    const west = Math.floor(bounds.getWest() / gridSize) * gridSize

    // Create grid squares
    for (let lat = south; lat < north; lat += gridSize) {
      for (let lng = west; lng < east; lng += gridSize) {
        const squareBounds = L.latLngBounds(
          [lat, lng],
          [lat + gridSize, lng + gridSize]
        )
        
        const squareId = `${lat.toFixed(6)}_${lng.toFixed(6)}`
        
        // Check if this square was previously selected
        const wasSelected = gridSquares.value.has(squareId) && 
                           gridSquares.value.get(squareId)!.selected

        createGridSquare(squareId, squareBounds, wasSelected)
      }
    }
  }

  // Create individual grid square
  function createGridSquare(id: string, bounds: L.LatLngBounds, selected = false) {
    if (!gridLayer) return

    const polygon = L.polygon([
      [bounds.getSouth(), bounds.getWest()],
      [bounds.getNorth(), bounds.getWest()],
      [bounds.getNorth(), bounds.getEast()],
      [bounds.getSouth(), bounds.getEast()]
    ], {
      fillColor: selected ? '#22c55e' : 'transparent',
      fillOpacity: selected ? 0.3 : 0,
      color: '#6b7280',
      weight: 1,
      opacity: 0.5
    })

    // Add click and drag event handlers
    polygon.on('click', (e) => handleSquareClick(id, e))
    polygon.on('mousedown', (e) => handleMouseDown(id, e))
    polygon.on('mouseenter', () => handleMouseEnter(id))
    polygon.on('mouseover', () => handleMouseOver(id))
    polygon.on('mouseout', () => handleMouseOut(id))
    polygon.on('mouseup', handleMouseUp)
    
    // Add touch support for mobile
    polygon.on('touchstart', (e) => handleTouchStart(id, e))
    polygon.on('touchmove', (e) => handleTouchMove(id, e))
    polygon.on('touchend', handleTouchEnd)

    polygon.addTo(gridLayer)

    const square: GridSquare = {
      id,
      bounds,
      selected,
      polygon
    }

    gridSquares.value.set(id, square)
  }

  // Handle square click
  function handleSquareClick(squareId: string, e: L.LeafletMouseEvent) {
    // Prevent click event if we just finished a drag operation
    if (justFinishedDrag.value) {
      justFinishedDrag.value = false
      L.DomEvent.stopPropagation(e)
      return
    }
    
    L.DomEvent.stopPropagation(e)
    toggleSquareSelection(squareId)
  }

  // Handle mouse down for drag selection
  function handleMouseDown(squareId: string, e: L.LeafletMouseEvent) {
    if (e.originalEvent.button !== 0) return // Only left mouse button
    
    L.DomEvent.stopPropagation(e)
    
    isDragging.value = true
    dragStartSquare.value = squareId
    
    const square = gridSquares.value.get(squareId)
    if (square) {
      dragMode.value = square.selected ? 'deselect' : 'select'
      toggleSquareSelection(squareId)
    }

    // Add global mouse events
    document.addEventListener('mouseup', handleGlobalMouseUp)
    document.addEventListener('mouseleave', handleGlobalMouseUp)
  }

  // Handle mouse enter during drag
  function handleMouseEnter(squareId: string) {
    if (!isDragging.value || !dragStartSquare.value) return
    
    const square = gridSquares.value.get(squareId)
    if (!square) return

    // Apply drag mode to this square
    if (dragMode.value === 'select' && !square.selected) {
      toggleSquareSelection(squareId)
    } else if (dragMode.value === 'deselect' && square.selected) {
      toggleSquareSelection(squareId)
    }
  }

  // Handle hover effects
  function handleMouseOver(squareId: string) {
    if (isDragging.value) return
    
    const square = gridSquares.value.get(squareId)
    if (!square || !square.polygon) return

    // Add hover effect
    square.polygon.setStyle({
      opacity: 1,
      weight: 2
    })
  }

  function handleMouseOut(squareId: string) {
    if (isDragging.value) return
    
    const square = gridSquares.value.get(squareId)
    if (!square || !square.polygon) return

    // Remove hover effect
    square.polygon.setStyle({
      opacity: 0.5,
      weight: 1
    })
  }

  // Handle mouse up
  function handleMouseUp() {
    // This is handled by global mouse up
  }

  // Handle global mouse up
  function handleGlobalMouseUp() {
    if (isDragging.value) {
      justFinishedDrag.value = true
      // Reset the flag after a short delay to allow click event to be processed
      setTimeout(() => {
        justFinishedDrag.value = false
      }, 50)
    }
    
    isDragging.value = false
    dragStartSquare.value = null
    
    document.removeEventListener('mouseup', handleGlobalMouseUp)
    document.removeEventListener('mouseleave', handleGlobalMouseUp)
  }

  // Touch event handlers for mobile
  function handleTouchStart(squareId: string, e: L.LeafletEvent) {
    L.DomEvent.stopPropagation(e)
    
    isDragging.value = true
    dragStartSquare.value = squareId
    
    const square = gridSquares.value.get(squareId)
    if (square) {
      dragMode.value = square.selected ? 'deselect' : 'select'
      toggleSquareSelection(squareId)
    }

    // Add global touch events
    document.addEventListener('touchend', handleGlobalTouchEnd)
    document.addEventListener('touchcancel', handleGlobalTouchEnd)
  }

  function handleTouchMove(squareId: string, e: L.LeafletEvent) {
    if (!isDragging.value || !dragStartSquare.value) return
    
    L.DomEvent.stopPropagation(e)
    handleMouseEnter(squareId)
  }

  function handleTouchEnd() {
    // This is handled by global touch end
  }

  function handleGlobalTouchEnd() {
    if (isDragging.value) {
      justFinishedDrag.value = true
      // Reset the flag after a short delay to allow click event to be processed
      setTimeout(() => {
        justFinishedDrag.value = false
      }, 50)
    }
    
    isDragging.value = false
    dragStartSquare.value = null
    
    document.removeEventListener('touchend', handleGlobalTouchEnd)
    document.removeEventListener('touchcancel', handleGlobalTouchEnd)
  }

  // Toggle square selection
  function toggleSquareSelection(squareId: string) {
    const square = gridSquares.value.get(squareId)
    if (!square || !square.polygon) return

    square.selected = !square.selected
    
    // Update visual appearance
    square.polygon.setStyle({
      fillColor: square.selected ? '#22c55e' : 'transparent',
      fillOpacity: square.selected ? 0.3 : 0
    })
  }

  // Clear all selections
  function clearSelection() {
    gridSquares.value.forEach(square => {
      if (square.selected && square.polygon) {
        square.selected = false
        square.polygon.setStyle({
          fillColor: 'transparent',
          fillOpacity: 0
        })
      }
    })
  }

  // Clear grid from map
  function clearGrid() {
    if (gridLayer) {
      gridLayer.clearLayers()
    }
    gridSquares.value.clear()
  }

  // Get selected bounds for API query
  function getSelectedBounds(): L.LatLngBounds[] {
    return selectedSquares.value.map(square => square.bounds)
  }

  // Cleanup
  function cleanup() {
    if (map) {
      map.off('moveend', updateGrid)
      map.off('zoomend', updateGrid)
    }
    clearGrid()
    if (gridLayer) {
      gridLayer.remove()
    }
  }

  return {
    // State
    selectedCount,
    totalArea,
    isDragging,
    
    // Methods
    initializeGrid,
    clearSelection,
    getSelectedBounds,
    cleanup
  }
}
