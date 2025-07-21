/**
 * Parse WKT POLYGON string to extract bounding box coordinates
 */
export function parseWktPolygon(wkt: string): { bounds: [[number, number], [number, number]] } | null {
  // Example WKT: "POLYGON((13.8 45.8,14.2 45.8,14.2 46.0,13.8 46.0,13.8 45.8))"
  const polygonMatch = wkt.match(/POLYGON\s*\(\s*\((.*?)\)\s*\)/i);
  
  if (!polygonMatch) {
    console.warn('Invalid WKT format:', wkt);
    return null;
  }

  const coordString = polygonMatch[1];
  const coordinates = coordString.split(',').map(coord => {
    const [lon, lat] = coord.trim().split(/\s+/).map(Number);
    return [lon, lat];
  });

  if (coordinates.length < 4) {
    console.warn('Invalid polygon coordinates:', coordinates);
    return null;
  }

  // Calculate bounding box from polygon coordinates
  const lons = coordinates.map(coord => coord[0]);
  const lats = coordinates.map(coord => coord[1]);
  
  const minLon = Math.min(...lons);
  const maxLon = Math.max(...lons);
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);

  // Return in Leaflet bounds format: [[south, west], [north, east]]
  return {
    bounds: [[minLat, minLon], [maxLat, maxLon]] as [[number, number], [number, number]]
  };
}

/**
 * Format date for display
 */
export function formatDate(dateString: string): string {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  });
}

/**
 * Format file size for display
 */
export function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
