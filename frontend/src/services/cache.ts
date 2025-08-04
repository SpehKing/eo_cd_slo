import type { ImageMetadata } from '@/types/api';

interface CacheStats {
  totalSize: number;
  itemCount: number;
}

class SessionCacheService {
  // Cache image metadata - no-op implementation
  cacheImageMetadata(images: ImageMetadata[], boundsKey: string) {
    // Cache disabled - no implementation
  }

  // Get cached image metadata by bounds - always returns null
  getCachedImagesByBounds(boundsKey: string): ImageMetadata[] | null {
    return null;
  }

  // Cache image preview URL blob - no-op implementation
  async cacheImagePreview(imageId: number | string, blob: Blob) {
    // Cache disabled - no implementation
  }

  // Get cached image preview - always returns null
  getCachedImagePreview(imageId: number | string): string | null {
    return null;
  }

  // Get cache statistics - returns empty stats
  getCacheStatistics(): CacheStats & { maxSize: number; cacheHitRate?: number } {
    return {
      totalSize: 0,
      itemCount: 0,
      maxSize: 0
    };
  }

  // Generate bounds key for caching
  generateBoundsKey(bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number }): string {
    const precision = 4;
    return `${bounds.minLon.toFixed(precision)}_${bounds.minLat.toFixed(precision)}_${bounds.maxLon.toFixed(precision)}_${bounds.maxLat.toFixed(precision)}`;
  }

  // Check if image metadata exists in cache - always returns false
  hasImageMetadata(imageId: number): boolean {
    return false;
  }

  // Check if image preview exists in cache - always returns false
  hasImagePreview(imageId: number | string): boolean {
    return false;
  }
}

export const cacheService = new SessionCacheService();
