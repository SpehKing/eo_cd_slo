import type { ImageMetadata } from '@/types/api';

interface CacheItem<T> {
  data: T;
  timestamp: number;
  size: number; // in bytes
}

interface CacheStats {
  totalSize: number;
  itemCount: number;
}

class SessionCacheService {
  private readonly CACHE_DURATION = 60 * 60 * 1000; // 1 hour in milliseconds
  private readonly MAX_CACHE_SIZE = 0.5 * 1024 * 1024 * 1024; // 0.5GB in bytes
  private readonly METADATA_PREFIX = 'img_meta_';
  private readonly PREVIEW_PREFIX = 'img_preview_';
  private readonly BOUNDS_PREFIX = 'bounds_';
  private readonly STATS_KEY = 'cache_stats';

  private getCacheStats(): CacheStats {
    const stats = sessionStorage.getItem(this.STATS_KEY);
    return stats ? JSON.parse(stats) : { totalSize: 0, itemCount: 0 };
  }

  private updateCacheStats(sizeDelta: number, countDelta: number) {
    const stats = this.getCacheStats();
    stats.totalSize += sizeDelta;
    stats.itemCount += countDelta;
    sessionStorage.setItem(this.STATS_KEY, JSON.stringify(stats));
  }

  private getItemSize(data: any): number {
    return new Blob([JSON.stringify(data)]).size;
  }

  private isExpired(timestamp: number): boolean {
    return Date.now() - timestamp > this.CACHE_DURATION;
  }

  private evictExpiredItems() {
    const keysToRemove: string[] = [];
    let reclaimedSize = 0;
    let reclaimedCount = 0;

    for (let i = 0; i < sessionStorage.length; i++) {
      const key = sessionStorage.key(i);
      if (!key || key === this.STATS_KEY) continue;

      if (key.startsWith(this.METADATA_PREFIX) || 
          key.startsWith(this.PREVIEW_PREFIX) || 
          key.startsWith(this.BOUNDS_PREFIX)) {
        try {
          const item = JSON.parse(sessionStorage.getItem(key) || '{}');
          if (this.isExpired(item.timestamp)) {
            keysToRemove.push(key);
            reclaimedSize += item.size || 0;
            reclaimedCount++;
          }
        } catch (e) {
          // Invalid item, remove it
          keysToRemove.push(key);
        }
      }
    }

    keysToRemove.forEach(key => sessionStorage.removeItem(key));
    if (reclaimedCount > 0) {
      this.updateCacheStats(-reclaimedSize, -reclaimedCount);
    }
  }

  private evictOldestItems(spaceNeeded: number) {
    const items: { key: string; timestamp: number; size: number }[] = [];

    for (let i = 0; i < sessionStorage.length; i++) {
      const key = sessionStorage.key(i);
      if (!key || key === this.STATS_KEY) continue;

      if (key.startsWith(this.METADATA_PREFIX) || 
          key.startsWith(this.PREVIEW_PREFIX) || 
          key.startsWith(this.BOUNDS_PREFIX)) {
        try {
          const item = JSON.parse(sessionStorage.getItem(key) || '{}');
          items.push({ key, timestamp: item.timestamp, size: item.size || 0 });
        } catch (e) {
          // Invalid item, will be cleaned up
        }
      }
    }

    // Sort by timestamp (oldest first)
    items.sort((a, b) => a.timestamp - b.timestamp);

    let reclaimedSize = 0;
    let reclaimedCount = 0;

    for (const item of items) {
      if (reclaimedSize >= spaceNeeded) break;
      sessionStorage.removeItem(item.key);
      reclaimedSize += item.size;
      reclaimedCount++;
    }

    if (reclaimedCount > 0) {
      this.updateCacheStats(-reclaimedSize, -reclaimedCount);
    }
  }

  private ensureSpace(itemSize: number) {
    this.evictExpiredItems();
    
    const stats = this.getCacheStats();
    if (stats.totalSize + itemSize > this.MAX_CACHE_SIZE) {
      const spaceNeeded = (stats.totalSize + itemSize) - this.MAX_CACHE_SIZE;
      this.evictOldestItems(spaceNeeded);
    }
  }

  // Cache image metadata
  cacheImageMetadata(images: ImageMetadata[], boundsKey: string) {
    try {
      const cacheItem: CacheItem<ImageMetadata[]> = {
        data: images,
        timestamp: Date.now(),
        size: this.getItemSize(images)
      };

      this.ensureSpace(cacheItem.size);
      
      sessionStorage.setItem(this.BOUNDS_PREFIX + boundsKey, JSON.stringify(cacheItem));
      this.updateCacheStats(cacheItem.size, 1);

      // Also cache individual metadata items for quick lookup
      images.forEach(image => {
        const metaCacheItem: CacheItem<ImageMetadata> = {
          data: image,
          timestamp: Date.now(),
          size: this.getItemSize(image)
        };
        
        this.ensureSpace(metaCacheItem.size);
        sessionStorage.setItem(this.METADATA_PREFIX + image.id, JSON.stringify(metaCacheItem));
        this.updateCacheStats(metaCacheItem.size, 1);
      });
    } catch (e) {
      console.warn('Failed to cache image metadata:', e);
    }
  }

  // Get cached image metadata by bounds
  getCachedImagesByBounds(boundsKey: string): ImageMetadata[] | null {
    try {
      const cached = sessionStorage.getItem(this.BOUNDS_PREFIX + boundsKey);
      if (!cached) return null;

      const item: CacheItem<ImageMetadata[]> = JSON.parse(cached);
      if (this.isExpired(item.timestamp)) {
        sessionStorage.removeItem(this.BOUNDS_PREFIX + boundsKey);
        this.updateCacheStats(-item.size, -1);
        return null;
      }

      return item.data;
    } catch (e) {
      console.warn('Failed to retrieve cached images:', e);
      return null;
    }
  }

  // Cache image preview URL blob
  async cacheImagePreview(imageId: number, blob: Blob) {
    try {
      // Convert blob to base64 for storage
      const arrayBuffer = await blob.arrayBuffer();
      const base64 = btoa(String.fromCharCode(...new Uint8Array(arrayBuffer)));
      
      const cacheItem: CacheItem<string> = {
        data: base64,
        timestamp: Date.now(),
        size: base64.length
      };

      this.ensureSpace(cacheItem.size);
      
      sessionStorage.setItem(this.PREVIEW_PREFIX + imageId, JSON.stringify(cacheItem));
      this.updateCacheStats(cacheItem.size, 1);
    } catch (e) {
      console.warn('Failed to cache image preview:', e);
    }
  }

  // Get cached image preview
  getCachedImagePreview(imageId: number): string | null {
    try {
      const cached = sessionStorage.getItem(this.PREVIEW_PREFIX + imageId);
      if (!cached) return null;

      const item: CacheItem<string> = JSON.parse(cached);
      if (this.isExpired(item.timestamp)) {
        sessionStorage.removeItem(this.PREVIEW_PREFIX + imageId);
        this.updateCacheStats(-item.size, -1);
        return null;
      }

      // Convert base64 back to blob URL
      const byteCharacters = atob(item.data);
      const byteNumbers = new Array(byteCharacters.length);
      for (let i = 0; i < byteCharacters.length; i++) {
        byteNumbers[i] = byteCharacters.charCodeAt(i);
      }
      const byteArray = new Uint8Array(byteNumbers);
      const blob = new Blob([byteArray], { type: 'image/jpeg' });
      
      return URL.createObjectURL(blob);
    } catch (e) {
      console.warn('Failed to retrieve cached image preview:', e);
      return null;
    }
  }

  // Get cache statistics
  getCacheStatistics(): CacheStats & { maxSize: number; cacheHitRate?: number } {
    const stats = this.getCacheStats();
    return {
      ...stats,
      maxSize: this.MAX_CACHE_SIZE,
      totalSize: stats.totalSize,
      itemCount: stats.itemCount
    };
  }

  // Generate bounds key for caching
  generateBoundsKey(bounds: { minLon: number; minLat: number; maxLon: number; maxLat: number }): string {
    // Round to reasonable precision to improve cache hits for similar bounds
    const precision = 4;
    return `${bounds.minLon.toFixed(precision)}_${bounds.minLat.toFixed(precision)}_${bounds.maxLon.toFixed(precision)}_${bounds.maxLat.toFixed(precision)}`;
  }

  // Check if image metadata exists in cache
  hasImageMetadata(imageId: number): boolean {
    const cached = sessionStorage.getItem(this.METADATA_PREFIX + imageId);
    if (!cached) return false;

    try {
      const item: CacheItem<ImageMetadata> = JSON.parse(cached);
      return !this.isExpired(item.timestamp);
    } catch {
      return false;
    }
  }

  // Check if image preview exists in cache
  hasImagePreview(imageId: number): boolean {
    const cached = sessionStorage.getItem(this.PREVIEW_PREFIX + imageId);
    if (!cached) return false;

    try {
      const item: CacheItem<string> = JSON.parse(cached);
      return !this.isExpired(item.timestamp);
    } catch {
      return false;
    }
  }
}

export const cacheService = new SessionCacheService();
