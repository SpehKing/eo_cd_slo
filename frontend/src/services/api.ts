import type { ImageListResponse, ImageQueryParams, ImageMetadata, ChangeMaskListResponse, MaskQueryParams, DateRangeResponse } from '@/types/api';
import { cacheService } from './cache';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';
const API_PREFIX = '/api/v1/public';

class ApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = `${API_BASE_URL}${API_PREFIX}`;
  }

  async fetchImages(params: ImageQueryParams = {}): Promise<ImageListResponse> {
    const url = new URL(`${this.baseUrl}/images`);
    
    // Add query parameters
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        url.searchParams.append(key, value.toString());
      }
    });

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch images: ${response.statusText}`);
    }

    return response.json();
  }

  async fetchMasks(params: MaskQueryParams = {}): Promise<ChangeMaskListResponse> {
    const url = new URL(`${this.baseUrl}/change-masks`);
    
    // Add query parameters
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        url.searchParams.append(key, value.toString());
      }
    });

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch masks: ${response.statusText}`);
    }

    const jsonData = await response.json();

    return jsonData;
  }

  async fetchImageMetadata(imageId: number): Promise<ImageMetadata> {
    const url = `${this.baseUrl}/images/${imageId}?format=metadata`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch image metadata: ${response.statusText}`);
    }

    return response.json();
  }

  async fetchImagePreview(imageId: number): Promise<Blob> {
    const url = `${this.baseUrl}/images/${imageId}?format=preview`;
    
    const response = await fetch(url, {
      method: 'GET',
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch image preview: ${response.statusText}`);
    }

    return response.blob();
  }

  async getImagePreviewUrl(imageId: number): Promise<string> {
    // Check cache first
    const cachedUrl = cacheService.getCachedImagePreview(imageId);
    if (cachedUrl) {
      return cachedUrl;
    }

    // Fetch from API and cache
    try {
      const blob = await this.fetchImagePreview(imageId);
      await cacheService.cacheImagePreview(imageId, blob);
      
      // Return blob URL
      return URL.createObjectURL(blob);
    } catch (error) {
      console.error(`Failed to get preview for image ${imageId}:`, error);
      // Return direct URL as fallback
      return `${this.baseUrl}/images/${imageId}?format=preview`;
    }
  }

  // Synchronous method for immediate URL (for existing code compatibility)
  getImagePreviewUrlSync(imageId: number): string {
    const cachedUrl = cacheService.getCachedImagePreview(imageId);
    if (cachedUrl) {
      return cachedUrl;
    }
    
    // Return direct URL if not cached
    return `${this.baseUrl}/images/${imageId}?format=preview`;
  }

  async getMaskPreviewUrl(imgAId: number, imgBId: number): Promise<string> {
    const cacheKey = `mask_${imgAId}_${imgBId}`;
    const cachedUrl = cacheService.getCachedImagePreview(cacheKey);
    if (cachedUrl) {
      return cachedUrl;
    }

    try {
      const url = `${this.baseUrl}/change-masks?format=preview&img_a_id=${imgAId}&img_b_id=${imgBId}`;
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch mask preview: ${response.statusText}`);
      }
      
      const blob = await response.blob();
      await cacheService.cacheImagePreview(cacheKey, blob);
      
      return URL.createObjectURL(blob);
    } catch (error) {
      console.error(`Failed to get mask preview for images ${imgAId}-${imgBId}:`, error);
      // Return direct URL as fallback
      return `${this.baseUrl}/change-masks?format=preview&img_a_id=${imgAId}&img_b_id=${imgBId}`;
    }
  }

  getMaskPreviewUrlSync(imgAId: number, imgBId: number): string {
    const cacheKey = `mask_${imgAId}_${imgBId}`;
    const cachedUrl = cacheService.getCachedImagePreview(cacheKey);
    if (cachedUrl) {
      return cachedUrl;
    }
    
    // Return direct URL if not cached
    return `${this.baseUrl}/change-masks?format=preview&img_a_id=${imgAId}&img_b_id=${imgBId}`;
  }

  async fetchDateRange(): Promise<DateRangeResponse> {
    const url = `${this.baseUrl}/date-range`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to fetch date range: ${response.statusText}`);
    }

    return response.json();
  }

  async healthCheck(): Promise<boolean> {
    try {
      const response = await fetch(`${API_BASE_URL}/`, {
        method: 'GET',
      });
      return response.ok;
    } catch {
      return false;
    }
  }
}

export const apiService = new ApiService();
