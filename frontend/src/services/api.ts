import type { ImageListResponse, ImageQueryParams, ImageMetadata } from '@/types/api';

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

  getImagePreviewUrl(imageId: number): string {
    return `${this.baseUrl}/images/${imageId}?format=preview`;
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
