// API response types matching backend schemas
export interface ImageMetadata {
  id: number;
  time: string;
  bbox_wkt: string;
  width?: number;
  height?: number;
  data_type?: string;
  size_bytes: number;
}

export interface ImageListResponse {
  images: ImageMetadata[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
}

export interface ImageQueryParams {
  start_time?: string;
  end_time?: string;
  min_lon?: number;
  min_lat?: number;
  max_lon?: number;
  max_lat?: number;
  limit?: number;
  offset?: number;
}

export interface BoundingBox {
  minLon: number;
  minLat: number;
  maxLon: number;
  maxLat: number;
}
